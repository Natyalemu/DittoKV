use crate::error::Error;
use crate::id::Id;
use crate::log::log::LogEntry;
use crate::peer::Peer;
use crate::role::Role;
use crate::rpc::RequestVoteResponse;
use crate::rpc::RPC;
use crate::rpc::*;
use crate::state_machine::{StateMachine, StateMachineMsg};
use rand::random;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time;

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum MessageToClient {
    Success,
    Failed,
}

pub enum StateMachineQuery {
    LastLogIndex(oneshot::Sender<Option<u64>>),
    LastLogTerm(oneshot::Sender<Option<u64>>),
    EntryTerm(u64, oneshot::Sender<Option<u64>>),
    GetEntry(u64, oneshot::Sender<Option<LogEntry>>),
    CommitIndex(oneshot::Sender<u64>),
}

pub struct RaftServer {
    id: Id,
    peers: Mutex<Vec<std::sync::Arc<Peer>>>,
    listener: Arc<TcpListener>,
    term: u64,
    role: Role,
    client: HashMap<u64, String>,
    last_log_term: u64,
    commit_index: u64,
    voted_for: Option<Id>,

    tx_to_statemachine: Option<mpsc::Sender<StateMachineMsg>>,
    tx_query_statemachine: Option<mpsc::Sender<StateMachineQuery>>,

    tx_to_peers: Arc<Mutex<HashMap<u64, mpsc::Sender<RPC>>>>,
    tx_to_clients: Arc<Mutex<HashMap<u64, mpsc::Sender<RPC>>>>,

    rx_from_peers: Option<mpsc::Receiver<(u64, RPC)>>,
    tx_to_server: mpsc::Sender<(u64, RPC)>,
    next_index: HashMap<u64, u64>,
    match_index: HashMap<u64, u64>,

    cmd_to_server: mpsc::Sender<(u64, RPC)>,
    rx_from_clients: Option<mpsc::Receiver<(u64, RPC)>>,
}

impl RaftServer {
    pub fn new(
        id: Id,
        listener: TcpListener,
        peers: Vec<std::sync::Arc<Peer>>,
        role: Role,
    ) -> Self {
        // placeholder channels for initialization; actual receivers are set elsewhere by caller
        let (tx_to_server, rx_from_peers) = mpsc::channel::<(u64, RPC)>(100);
        let (cmd_to_server, rx_from_clients) = mpsc::channel::<(u64, RPC)>(100);

        Self {
            id,
            peers: Mutex::new(peers),
            listener: Arc::new(listener),
            term: 0,
            role: role,
            client: HashMap::new(),
            last_log_term: 0,
            commit_index: 0,
            voted_for: None,
            tx_to_statemachine: None,
            tx_query_statemachine: None,
            tx_to_peers: Arc::new(Mutex::new(HashMap::new())),
            tx_to_clients: Arc::new(Mutex::new(HashMap::new())),
            rx_from_peers: Some(rx_from_peers),
            tx_to_server,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            cmd_to_server,
            rx_from_clients: Some(rx_from_clients),
        }
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        let (tx_to_state_machine, rx_from_log) = mpsc::channel::<StateMachineMsg>(100);
        let (tx_query_state_machine, mut rx_query) = mpsc::channel::<StateMachineQuery>(100);

        self.tx_to_statemachine = Some(tx_to_state_machine.clone());
        self.tx_query_statemachine = Some(tx_query_state_machine.clone());

        let mut owned_state_machine = StateMachine::new();

        let mut rx_from_log = rx_from_log;
        let server_id = self.id.get_id().clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    maybe_cmd = rx_from_log.recv() => {
                        match maybe_cmd {
                            Some(cmd) => {
                                match cmd {
                                    StateMachineMsg::Append(entry) => {
                                        let cloned_server_id = server_id.clone();
                                        owned_state_machine.log(entry,cloned_server_id);
                                    }
                                    StateMachineMsg::CommitTo(idx) => {
                                        owned_state_machine.update_commit_index(idx);
                                        owned_state_machine.store();
                                    }
                                    StateMachineMsg::ShutDown => {
                                        break;
                                    }
                                }
                            }
                            None => break,
                        }
                    }

                    maybe_query = rx_query.recv() => {
                        match maybe_query {
                            Some(query) => {
                                match query {
                                    StateMachineQuery::LastLogIndex(resp_tx) => {
                                        let _ = resp_tx.send(owned_state_machine.last_log_index());
                                    }
                                    StateMachineQuery::LastLogTerm(resp_tx) => {
                                        let _ = resp_tx.send(owned_state_machine.last_log_term());
                                    }
                                    StateMachineQuery::EntryTerm(idx, resp_tx) => {
                                        let _ = resp_tx.send(owned_state_machine.entry_term(idx));
                                    }
                                    StateMachineQuery::GetEntry(idx, resp_tx) => {
                                        let _ = resp_tx.send(owned_state_machine.get_entry(idx));
                                    }
                                    StateMachineQuery::CommitIndex(resp_tx) => {
                                        let _ = resp_tx.send(owned_state_machine.commit_index());
                                    }
                                }
                            }
                            None => break,
                        }

                    }


                }
            }
        });

        let mut rx = self
            .rx_from_peers
            .take()
            .expect("rx_from_peers must be Some when run() is called");

        let listener = Arc::clone(&self.listener);
        let tx_to_peers_for_accept = Arc::clone(&self.tx_to_peers);
        let tx_to_clients_for_accept = Arc::clone(&self.tx_to_clients);
        let tx_to_server_for_accept = self.tx_to_server.clone();
        let cmd_to_server_for_accept = self.cmd_to_server.clone();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _addr)) => {
                        let tx_to_peers_inner = Arc::clone(&tx_to_peers_for_accept);
                        let tx_to_clients_inner = Arc::clone(&tx_to_clients_for_accept);
                        let tx_to_server_inner = tx_to_server_for_accept.clone();
                        let cmd_to_server_inner = cmd_to_server_for_accept.clone();
                        tokio::spawn(async move {
                            let _ = accept_handshake_and_spawn(
                                stream,
                                tx_to_peers_inner,
                                tx_to_clients_inner,
                                tx_to_server_inner,
                                cmd_to_server_inner,
                            )
                            .await;
                        });
                    }
                    Err(_) => continue,
                }
            }
        });

        // dial out to known peers (if any)
        let peers_to_connect: Vec<std::sync::Arc<Peer>> = {
            let guard = self.peers.lock().await;
            guard.iter().cloned().collect()
        };

        let tx_to_peers_for_dial = Arc::clone(&self.tx_to_peers);
        let tx_to_clients_for_dial = Arc::clone(&self.tx_to_clients);
        let tx_to_server_for_dial = self.tx_to_server.clone();
        let cmd_to_server_for_dial = self.cmd_to_server.clone();
        let our_id = self.id.get_id();

        for peer in peers_to_connect {
            let addr = peer.addr.clone();
            let tx_to_peers_map = Arc::clone(&tx_to_peers_for_dial);
            let tx_to_clients_map = Arc::clone(&tx_to_clients_for_dial);
            let tx_to_server = tx_to_server_for_dial.clone();
            let cmd_to_server = cmd_to_server_for_dial.clone();
            tokio::spawn(async move {
                if let Ok(stream) = TcpStream::connect(addr).await {
                    let _ = dial_handshake_and_spawn(
                        stream,
                        our_id,
                        tx_to_peers_map,
                        tx_to_clients_map,
                        tx_to_server,
                        cmd_to_server,
                    )
                    .await;
                }
            });
        }

        for (id, addr) in &self.client {
            let cloned_id = *id;
            let cmd_to_server = self.cmd_to_server.clone();

            let (tx_to_client, rx_from_server) = mpsc::channel::<RPC>(100);
            {
                let mut cmap = self.tx_to_clients.lock().await;
                cmap.insert(cloned_id, tx_to_client);
            }

            let stream = TcpStream::connect(addr).await.map_err(|e| Error::Io(e))?;
            let (reader, writer) = stream.into_split();
            let buf_reader = BufReader::new(reader);
            tokio::spawn(async move {
                client_task(cloned_id, buf_reader, writer, cmd_to_server, rx_from_server).await;
            });
        }

        loop {
            match self.role {
                Role::Follower => {
                    let duration = self.random_election_timeout_duration();
                    let timeout = tokio::time::sleep(duration);
                    tokio::pin!(timeout);

                    tokio::select! {
                        maybe = rx.recv() => {
                            match maybe {
                                Some((peer_id, rpc)) => {
                                    self.handle_follower(peer_id, rpc).await?;
                                }
                                None => return Err(Error::ChannelClosed),
                            }
                        }
                        _ = &mut timeout => {
                            self.role = Role::Candidate;
                        }
                    }
                }

                Role::Candidate => {
                    if let Err(e) = self.candidate_handler(&mut rx).await {
                        return Err(e);
                    };
                }

                Role::Leader => {
                    if let Err(e) = self.leader_handler(&mut rx).await {
                        return Err(e);
                    }
                }
            }
        }
    }

    fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(50)
    }

    async fn init_leader_state(&mut self) -> Result<(), Error> {
        let last_index = self.sm_last_log_index().await?.unwrap_or(0);
        self.commit_index = self.sm_commit_index().await?;
        self.next_index = self
            .next_index
            .clone()
            .into_iter()
            .collect::<HashMap<u64, u64>>();

        let map = self.tx_to_peers.lock().await;
        for &peer_id in map.keys() {
            self.next_index.insert(peer_id, last_index + 1);
            self.match_index.insert(peer_id, 0);
        }

        self.match_index.insert(self.id.get_id(), last_index);
        Ok(())
    }

    async fn build_append_for_peer(
        &self,
        peer_id: u64,
    ) -> Result<Option<AppendEntryRequest>, Error> {
        let next_idx = *self.next_index.get(&peer_id).unwrap_or(&1);
        let prev_index = next_idx.saturating_sub(1);
        let prev_term = if prev_index == 0 {
            0
        } else {
            self.sm_entry_term(prev_index).await?.unwrap_or(0)
        };

        let entry = self.sm_get_entry(next_idx).await?;

        Ok(Some(AppendEntryRequest {
            term: self.term,
            leader_id: self.id,
            prev_log_index: prev_index,
            prev_log_term: prev_term,
            entry,
            leader_commit: self.commit_index,
        }))
    }

    async fn try_advance_commit(&mut self) -> Result<(), Error> {
        let mut match_indexes: Vec<u64> = self.match_index.values().copied().collect();

        let leader_last = self.sm_last_log_index().await?.unwrap_or(0);
        match_indexes.push(leader_last);

        match_indexes.sort_unstable_by(|a, b| b.cmp(a));

        let cluster_size = {
            let map = self.tx_to_peers.lock().await;
            map.len() + 1
        };
        let majority = (cluster_size / 2) + 1;
        if majority == 0 || majority > match_indexes.len() {
            return Ok(());
        }
        let candidate_n = match_indexes[majority - 1];

        if candidate_n > self.commit_index {
            if let Some(term_at_n) = self.sm_entry_term(candidate_n).await? {
                if term_at_n == self.term {
                    self.commit_index = candidate_n;
                    if let Some(tx) = &self.tx_to_statemachine {
                        let _ = tx.send(StateMachineMsg::CommitTo(candidate_n)).await;
                    }
                }
            }
        }
        Ok(())
    }

    async fn send_replication_round(&self) -> Result<(), Error> {
        let term = self.term;
        let leader_id = self.id;
        let leader_commit = self.commit_index;
        let prev_log_index = self.sm_last_log_index().await?.unwrap_or(0);
        let prev_log_term = self.sm_last_log_term().await?.unwrap_or(0);

        let work: Vec<(u64, mpsc::Sender<RPC>)> = {
            let map = self.tx_to_peers.lock().await;
            map.iter().map(|(&k, v)| (k, v.clone())).collect()
        };

        for (pid, tx) in work {
            let next_idx = *self.next_index.get(&pid).unwrap_or(&1);
            let last_idx = self.sm_last_log_index().await?.unwrap_or(0);

            let req = if next_idx <= last_idx {
                self.build_append_for_peer(pid)
                    .await?
                    .unwrap_or(AppendEntryRequest {
                        term,
                        leader_id,
                        prev_log_index,
                        prev_log_term,
                        entry: None,
                        leader_commit,
                    })
            } else {
                AppendEntryRequest {
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entry: None,
                    leader_commit,
                }
            };

            let _ = tx.send(RPC::AppendEntryRequest(req)).await;
        }

        Ok(())
    }

    pub async fn leader_handler(
        &mut self,
        rx: &mut mpsc::Receiver<(u64, RPC)>,
    ) -> Result<(), Error> {
        self.init_leader_state().await?;
        self.send_replication_round().await?;

        let mut ticker = time::interval(self.heartbeat_interval());
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        let mut rx_from_clients = self
            .rx_from_clients
            .take()
            .expect("rx_from_clients must be Some when run() is called");

        loop {
            tokio::select! {
                maybe = rx.recv() => {
                    match maybe {
                        Some((peer_id, rpc)) => {
                            match rpc {
                                RPC::AppendEntryResponse(resp) => {
                                    if resp.term > self.term {
                                        self.term = resp.term;
                                        self.voted_for = None;
                                        self.role = Role::Follower;
                                        return Ok(());
                                    }

                                    if resp.success {
                                        let ni = self.next_index.get(&peer_id).copied().unwrap_or(1);
                                        let replicated_idx = ni.saturating_sub(1);
                                        self.match_index.insert(peer_id, replicated_idx);
                                        self.next_index.insert(peer_id, replicated_idx + 1);
                                        self.try_advance_commit().await?;
                                    } else {
                                        let ni = self.next_index.entry(peer_id).or_insert(1);
                                        if *ni > 1 { *ni -= 1; } else { *ni = 1; }

                                        let map = self.tx_to_peers.lock().await;
                                        if let Some(tx) = map.get(&peer_id) {
                                            if let Some(req) = self.build_append_for_peer(peer_id).await? {
                                                let _ = tx.send(RPC::AppendEntryRequest(req)).await;
                                            }
                                        }
                                    }
                                }

                                RPC::RequestVoteRequest(req) => {
                                    if req.term > self.term {
                                        self.term = req.term;
                                        self.voted_for = None;
                                        self.role = Role::Follower;
                                        return Ok(());
                                    } else {
                                        let map = self.tx_to_peers.lock().await;
                                        if let Some(tx) = map.get(&peer_id) {
                                            let _ = tx.send(RPC::RequestVoteResponse(RequestVoteResponse {
                                                term: self.term,
                                                vote_granted: false,
                                            })).await;
                                        }
                                    }
                                }

                                RPC::AppendEntryRequest(areq) => {
                                    if areq.term > self.term {
                                        self.term = areq.term;
                                        self.voted_for = None;
                                        self.role = Role::Follower;
                                        return Ok(());
                                    }
                                }

                                _ => { }
                            }
                        }
                        None => return Err(Error::ChannelClosed),
                    }
                }

                Some((client_id, rpc)) = rx_from_clients.recv() => {
                    match rpc {
                        RPC::CommandRequest(req) => {
                            let client_cmd = req.command;
                            let entry = crate::log::log::LogEntry {
                                term: self.term,
                                command: client_cmd,
                            };
                            let tx = match &self.tx_to_statemachine {
                                Some(tx) => tx.clone(),
                                None => return Err(Error::Internal("state machine channel missing".into())),
                            };
                            tx.send(StateMachineMsg::Append(entry)).await.map_err(|_| Error::FailedToSendToStateMachine)?;
                            self.send_replication_round().await?;
                        }
                        RPC::WhoIsTheLeader(_) => {
                            let id = self.id.get_id() as u64;
                            let resp = RPC::IAmTheLeader(IAmTheLeader { id });
                            let map = self.tx_to_clients.lock().await;
                            if let Some(tx_to_client) = map.get(&client_id) {
                                let _ = tx_to_client.send(resp).await;
                            }
                        }
                        _ => {}
                    }
                }

                _ = ticker.tick() => {
                    self.send_replication_round().await?;
                }
            }
        }
    }

    async fn candidate_handler(
        &mut self,
        rx: &mut mpsc::Receiver<(u64, RPC)>,
    ) -> Result<(), Error> {
        loop {
            let majority = {
                let map = self.tx_to_peers.lock().await;
                (map.len() / 2) + 1
            };
            self.term += 1;
            self.voted_for = Some(self.id);
            let mut votes: usize = 1;

            let request_vote_request = RequestVoteRequest {
                term: self.term,
                candidate_id: self.id,
                last_log_term: self.last_log_term,
                last_log_index: self.sm_last_log_index().await?.unwrap_or(0),
            };

            let map = self.tx_to_peers.lock().await;
            for (_, sender) in map.iter() {
                let _ = sender
                    .send(RPC::RequestVoteRequest(request_vote_request.clone()))
                    .await;
            }
            drop(map);

            let mut timeout = tokio::time::sleep(self.random_election_timeout_duration());
            tokio::pin!(timeout);

            while votes < majority {
                tokio::select! {
                    maybe = rx.recv() => {
                        match maybe {
                            Some((_, rpc)) => {
                                match rpc {
                                    RPC::RequestVoteResponse(resp) => {
                                        if resp.term > self.term {
                                            self.term = resp.term;
                                            self.voted_for = None;
                                            self.role = Role::Follower;
                                            return Ok(());
                                        }
                                        if resp.vote_granted {
                                            votes += 1;
                                            if votes >= majority {
                                                self.role = Role::Leader;
                                                return Ok(());
                                            }
                                        }
                                    }
                                    RPC::AppendEntryRequest(areq) => {
                                        if areq.term >= self.term {
                                            self.term = areq.term;
                                            self.role = Role::Follower;
                                            return Ok(());
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            None => return Err(Error::ChannelClosed),
                        }
                    }

                    _ = &mut timeout => {
                        break;
                    }
                }
            }
        }
    }

    fn random_election_timeout_duration(&self) -> Duration {
        // use rand::random for simplicity
        let v: u64 = (random::<u64>() % 151) + 150; // range 150..=300
        Duration::from_millis(v)
    }

    pub async fn handle_follower(&mut self, peer_id: u64, rpc: RPC) -> Result<(), Error> {
        match rpc {
            RPC::AppendEntryRequest(req) => match req.entry {
                Some(entry) => {
                    if let Some(tx) = &self.tx_to_statemachine {
                        tx.send(StateMachineMsg::Append(entry))
                            .await
                            .map_err(|_| Error::FailedToSendToStateMachine)?;
                        tx.send(StateMachineMsg::CommitTo(req.leader_commit))
                            .await
                            .map_err(|_| Error::FailedToSendToStateMachine)?;
                    } else {
                        return Err(Error::Internal("state machine channel missing".into()));
                    }

                    let map = self.tx_to_peers.lock().await;
                    if let Some(peer_tx) = map.get(&peer_id) {
                        peer_tx
                            .send(RPC::AppendEntryResponse(AppendEntryResponse {
                                term: self.term,
                                success: true,
                            }))
                            .await
                            .map_err(|_| Error::FailedToSendRPC)?;
                        Ok(())
                    } else {
                        Err(Error::FailedToGetTheKey)
                    }
                }
                None => Ok(()),
            },

            RPC::RequestVoteRequest(req) => self.handle_request_vote(peer_id, req).await,

            _ => Ok(()),
        }
    }

    pub async fn handle_request_vote(
        &mut self,
        peer_id: u64,
        req: RequestVoteRequest,
    ) -> Result<(), Error> {
        let vote_granted = if req.term < self.term {
            false
        } else {
            if req.term > self.term {
                self.term = req.term;
                self.voted_for = None;
            }

            if let Some(voted_for) = self.voted_for.as_ref() {
                if voted_for != &req.candidate_id {
                    false
                } else {
                    true
                }
            } else {
                let local_last = self.sm_last_log_index().await?.unwrap_or(0);
                let up_to_date = (req.last_log_term > self.last_log_term)
                    || (req.last_log_term == self.last_log_term
                        && req.last_log_index >= local_last);

                if up_to_date {
                    self.voted_for = Some(req.candidate_id);
                    true
                } else {
                    false
                }
            }
        };

        let map = self.tx_to_peers.lock().await;
        if let Some(peer_tx) = map.get(&peer_id) {
            let response = RPC::RequestVoteResponse(RequestVoteResponse {
                term: self.term,
                vote_granted,
            });

            peer_tx
                .send(response)
                .await
                .map_err(|_| Error::FailedToSendRPC)?;
            Ok(())
        } else {
            Err(Error::FailedToGetTheKey)
        }
    }

    pub fn log_term(&mut self) {
        self.last_log_term = self.term;
    }

    async fn sm_last_log_index(&self) -> Result<Option<u64>, Error> {
        let (tx, rx) = oneshot::channel();
        let sender = self
            .tx_query_statemachine
            .as_ref()
            .ok_or_else(|| Error::Internal("state machine query channel missing".into()))?
            .clone();
        sender
            .send(StateMachineQuery::LastLogIndex(tx))
            .await
            .map_err(|_| Error::Internal("failed to send query".into()))?;
        rx.await
            .map_err(|_| Error::Internal("state machine actor dropped".into()))
    }

    async fn sm_last_log_term(&self) -> Result<Option<u64>, Error> {
        let (tx, rx) = oneshot::channel();
        let sender = self
            .tx_query_statemachine
            .as_ref()
            .ok_or_else(|| Error::Internal("state machine query channel missing".into()))?
            .clone();
        sender
            .send(StateMachineQuery::LastLogTerm(tx))
            .await
            .map_err(|_| Error::Internal("failed to send query".into()))?;
        rx.await
            .map_err(|_| Error::Internal("state machine actor dropped".into()))
    }

    async fn sm_entry_term(&self, idx: u64) -> Result<Option<u64>, Error> {
        let (tx, rx) = oneshot::channel();
        let sender = self
            .tx_query_statemachine
            .as_ref()
            .ok_or_else(|| Error::Internal("state machine query channel missing".into()))?
            .clone();
        sender
            .send(StateMachineQuery::EntryTerm(idx, tx))
            .await
            .map_err(|_| Error::Internal("failed to send query".into()))?;
        rx.await
            .map_err(|_| Error::Internal("state machine actor dropped".into()))
    }

    async fn sm_get_entry(&self, idx: u64) -> Result<Option<LogEntry>, Error> {
        let (tx, rx) = oneshot::channel();
        let sender = self
            .tx_query_statemachine
            .as_ref()
            .ok_or_else(|| Error::Internal("state machine query channel missing".into()))?
            .clone();
        sender
            .send(StateMachineQuery::GetEntry(idx, tx))
            .await
            .map_err(|_| Error::Internal("failed to send query".into()))?;
        rx.await
            .map_err(|_| Error::Internal("state machine actor dropped".into()))
    }

    async fn sm_commit_index(&self) -> Result<u64, Error> {
        let (tx, rx) = oneshot::channel();
        let sender = self
            .tx_query_statemachine
            .as_ref()
            .ok_or_else(|| Error::Internal("state machine query channel missing".into()))?
            .clone();
        sender
            .send(StateMachineQuery::CommitIndex(tx))
            .await
            .map_err(|_| Error::Internal("failed to send query".into()))?;
        rx.await
            .map_err(|_| Error::Internal("state machine actor dropped".into()))
    }
}

async fn accept_handshake_and_spawn(
    stream: TcpStream,
    tx_to_peers: Arc<Mutex<HashMap<u64, mpsc::Sender<RPC>>>>,
    tx_to_clients: Arc<Mutex<HashMap<u64, mpsc::Sender<RPC>>>>,
    tx_to_server: mpsc::Sender<(u64, RPC)>,
    cmd_to_server: mpsc::Sender<(u64, RPC)>,
) -> Result<(), ()> {
    let (reader, writer) = stream.into_split();
    let mut buf_reader = BufReader::new(reader);

    let mut first_line = String::new();
    match tokio::time::timeout(
        Duration::from_secs(3),
        buf_reader.read_line(&mut first_line),
    )
    .await
    {
        Ok(Ok(n)) => {
            if n == 0 {
                return Err(());
            }
        }
        _ => return Err(()),
    }

    let v: Value = serde_json::from_str(first_line.trim()).map_err(|_| ())?;
    let kind = v.get("type").and_then(|t| t.as_str()).ok_or(())?;

    match kind {
        "node" => {
            let peer_id = v.get("id").and_then(|x| x.as_u64()).ok_or(())? as u64;
            let (tx_to_peer, rx_from_server) = mpsc::channel::<RPC>(100);
            {
                let mut map = tx_to_peers.lock().await;
                map.insert(peer_id, tx_to_peer.clone());
            }
            tokio::spawn(async move {
                peer_task(peer_id, buf_reader, writer, tx_to_server, rx_from_server).await;
            });
            Ok(())
        }
        "client" => {
            let client_id = v
                .get("client_id")
                .and_then(|x| x.as_u64())
                .unwrap_or_else(|| random::<u64>()) as u64;
            let (tx_to_client, rx_from_server) = mpsc::channel::<RPC>(100);
            {
                let mut cmap = tx_to_clients.lock().await;
                cmap.insert(client_id, tx_to_client.clone());
            }
            tokio::spawn(async move {
                client_task(client_id, buf_reader, writer, cmd_to_server, rx_from_server).await;
            });
            Ok(())
        }
        _ => Err(()),
    }
}

async fn dial_handshake_and_spawn(
    stream: TcpStream,
    our_id: u64,
    tx_to_peers: Arc<Mutex<HashMap<u64, mpsc::Sender<RPC>>>>,
    tx_to_clients: Arc<Mutex<HashMap<u64, mpsc::Sender<RPC>>>>,
    tx_to_server: mpsc::Sender<(u64, RPC)>,
    cmd_to_server: mpsc::Sender<(u64, RPC)>,
) -> Result<(), ()> {
    let (reader, mut writer) = stream.into_split();
    let buf_reader = BufReader::new(reader);

    let identify = serde_json::json!({ "type":"node", "id": our_id });
    let serialized = serde_json::to_string(&identify).map_err(|_| ())?;
    writer
        .write_all(serialized.as_bytes())
        .await
        .map_err(|_| ())?;
    writer.write_all(b"\n").await.map_err(|_| ())?;

    let peer_id = random::<u64>();

    let (tx_to_peer, rx_from_server) = mpsc::channel::<RPC>(100);
    {
        let mut map = tx_to_peers.lock().await;
        map.insert(peer_id, tx_to_peer.clone());
    }

    tokio::spawn(async move {
        peer_task(peer_id, buf_reader, writer, tx_to_server, rx_from_server).await;
    });

    Ok(())
}

pub async fn client_task(
    client_id: u64,
    reader: BufReader<OwnedReadHalf>,
    mut writer: OwnedWriteHalf,
    cmd_to_server: mpsc::Sender<(u64, RPC)>,
    mut rx_from_server: mpsc::Receiver<RPC>,
) {
    let mut lines = reader.lines();

    loop {
        tokio::select! {
            line = lines.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        match serde_json::from_str::<RPC>(&line) {
                            Ok(rpc) => {
                                if cmd_to_server.send((client_id, rpc)).await.is_err() {
                                    break;
                                }
                            }
                            Err(_) => {}
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            Some(msg) = rx_from_server.recv() => {
                match serde_json::to_string(&msg) {
                    Ok(serialized) => {
                        if writer.write_all(serialized.as_bytes()).await.is_err() { break; }
                        if writer.write_all(b"\n").await.is_err() { break; }
                    }
                    Err(_) => {}
                }
            }
        }
    }
}

pub async fn peer_task(
    peer_id: u64,
    reader: BufReader<OwnedReadHalf>,
    mut writer: OwnedWriteHalf,
    tx_to_server: mpsc::Sender<(u64, RPC)>,
    mut rx_from_server: mpsc::Receiver<RPC>,
) {
    let mut lines = reader.lines();

    loop {
        tokio::select! {
            line = lines.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        match serde_json::from_str::<RPC>(&line) {
                            Ok(rpc) => {
                                if tx_to_server.send((peer_id, rpc)).await.is_err() { break; }
                            }
                            Err(_) => {}
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            Some(msg) = rx_from_server.recv() => {
                match serde_json::to_string(&msg) {
                    Ok(serialized) => {
                        if writer.write_all(serialized.as_bytes()).await.is_err() { break; }
                        if writer.write_all(b"\n").await.is_err() { break; }
                    }
                    Err(_) => {}
                }
            }
        }
    }
}
