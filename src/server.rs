use crate::error::Error;
use crate::id::Id;
use crate::log::cmd::Command;
use crate::log::log;
use crate::peer::Peer;
use crate::role::Role;
use crate::rpc::RequestVoteResponse;
use crate::rpc::RPC;
use crate::rpc::*;
use crate::state_machine::{StateMachine, StateMachineMsg};
use log::LogEntry;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::time;

use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum MessageToClient {
    Success,
    Failed,
}

/// Queries that ask the state machine for read-only data and expect a reply via oneshot
pub enum StateMachineQuery {
    LastLogIndex(oneshot::Sender<Option<u64>>),
    LastLogTerm(oneshot::Sender<Option<u64>>),
    EntryTerm(u64, oneshot::Sender<Option<u64>>),
    GetEntry(u64, oneshot::Sender<Option<LogEntry>>),
    CommitIndex(oneshot::Sender<u64>),
}

/// Server actor (Raft node)
pub struct Server {
    id: Id,
    peers: tokio::sync::Mutex<Vec<std::sync::Arc<Peer>>>,
    listener: tokio::net::TcpListener,
    term: u64,
    role: Role,
    client: HashMap<u64, String>,
    last_log_term: u64,
    commit_index: u64,
    voted_for: Option<Id>,

    /// Sender for commands to the state-machine actor (Append, Commit)
    tx_to_statemachine: Option<mpsc::Sender<StateMachineMsg>>,
    /// Sender for queries to the state-machine actor (read-only requests)
    tx_query_statemachine: Option<mpsc::Sender<StateMachineQuery>>,

    tx_to_peers: HashMap<u64, mpsc::Sender<RPC>>,
    rx_from_peers: Option<mpsc::Receiver<(u64, RPC)>>,
    tx_to_server: mpsc::Sender<(u64, RPC)>,
    next_index: HashMap<u64, u64>,
    match_index: HashMap<u64, u64>,

    // Channel for sending commands to the leader (from client_task)
    cmd_to_server: mpsc::Sender<(u64, RPC)>,
    // Channel for receiving commands from the clients.
    rx_from_clients: Option<mpsc::Receiver<(u64, RPC)>>,

    // Channels for sending MessageToClient to update client about command status
    tx_to_clients: HashMap<u64, mpsc::Sender<RPC>>,
}

impl Server {
    pub fn new(_id: Id, _addr: String, _peers: Vec<std::sync::Arc<Peer>>) -> Self {
        todo!()
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        let (tx_to_state_machine, rx_from_log) = mpsc::channel::<StateMachineMsg>(100);
        let (tx_query_state_machine, mut rx_query) = mpsc::channel::<StateMachineQuery>(100);

        self.tx_to_statemachine = Some(tx_to_state_machine.clone());
        self.tx_query_statemachine = Some(tx_query_state_machine.clone());

        let mut owned_state_machine = StateMachine::new();

        let mut rx_from_log = rx_from_log;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    maybe_cmd = rx_from_log.recv() => {
                        match maybe_cmd {
                            Some(cmd) => {
                                match cmd {
                                    StateMachineMsg::Append(entry) => {
                                        owned_state_machine.log(entry);
                                    }
                                    StateMachineMsg::CommitTo(idx) => {
                                        owned_state_machine.update_commit_index(idx);
                                        owned_state_machine.store();
                                    }

                                    StateMachineMsg::ShutDown => { todo!()
                                    }

                                }
                            }
                            None => {
                                break;
                            }
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
                            None => {
                                break;
                            }
                        }
                    }
                }
            }
        });

        let mut rx = self
            .rx_from_peers
            .take()
            .expect("rx_from_peers must be Some when run() is called");

        let peers_to_connect: Vec<std::sync::Arc<Peer>> = {
            let guard = self.peers.lock().await;
            guard.iter().cloned().collect()
        };

        for peer in peers_to_connect {
            let addr = peer.addr.clone();
            let peer_id = peer.id.clone().get_id();
            let tx_to_server = self.tx_to_server.clone();

            let (tx_to_peer, rx_from_server) = mpsc::channel::<RPC>(100);
            self.tx_to_peers.insert(peer_id, tx_to_peer);

            let stream = TcpStream::connect(addr).await.map_err(|e| Error::Io(e))?;
            let (reader, writer) = stream.into_split();

            tokio::spawn(async move {
                peer_task(peer_id, reader, writer, tx_to_server, rx_from_server).await;
            });
        }

        for (id, addr) in &self.client {
            let cloned_id = *id;
            let cmd_to_server = self.cmd_to_server.clone();

            let (tx_to_client, rx_from_server) = mpsc::channel::<RPC>(100);
            self.tx_to_clients.insert(cloned_id, tx_to_client);

            let stream = TcpStream::connect(addr).await.map_err(|e| Error::Io(e))?;
            let (reader, writer) = stream.into_split();
            tokio::spawn(async move {
                client_task(cloned_id, reader, writer, cmd_to_server, rx_from_server).await;
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

        for &peer_id in self.tx_to_peers.keys() {
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

        let cluster_size = self.tx_to_peers.len() + 1;
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

        let mut work: Vec<(u64, mpsc::Sender<RPC>)> = Vec::with_capacity(self.tx_to_peers.len());
        for (&pid, tx) in &self.tx_to_peers {
            work.push((pid, tx.clone()));
        }

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

                                        if let Some(tx) = self.tx_to_peers.get(&peer_id) {
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
                                        if let Some(tx) = self.tx_to_peers.get(&peer_id) {
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
                        RPC::CommandRequest( req) =>{
                            let client_cmd = req.command;
                            let entry = LogEntry {
                                term: self.term,
                                command: client_cmd,
                            };
                            let tx = match &self.tx_to_statemachine {
                                Some(tx) => tx.clone(),
                                None => return Err(Error::Internal("state machine channel missing".into())),
                            };
                            tx.send(StateMachineMsg::Append(entry)).await.map_err(|_| Error::FailedToSendToStateMachine)?;
                            self.send_replication_round().await?;
                        },
                        RPC::WhoIsTheLeader(req) => {
                            let id = self.id.get_id() as u64;
                            let resp = RPC::IAmTheLeader(
                                IAmTheLeader{
                                    id,}

                                );
                            if let Some(tx_to_client) = self.tx_to_clients.get(&client_id){
                                tx_to_client.send(resp);

                            }
                            eprint!("Coudn't access server sender");


                        },
                        _ =>{
                        },

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
            let majority = (self.tx_to_peers.len() / 2) + 1;
            self.term += 1;
            self.voted_for = Some(self.id);
            let mut votes: usize = 1;

            let request_vote_request = RequestVoteRequest {
                term: self.term,
                candidate_id: self.id,
                last_log_term: self.last_log_term,
                last_log_index: self.sm_last_log_index().await?.unwrap_or(0),
            };

            for (_, sender) in &self.tx_to_peers {
                let _ = sender
                    .send(RPC::RequestVoteRequest(request_vote_request.clone()))
                    .await;
            }

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
        let ms = rand::thread_rng().gen_range(150..=300);
        Duration::from_millis(ms)
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

                    if let Some(peer_tx) = self.tx_to_peers.get(&peer_id) {
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

        if let Some(peer_tx) = self.tx_to_peers.get(&peer_id) {
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

pub async fn client_task(
    client_id: u64,
    reader: OwnedReadHalf,
    mut writer: OwnedWriteHalf,
    cmd_to_server: mpsc::Sender<(u64, RPC)>,
    mut rx_from_server: mpsc::Receiver<RPC>,
) {
    let mut line = BufReader::new(reader).lines();

    loop {
        tokio::select! {
            line = line.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        match serde_json::from_str::<RPC>(&line) {
                            Ok(rpc) => {
                                if let Err(e) = cmd_to_server.send((client_id,rpc)).await {
                                    eprintln!("Failed to send RPC to server: {}", e);
                                    break;
                                }
                            }
                            Err(e) => {
                                eprintln!("Failed to parse RPC from client {}: {}",client_id, e);
                            }
                        }
                    }
                    Ok(None) => {
                        println!("client {} disconnected", client_id);
                        break;
                    }
                    Err(e) => {
                        eprintln!("Error reading from client {}: {}", client_id, e);
                        break;
                    }
                }
            }

            Some(msg) = rx_from_server.recv() => {
                match serde_json::to_string(&msg) {
                    Ok(serialized) => {
                        if let Err(e) = writer.write_all(serialized.as_bytes()).await {
                            eprintln!("Failed to write to client {}: {}", client_id, e);
                            break;
                        }
                        if let Err(e) = writer.write_all(b"\n").await {
                            eprintln!("Failed to write newline to client {}: {}", client_id, e);
                            break;
                        }
                    }
                    Err(e) => eprintln!("Failed to serialize RPC for client {}: {}", client_id, e),
                }
            }
        }
    }
}

pub async fn peer_task(
    peer_id: u64,
    reader: OwnedReadHalf,
    mut writer: OwnedWriteHalf,
    tx_to_server: mpsc::Sender<(u64, RPC)>,
    mut rx_from_server: mpsc::Receiver<RPC>,
) {
    let mut lines = BufReader::new(reader).lines();

    loop {
        tokio::select! {
            line = lines.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        match serde_json::from_str::<RPC>(&line) {
                            Ok(rpc) => {
                                if let Err(e) = tx_to_server.send((peer_id, rpc)).await {
                                    eprintln!("Failed to send RPC to server: {}", e);
                                    break;
                                }
                            }
                            Err(e) => {
                                eprintln!("Failed to parse RPC from peer {}: {}", peer_id, e);
                            }
                        }
                    }
                    Ok(None) => {
                        println!("Peer {} disconnected", peer_id);
                        break;
                    }
                    Err(e) => {
                        eprintln!("Error reading from peer {}: {}", peer_id, e);
                        break;
                    }
                }
            }

            Some(msg) = rx_from_server.recv() => {
                match serde_json::to_string(&msg) {
                    Ok(serialized) => {
                        if let Err(e) = writer.write_all(serialized.as_bytes()).await {
                            eprintln!("Failed to write to peer {}: {}", peer_id, e);
                            break;
                        }
                        if let Err(e) = writer.write_all(b"\n").await {
                            eprintln!("Failed to write newline to peer {}: {}", peer_id, e);
                            break;
                        }
                    }
                    Err(e) => eprintln!("Failed to serialize RPC for peer {}: {}", peer_id, e),
                }
            }
        }
    }

    println!("Peer task {} exiting", peer_id);
}
