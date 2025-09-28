use crate::error::Error;
use crate::id::Id;
use crate::log::{cmd, log};
use crate::peer::{self, Peer};
use crate::role::Role;
use crate::rpc::RequestVoteResponse;
use crate::rpc::RPC;
use crate::rpc::*;
use crate::state_machine::StateMachine;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{TcpListener, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::select;
use tokio::time;

use tokio::sync::mpsc;
struct Server {
    id: Id,
    state_machine: Arc<StateMachine>,
    peers: Mutex<Vec<Arc<Peer>>>,
    listener: TcpListener,
    term: u64,
    role: Role,
    client: Vec<TcpStream>,
    last_log_term: u64,
    commit_index: u64,
    voted_for: Option<Id>,
    tx_to_peers: HashMap<u64, mpsc::Sender<RPC>>,
    rx_from_peers: Option<mpsc::Receiver<(u64, RPC)>>,
    tx_to_server: mpsc::Sender<(u64, RPC)>,
    next_index: HashMap<u64, u64>,
    match_index: HashMap<u64, u64>,
}

impl Server {
    //A new server is created with a given number of peers, depending on its order of creation.
    // The first server receives an empty vector because no other servers are active, but
    // successive servers receive knowledge about the active servers and consequently will
    // track these servers as peer.
    pub fn new(id: Id, addr: String, peers: Vec<Arc<Peer>>) {
        todo!();
    }

    // The server runs in a loop, performing different actions based on its current role:
    // 1) If the server is in the follower state, it listens to the leader to perform commands issued by the leader.
    // If a client sends a command directly to a follower, the server returns an error to the client.
    // If a timeout is reached without receiving a heartbeat from the leader, the server nominates itself for election.
    // 2) If the server is in the leader state, the run function listens for client commands, applies them to the state machine,
    // and distributes the commands to the remaining peers.
    // 3) If the server is in the candidate state, it remains in this state until it receives votes from more than half of the peers,
    // or until a notification is received from a new leader.

    pub async fn run(&mut self) -> Result<(), crate::error::Error> {
        //Assumptions:
        // 1) All members of the cluster are provided with each other’s addresses.
        // 2)Therefore, each server will try to connect to these servers.
        // 3) For each incoming client request, each server will spawn a new thread to handle the request.

        let mut rx = self
            .rx_from_peers
            .take()
            .expect("rx_from_peers must be Some when run() is called");

        let peers_to_connect: Vec<Arc<Peer>> = {
            let guard = self.peers.lock().unwrap();
            guard.iter().cloned().collect()
        };

        for peer in peers_to_connect {
            let addr = peer.addr.clone();
            let peer_id = peer.id.clone().get_id();
            let tx_to_server = self.tx_to_server.clone();

            let (tx_to_peer, rx_from_server) = mpsc::channel::<RPC>(100);
            self.tx_to_peers.insert(peer_id, tx_to_peer);

            let stream = TcpStream::connect(addr).await.unwrap();
            let (reader, writer) = stream.into_split();

            tokio::spawn(async move {
                peer_task(peer_id, reader, writer, tx_to_server, rx_from_server).await;
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
                                None => {
                                    return Err(crate::error::Error::ChannelClosed);
                                }
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
                    self.leader_handler(&mut rx);
                }
            }
        }
    }
    //leader starts here
    fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(50)
    }

    fn init_leader_state(&mut self) {
        let last_index = self.state_machine.as_ref().last_log_index().unwrap_or(0);
        self.commit_index = self.state_machine.commit_index();

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
    }

    fn build_append_for_peer(&self, peer_id: u64) -> Option<AppendEntryRequest> {
        let next_idx = *self.next_index.get(&peer_id)?;
        let prev_index = next_idx.saturating_sub(1);
        let prev_term = if prev_index == 0 {
            0
        } else {
            self.state_machine.entry_term(prev_index).unwrap_or(0)
        };

        let entry = self.state_machine.get_entry(next_idx);

        Some(AppendEntryRequest {
            term: self.term,
            leader_id: self.id,
            prev_log_index: prev_index,
            prev_log_term: prev_term,
            entry, // Option<LogEntry> where None => heartbeat
            leader_commit: self.commit_index,
        })
    }

    fn try_advance_commit(&mut self) {
        let mut match_indexes: Vec<u64> = self.match_index.values().copied().collect();

        let leader_last = self.state_machine.last_log_index().unwrap_or(0);
        match_indexes.push(leader_last);

        match_indexes.sort_unstable_by(|a, b| b.cmp(a));

        let majority_count = (self.tx_to_peers.len() + 1) / 2; // peers excludes leader
        if majority_count >= match_indexes.len() {
            return;
        }
        let candidate_n = match_indexes[majority_count];

        if candidate_n > self.commit_index {
            if let Some(term_at_n) = self.state_machine.entry_term(candidate_n) {
                if term_at_n == self.term {
                    self.commit_index = candidate_n;
                    // update state machine's commit index / apply entries
                    let _ = Arc::get_mut(&mut self.state_machine)
                        .unwrap()
                        .update_commit_index(self.commit_index);
                }
            }
        }
    }

    /// Send either heartbeat (entry = None) or a real AppendEntryRequest to each follower,
    /// using next_index to decide which entry to send.
    async fn send_replication_round(&self) {
        let term = self.term;
        let leader_id = self.id;
        let leader_commit = self.commit_index;
        let prev_log_index = self.state_machine.last_log_index().unwrap_or(0);
        let prev_log_term = self.state_machine.last_log_term().unwrap_or(0);

        let mut work: Vec<(u64, mpsc::Sender<RPC>)> = Vec::with_capacity(self.tx_to_peers.len());
        for (&pid, tx) in &self.tx_to_peers {
            work.push((pid, tx.clone()));
        }

        for (pid, tx) in work {
            let next_idx = self.next_index.get(&pid).copied().unwrap_or(1);
            let last_idx = self.state_machine.last_log_index().unwrap_or(0);

            let req = if next_idx <= last_idx {
                self.build_append_for_peer(pid)
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
    }

    /// Leader main loop. `rx` should be the receiver taken in `run()` and passed in.
    pub async fn leader_handler(
        &mut self,
        rx: &mut mpsc::Receiver<(u64, RPC)>,
    ) -> Result<(), Error> {
        self.init_leader_state();
        self.send_replication_round().await;

        let mut ticker = time::interval(self.heartbeat_interval());
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                maybe = rx.recv() => {
                    match maybe {
                        Some((peer_id, rpc)) => {
                            match rpc {
                                RPC::AppendEntryResponse(resp) => {
                                    if resp.term > self.term {
                                        // step down
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
                                        self.try_advance_commit();
                                    } else {
                                        let ni = self.next_index.entry(peer_id).or_insert(1);
                                        if *ni > 1 { *ni -= 1; } else { *ni = 1; }

                                        if let Some(tx) = self.tx_to_peers.get(&peer_id) {
                                            if let Some(req) = self.build_append_for_peer(peer_id) {
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

                                _ => { /* ignore */ }
                            }
                        }
                        None => return Err(Error::ChannelClosed),
                    }
                }

                _ = ticker.tick() => {
                    self.send_replication_round().await;
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
                last_log_index: self.state_machine.as_ref().last_log_index().unwrap_or(0),
            };

            // send vote request to all peers
            for (_, sender) in &self.tx_to_peers {
                let _ = sender
                    .send(RPC::RequestVoteRequest(request_vote_request.clone()))
                    .await;
            }

            let mut timeout = tokio::time::sleep(self.random_election_timeout_duration());
            tokio::pin!(timeout);

            // wait for responses until either we win or timeout again
            while votes < majority {
                tokio::select! {
                    maybe = rx.recv() => {
                        match maybe {
                            Some((_, rpc)) => {
                                match rpc {
                                    RPC::RequestVoteResponse(resp) => {
                                        if resp.term > self.term {
                                            // found newer term, step down
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
                                        // found valid leader
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
                        // election timed out: restart with a new term
                        break; // exits inner loop, goes back to outer loop -> new election
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
        //1)Handle follower receives an rpc from the peer task.
        //2)Log change into the statemahcine.
        //3)Browse through the Hasmap using the id sent by the peers task to find the
        // respective sending channel then send AppendEntryResponse to peer task which is then
        // handled properly by the task created.
        match rpc {
            RPC::AppendEntryRequest(req) => match req.entry {
                Some(entry) => {
                    /// Note: The state machine only records changes in the log. A separate task is required
                    /// to synchronize this log with the state machine's persistent storage (a BTree).
                    /// Implementation: The Raft actor (leader or follower) can send a notification via a channel
                    /// to this dedicated storage task whenever new entries are committed (e.g., via `ready_to_apply`).
                    /// Upon notification, the task will apply log entries up to the current commit index,
                    /// thus updating the permanent storage.
                    if let Some(state_machine) = Arc::get_mut(&mut self.state_machine) {
                        state_machine.log(entry);

                        let leader_commit_index = req.leader_commit;
                        state_machine.update_commit_index(leader_commit_index);
                        self.last_log_term =
                            state_machine.last_log_term().unwrap_or(self.last_log_term);
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
                let up_to_date = (req.last_log_term > self.last_log_term)
                    || (req.last_log_term == self.last_log_term
                        && req.last_log_index
                            >= self.state_machine.as_ref().last_log_index().unwrap_or(0));

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
}
/// Task used for a communication line between a server and the different nodes. The server running
/// on the machine talk to these different task throught channel and these channels communicate
/// with the respective nodes by the tcp address provided.
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
