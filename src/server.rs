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
use tokio::sync::mpsc;
struct Server {
    id: Id,
    state_machine: StateMachine,
    peers: Mutex<Vec<Arc<Peer>>>,
    listener: TcpListener,
    term: u64,
    role: Role,
    client: Vec<TcpStream>,
    last_log_term: u64,
    voted_for: Option<Id>,
    tx_to_peers: HashMap<u64, mpsc::Sender<RPC>>,
    rx_from_peers: Option<mpsc::Receiver<(u64, RPC)>>,
    tx_to_server: mpsc::Sender<(u64, RPC)>,
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
                    self.candidate_handler(&mut rx).await;
                }

                Role::Leader => {}
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
                last_log_index: self.state_machine.last_log_index(),
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
                    self.state_machine.log(entry);
                    self.last_log_term = self.term;

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
                        && req.last_log_index >= self.state_machine.last_log_index());

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
