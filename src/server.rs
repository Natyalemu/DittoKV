use crate::error::Error;
use crate::id::Id;
use crate::log::{cmd, log};
use crate::peer::{self, Peer};
use crate::role::Role;
use crate::rpc::*;
use crate::rpc::RPC;
use crate::rpc::RequestVoteResponse;
use crate::state_machine::StateMachine;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{TcpListener, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;



struct Server {
    id: Id,
    state_machine: StateMachine,
    peers: Mutex<Vec<Arc<Peer>>>,
    listener: TcpListener,
    term: u64,
    role: Role,
    client:Vec<TcpStream>,
    last_log_term: u64,
    voted_for: Option<Id>,
    tx_to_peers: HashMap<u64, mpsc::Sender<RPC>>,
    rx_from_peers: mpsc::Receiver<(u64, RPC)>,
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

    pub async fn run(&mut self) -> crate::error::Error {
        //Assumptions:
        // 1) All members of the cluster are provided with each other’s addresses.
        //    Therefore, each server will try to connect to these servers.
        // 2) For each incoming client request, each server will spawn a new thread to handle the request.
        //

        let mut peers = self.peers.lock().unwrap();
        for peer in peers.iter_mut() {
            let stream = TcpStream::connect(peer.addr).await.unwrap();
            let (reader, writer) = stream.into_split();
            let (tx_to_peer, rx_from_server) = mpsc::channel::<RPC>(100);
            self.tx_to_peers.insert(peer.get_id(), tx_to_peer);
            let tx_to_server = self.tx_to_server.clone();

            let cloned_peer = Arc::clone(peer);
            tokio::spawn(async move {
                peer_task(
                    cloned_peer.get_id(),
                    reader,
                    writer,
                    tx_to_server,
                    rx_from_server,
                )
                .await;
            });
        }
        loop {
            match self.role {
                Role::Follower => {
                    tokio::select! {
                        Some((peer_id, rpc)) = self.rx_from_peers.recv() => {
                            self.handle_follower(peer_id, rpc).await;
                        }
                        _ = self.election_timeout() => {
                            self.become_candidate();
                        }
                    }
                }
                Role::Candidate => { /* ... */ }
                Role::Leader => { /* ... */ }
            }
        }
    }
    //Helper function for filtering election requirement

    pub fn elect(&mut self, vote_request: RequestVoteRequest) -> RequestVoteResponse {
        if vote_request.last_log_term > self.term {
            RequestVoteResponse {
                term: vote_request.term,
                vote_granted: true,
            }
        } else {
            RequestVoteResponse {
                term: vote_request.term,
                vote_granted: false,
            }
        }
    }

    pub async fn handle_follower(&mut self,peer_id:u64, rpc: RPC)-> Result<(), Error> {
        //1)Handle follower receives an rpc from the peer task.
        //2)Log change into the statemahcine.
        //3)Browse through the Hasmap using the id sent by the peers task to find the
        // respective sending channel then send AppendEntryResponse to peer task which is then
        // handled properly by the task created.
        match rpc {
            RPC::AppendEntryRequest(req) => {
                self.state_machine.log(req.entry);
                self.last_log_term;
                if let Some(peer_tx) = self.tx_to_peers.get(&peer_id) {
                    let _ = peer_tx.send(RPC::AppendEntryResponse(AppendEntryResponse {
                        term: self.term,
                        success: true,}))
                                .await;
                    Ok(())
                        }
                else {
                    Err(Error::FailedToGetTheKey)
                }
                
                    }
            RPC::RequestVoteResponse(req){
                self.handle_request_vote(peer_id, req); }
                
            
        }
    }
   pub async fn handle_request_vote(
    &mut self,
    peer_id: u64,
    req: RequestVoteRequest,
) -> Result<(), Error> {
    // Step 1: Determine vote grant
    let vote_granted = if req.term < self.term {
        false
    } else {
        if req.term > self.term {
            self.term = req.term;
            self.voted_for = None;
        }

        if let Some(  voted_for) = self.voted_for.as_ref() {
            if voted_for != &req.candidate_id {
                false
            } else {
                true
            }
        } else {
         
            // check log freshness
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

    // Step 2: Send response over the peer channel
    if let Some(peer_tx) = self.tx_to_peers.get(&peer_id) {
        let response = RPC::RequestVoteResponse(RequestVoteResponse {
            term: self.term,
            vote_granted,
        });

        peer_tx.send(response).await.map_err(|_| Error::FailedToSendRPC)?;
        Ok(())
    } else {
        Err(Error::FailedToGetTheKey)
    }
}

    


    fn log_term(&mut self){
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
