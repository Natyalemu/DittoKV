use std::net::{TcpListener, ToSocketAddrs};
use std::string;

use crate::error::Error;
use crate::id::Id;
use crate::log::{cmd, log};
use crate::peer::Peer;
use crate::role::Role;
use crate::rpc::*;
use crate::rpc::*;
use crate::state_machine::StateMachine;
use tokio::net::TcpStream;

struct Server {
    id: Id,
    state_machine: StateMachine,
    peers: Vec<Peer>,
    listener: TcpListener,
    term: u64,
    role: Role,
    client: Vec<TcpStream>,
    voted: bool,
}
struct Cluster {
    servers: Vec<Server>,
    peers: Vec<Peer>,
}

impl Server {
    //A new server is created with a given number of peers, depending on its order of creation.
    // The first server receives an empty vector because no other servers are active, but
    // successive servers receive knowledge about the active servers and consequently will
    // track these servers as peer.
    pub fn new(id: Id, addr: String, peers: Vec<Peer>) {
        let state_machine = StateMachine::new();
        let listener = TcpListener::bind(addr).unwrap();

        let client: Vec<TcpStream> = Vec::new();
        let sever = Server {
            id,
            state_machine,
            peers,
            listener,
            term: 0,
            role: Role::Follower,
            client,
            voted: false,
        };
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
        loop {
            //Assumptions:
            // 1) All members of the cluster are provided with each other’s addresses.
            //    Therefore, each server will try to connect to these servers.
            // 2) For each incoming client request, each server will spawn a new thread to handle the request.

            if self.role = Role::Follower {}

            if self.role = Role::Leader {}
            if self.role = Role::Candidate {}
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
}
