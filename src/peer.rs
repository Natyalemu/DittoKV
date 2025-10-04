use crate::id::{self, Id};
use std::clone::Clone;
use tokio::net::TcpStream;

#[derive(Clone)]
pub struct Peer {
    pub id: Id,
    pub addr: String,
}
impl Peer {
    pub fn new(id: Id, stream: TcpStream, addr: String) -> Self {
        Self { id, addr }
    }
    //This method implements the behavior of a follower server by monitoring incoming RPC messages
    // and handling them appropriately.
    // 1) If an AppendEntriesRequest is received from the leader, the follower appends the log.
    // 2) If a RequestVoteRequest is received from a candidate, the follower processes the election accordingly.
    pub async fn run_follower(&mut self) {
        loop {}
    }
    pub fn get_id(self) -> u64 {
        self.id.get_id()
    }
    pub fn id(&self) -> u64 {
        self.id.get_id()
    }
}
