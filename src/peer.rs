use std::sync::Arc;
use std::sync::Mutex;
use std::{error, net::ToSocketAddrs};

// The main communciation line between nodes. The server will hold id and addrs of these peers to
// communicate to the rest of the peers.
use crate::id::{self, Id};
use crate::rpc::RPC;
use std::error::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

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
}
