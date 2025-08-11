use std::{error, net::ToSocketAddrs};

// The main communciation line between nodes. The server will hold id and addrs of these peers to
// communicate to the rest of the peers.
use crate::id::{self, Id};
use std::error::Error;
use tokio::net::TcpStream;

struct Peer {
    id: Id,
    addr: TcpStream,
}

impl Peer {
    fn new(id: Id, addr: TcpStream) -> Self {
        Self { id, addr }
    }
}
