use std::{error, net::ToSocketAddrs};

// The main communciation line between nodes. The server will hold id and addrs of these peers to
// communicate to the rest of the peers.
use crate::id::{self, Id};
use std::error::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

struct Peer {
    id: Id,
    stream: TcpStream,
}

impl Peer {
    fn new(id: Id, stream: TcpStream) -> Self {
        Self { id, stream }
    }
    async fn write<B: AsRef<[u8]>>(&mut self, data: B) -> Result<(), Box<dyn Error>> {
        self.stream.write_all(data.as_ref()).await?;

        Ok(())
    }
}
