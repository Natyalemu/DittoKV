//Raft's client
use crate::error::Error;
use crate::log::cmd::{Command, Set};
use crate::peer::Peer;
use crate::rpc::{CommandRequest, RPC};
use crate::rpc::{IAmTheLeader, WhoIsTheLeader};
use serde_json;
use std::collections::HashMap;
use std::hash::Hasher;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};

pub struct RaftClient {
    leader: Option<u64>,
    nodes: Vec<Peer>,
    tx_to_nodes: HashMap<u64, mpsc::Sender<RPC>>,
    tx_to_client: mpsc::Sender<RPC>,
    rx_from_nodes: mpsc::Receiver<RPC>,
}

impl RaftClient {
    pub async fn new(nodes: Vec<Peer>, id: u64) -> Result<Self, Error> {
        let (tx_to_client, rx_from_nodes) = mpsc::channel::<RPC>(256);
        let mut tx_to_nodes: HashMap<u64, mpsc::Sender<RPC>> = HashMap::new();
        let mut discovered_leader: Option<u64> = None;

        for peer in nodes.iter() {
            let addr = peer.addr.clone();
            let node_id = peer.id() as u64;

            let stream = match TcpStream::connect(addr).await {
                Ok(s) => s,
                Err(_e) => continue,
            };

            let (read_half, write_half) = stream.into_split();
            let mut reader = BufReader::new(read_half);
            let mut writer = write_half;

            let client_id: u64 = id;
            let hello = serde_json::json!({
                "type": "client",
                "client_id": client_id,
            });
            let hello_serialized = serde_json::to_string(&hello)
                .map_err(|e| Error::Internal(format!("serde error: {}", e)))?;
            if writer.write_all(hello_serialized.as_bytes()).await.is_err() {
                continue;
            }
            if writer.write_all(b"\n").await.is_err() {
                continue;
            }

            let who_is = RPC::WhoIsTheLeader(WhoIsTheLeader {});
            let who_is_ser = serde_json::to_string(&who_is)
                .map_err(|e| Error::Internal(format!("serde error: {}", e)))?;
            if writer.write_all(who_is_ser.as_bytes()).await.is_err() {
                continue;
            }
            if writer.write_all(b"\n").await.is_err() {
                continue;
            }

            let mut first_response = String::new();
            match timeout(
                Duration::from_millis(800),
                reader.read_line(&mut first_response),
            )
            .await
            {
                Ok(Ok(0)) | Ok(Err(_)) | Err(_) => {}
                Ok(Ok(_n)) => {
                    if let Ok(rpc) = serde_json::from_str::<RPC>(first_response.trim()) {
                        if let RPC::IAmTheLeader(IAmTheLeader { id }) = rpc {
                            if discovered_leader.is_none() {
                                discovered_leader = Some(id);
                            }
                        }
                    }
                }
            }

            let (tx_to_node, mut rx_from_client) = mpsc::channel::<RPC>(256);
            let tx_to_client_clone = tx_to_client.clone();

            tokio::spawn(async move {
                peer_task_handler(node_id, tx_to_client_clone, rx_from_client, writer, reader)
                    .await;
            });

            tx_to_nodes.insert(node_id, tx_to_node);
        }

        Ok(RaftClient {
            leader: discovered_leader,
            nodes,
            tx_to_nodes,
            tx_to_client,
            rx_from_nodes,
        })
    }
    pub async fn set(&self, key: String, value: String) -> Result<(), Error> {
        let command = Command::Set(Set { key, value });
        let rpc = RPC::CommandRequest(CommandRequest { command });
        if let Err(e) = serde_json::to_string(&rpc) {
            eprintln!("failed to serialize command to rpc");
        }

        let leader_id = self.leader.unwrap().clone();
        let tx_to_leader = match self.tx_to_nodes.get(&leader_id) {
            Some(tx) => tx,
            None => {
                eprintln!("failed to access sender to leader");
                return Err(Error::NoLeader);
            }
        };

        if let Err(e) = tx_to_leader.send(rpc).await {
            eprintln!("client failed to send to task handler");
        }

        Ok(())
    }
}
pub async fn peer_task_handler(
    node_id: u64,
    tx_to_client: mpsc::Sender<RPC>,
    mut rx_from_client: mpsc::Receiver<RPC>,
    mut writer: OwnedWriteHalf,
    reader: BufReader<OwnedReadHalf>,
) {
    let mut line = BufReader::new(reader).lines();

    loop {
        tokio::select! {
            line = line.next_line() => {
                match line {
                    Ok(Some(line) )=> {
                        match serde_json::from_str::<RPC>(&line) {
                            Ok(rpc) => {
                                if let Err(e) = tx_to_client.send(rpc).await{
                                    eprintln!("Failed to send to client");

                                }



                            }
                            Err(e) => {
                            eprintln!("Failed to parse RPC from server {}", node_id);
                        }


                        }


                    }
                    Ok(None) => {
                    println!("Server {} disconnected", node_id);
                    break;
                }
                Err(e) => {
                    eprintln!("Error reading from server {}", node_id);
                    break;
                }



                }




            }

            Some(rpc)= rx_from_client.recv() =>{
                match serde_json::to_string(&rpc) {
                    Ok(serialized) =>{
                        if let Err(e) = writer.write_all(serialized.as_bytes()).await{
                            eprintln!("Failed to write to server {}", node_id);
                            break;

                        }
                        if let Err(e) = writer.write_all(b"\n").await {
                        eprintln!("Failed to write newline to server {}: {}", node_id, e);
                        break;
                    }


                    }
                    Err(e) => eprintln!("Failed to serialize RPC for server {}: {}", node_id, e),






                }


            }
        }
    }
    println!("Server task {} exiting", node_id);
}
