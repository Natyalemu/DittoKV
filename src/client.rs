//Raft's client
use crate::log::cmd::Command;
use crate::peer::Peer;
use crate::rpc::RPC;
use serde_json;
use std::collections::HashMap;
use std::hash::Hasher;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;

pub struct RaftCient {
    leader: u64,
    nodes: Vec<Peer>,
    tx_to_nodes: HashMap<u64, mpsc::Sender<RPC>>,
    tx_to_client: mpsc::Sender<RPC>,
    rx_from_nodes: mpsc::Receiver<RPC>,
}

impl RaftCient {
    pub async fn peer_task_handler(
        &self,
        node_id: u64,
        tx_to_client: mpsc::Sender<RPC>,
        mut rx_from_client: mpsc::Receiver<RPC>,
        mut writer: OwnedWriteHalf,
        reader: OwnedReadHalf,
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
}
