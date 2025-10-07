use std::sync::Arc;
use tokio::time::{sleep, Duration};

use ditto_kv::client::RaftClient;
use ditto_kv::id::Id;
use ditto_kv::peer::Peer;
use ditto_kv::role::Role;
use ditto_kv::server::RaftServer;
use ditto_kv::{self, id};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr1 = "127.0.0.1:7001".to_string();
    let addr2 = "127.0.0.1:7002".to_string();
    let id1 = Id::new(1);
    let id2 = Id::new(2);

    let peer1 = Peer::new(id1, addr1.clone());
    let peer2 = Peer::new(id2, addr2.clone());

    let listener1 = tokio::net::TcpListener::bind(&addr1).await?;
    let listener2 = tokio::net::TcpListener::bind(&addr2).await?;

    let peers_for_1: Vec<Arc<Peer>> = vec![Arc::new(peer2.clone())];
    let peers_for_2: Vec<Arc<Peer>> = vec![Arc::new(peer1.clone())];

    let mut server1 = RaftServer::new(Id::new(1u64), listener1, peers_for_1, Role::Leader);
    let mut server2 = RaftServer::new(Id::new(2u64), listener2, peers_for_2, Role::Follower);

    tokio::spawn(async move {
        if let Err(e) = server1.run().await {
            eprintln!("server1 exited with error: {:?}", e);
        } else {
            println!("server1 exited");
        }
    });

    tokio::spawn(async move {
        if let Err(e) = server2.run().await {
            eprintln!("server2 exited with error: {:?}", e);
        } else {
            println!("server2 exited");
        }
    });

    sleep(Duration::from_millis(200)).await;

    let client_nodes = vec![peer1.clone(), peer2.clone()];
    let mut client = RaftClient::new(client_nodes, 999u64)
        .await
        .map_err(|e| format!("failed to create client: {:?}", e))?;

    client
        .set("hello".into(), "world".into())
        .await
        .map_err(|e| format!("client set failed: {:?}", e))?;

    println!("client sent set command");

    sleep(Duration::from_secs(2)).await;

    println!("example finished");
    Ok(())
}
