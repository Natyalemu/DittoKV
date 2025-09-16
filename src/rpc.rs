use crate::{id::Id, log::log::LogEntry};
use serde::{Deserialize, Serialize};

//This should be updated so that the AppendEntryRequest transports number of LogEntry for efficient
//communication between leader and followers.
#[derive(Serialize, Deserialize, Debug)]
pub enum RPC {
    AppendEntryRequest(AppendEntryRequest),
    AppendEntryResponse(AppendEntryResponse),
    RequestVoteRequest(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
    InstallSnapshotRequest(InstallSnapshotRequest),
    InstallSnapshotResponse(InstallSnapshotResponse),
}
#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntryRequest {
    pub term: u64,
    pub leader_id: Id,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entry: LogEntry,
    pub leader_commit: u64,
}
impl AppendEntryRequest {
    pub fn get_entry(&self) -> LogEntry {
        self.entry.clone()
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntryResponse {
    pub term: u64,
    pub success: bool,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    pub term: u64,
    pub candidate_id: Id,
    pub last_login_index: u64,
    pub last_log_term: u64,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    pub term: u64,
    pub leader_id: u64,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub offset: u64,
    pub data: Vec<u8>,
    pub done: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    pub term: u64,
}
