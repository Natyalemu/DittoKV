use std::io;
use tokio::task::JoinError;
pub enum Error {
    Error,
    FailedToGetTheKey,
    FailedToSendRPC,
    ChannelClosed,
    SendError,
    Internal(String),
    FailedToSendToStateMachine,
    Io(io::Error),
}
