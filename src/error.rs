use std::fmt;
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
    NoLeader,
}
impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Error => write!(f, "Generic Error"),
            Error::FailedToGetTheKey => write!(f, "Failed to get the key"),
            Error::FailedToSendRPC => write!(f, "Failed to send RPC"),
            Error::ChannelClosed => write!(f, "Channel closed"),
            Error::SendError => write!(f, "Send error"),
            Error::Internal(msg) => write!(f, "Internal error: {}", msg),
            Error::FailedToSendToStateMachine => write!(f, "Failed to send to state machine"),
            Error::Io(e) => write!(f, "IO error: {:?}", e),
            Error::NoLeader => write!(f, "No leader found"),
        }
    }
}
