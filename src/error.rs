pub enum Error {
    Error,
    FailedToGetTheKey,
    FailedToSendRPC,
    ChannelClosed,
    SendError,
    Internal(String),
    FailedToSendToStateMachine,
}
