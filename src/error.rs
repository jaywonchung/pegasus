use thiserror::Error;

#[derive(Error, Debug)]
pub enum PegasusError {
    #[error("failed to connect SSH session or execute SSH command")]
    SshError(#[from] openssh::Error),
    #[error("failed to execute local command")]
    LocalCommandError(#[from] std::io::Error),
}
