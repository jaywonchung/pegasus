use thiserror::Error;

#[derive(Error, Debug)]
pub enum PegasusError {
    #[error("Failed to connect SSH session or execute SSH command: {0}")]
    SshError(#[from] openssh::Error),
    #[error("Failed to execute local command: {0}")]
    LocalCommandError(#[from] std::io::Error),
}
