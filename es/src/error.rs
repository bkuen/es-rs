#[derive(Debug, thiserror::Error)]
pub enum EsError {
    #[error("Invalid version")]
    InvalidVersion,

    #[cfg(feature = "postgres")]
    #[error("{0}")]
    DatabaseError(#[from] sqlx::Error),

    #[error("Transaction error")]
    TransactionError,
}