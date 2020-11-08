use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct ListResponse {
    pub paths: Vec<PathBuf>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SharedLockResponse {
    pub lock_id: Uuid,
}
