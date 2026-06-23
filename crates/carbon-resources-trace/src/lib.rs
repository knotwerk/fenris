use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourcesGateEvidence {
    pub gate: String,
    pub status: String,
}
