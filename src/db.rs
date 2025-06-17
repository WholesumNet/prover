use serde::{
    Serialize, Deserialize
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobKind {
    Assumption(String),

    Segment(String),

    Join(String),
    
    Groth16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proof {   
    // job_id assigned by the client 
    pub client_job_id: String,
    // job_id from prover pov: job_id+item_id
    pub job_id: String,

    pub kind: JobKind,

    // the client
    pub owner: Vec<u8>,

    pub input_blobs: Vec<Vec<u8>>,    
    pub blob: Vec<u8>,    
    pub hash: String,
}
