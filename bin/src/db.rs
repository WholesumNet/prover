use serde::{
    Serialize, Deserialize
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProveKind {
    Assumption(String),

    Segment(String),

    Join(String),
    
    Groth16(String),

    SP1ProveCompressedSubblock(String),
    SP1ProveCompressedAgg(String)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proof {  
    // as specified by the client 
    pub job_id: String,

    // hash of the input blobs
    pub input_hashes: Vec<String>,   

    pub kind: ProveKind,

    // the client
    pub owner: Vec<u8>,

    pub blob: Vec<u8>,    
    pub hash: String,
}
