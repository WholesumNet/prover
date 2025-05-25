use serde::{
    Serialize, Deserialize
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobType {
    Keccak([u8; 32]),

    Zkr([u8; 32]),

    Segment(u32),

    Join(u32),
    
    Groth16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proof {   
    // job_id assigned by the client 
    pub client_job_id: String,
    // job_id from prover pov: job_id+item_id
    pub job_id: String,

    pub job_type: JobType,

    // the client
    pub owner: String,

    pub input_blobs: Vec<Vec<u8>>,    
    pub blob: Vec<u8>,    
    pub hash: u128,

}
