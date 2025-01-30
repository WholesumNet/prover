use mongodb::bson::Bson;
use serde::{
    Serialize, Deserialize
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobType {
    Prove(u32),
    Join(String, String),
    Groth16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proof {   
    // job_id assigned by the client 
    pub client_job_id: String,
    // job_id from prover pov: job_id+item_id
    pub job_id: String,

    pub job_type: JobType,

    // ie the client
    pub owner: String,

    pub blob: Vec<u8>,
    pub blob_filepath: Option<String>,
    pub blob_cid: Option<String>,
}
