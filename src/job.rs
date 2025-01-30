use libp2p::PeerId;
use mongodb::bson::Bson;

#[derive(Debug, PartialEq, Eq)]
pub enum Status {    
    Running,
    
    // param: proof's cid
    ExecutionSucceeded(String),
    
    // param: error message
    ExecutionFailed(String),

    // param: failed upload retry attempts so far
    UploadFailed(u32),
}

#[derive(Debug, PartialEq, Eq)]
pub enum JobType {
    Prove(u32),
    Join(String, String),
    Groth16,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Proof {
    pub filepath: Option<String>,
    pub blob: Vec<u8>
}

// maintain lifecycle of a job
#[derive(Debug)]
pub struct Job {
    // job id as specified by the client
    pub base_id: String,

    // job_id as we know it: base_id+item_id
    pub id: String, 

    pub working_dir: String,
    
    // the client
    pub owner: PeerId,

    pub status: Status,

    pub job_type: JobType,

    // result of the job
    pub proof: Option<Proof>,

    // db oid
    pub db_oid: Option<Bson>, 
}

