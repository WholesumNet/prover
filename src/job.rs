use libp2p::PeerId;

#[derive(Debug, PartialEq, Eq)]
pub enum Status {    
    Running,
    
    // param: proof's cid
    ExecutionSucceeded(String),
    
    // param: error message
    ExecutionFailed(String),

    // param: error message
    UploadFailed(String),
}

#[derive(Debug, PartialEq, Eq)]
pub enum JobType {
    Prove(u32),
    Join(String, String),
    Groth16,
}


// maintain lifecycle of a job
#[derive(Debug)]
pub struct Job {
    // job id as specified by the client
    pub base_id: String, 
    
    // the client
    pub owner: PeerId,

    pub status: Status,

    pub proof_file_path: Option<String>,

    pub job_type: JobType,
}

