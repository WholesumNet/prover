use libp2p::PeerId;

#[derive(Debug, PartialEq, Eq)]
pub enum Status {    
    Running,
    
    ExecutionSucceded,
    
    // param: error message
    ExecutionFailed(String),
}

#[derive(Debug, PartialEq, Eq)]
pub enum JobType {
    Segment(u32),
    Join(u32),
    Groth16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proof {
    pub blob: Vec<u8>
}

// maintain lifecycle of a job
#[derive(Debug)]
pub struct Job {
    // job id as specified by the client
    pub base_id: String,

    // job_id as we know it: base_id+item_id
    pub id: String, 

    // pub working_dir: String,
    
    // the client
    pub owner: PeerId,

    pub status: Status,

    pub job_type: JobType,

    pub input_blobs: Vec<Vec<u8>>,

    // result of the job
    pub proof: Option<Proof>
}

