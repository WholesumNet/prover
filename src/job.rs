use std::collections::{
    HashMap, BTreeMap
};
use libp2p::PeerId;

#[derive(Debug, PartialEq, Eq)]
pub enum Status {
    WaitingForBlobs,

    Running,
    
    ExecutionSucceded,
    
    // param: error message
    ExecutionFailed(String),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Kind {
    Assumption(u128),

    // param: batch id
    Segment(u128),

    // param: batch id
    Join(u128),

    Groth16(u128),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proof {
    // xxh3_128
    pub hash: u128,

    pub blob: Vec<u8>
}

#[derive(Debug)]
pub struct Token {
    // xxh3_128
    pub hash: u128,

    pub owner: Vec<u8>
}

// maintain lifecycle of a job
#[derive(Debug)]
pub struct Job {
    // job id as specified by the client
    pub base_id: u128,

    // job_id as we know it: base_id+item_id
    pub id: String, 

    // pub working_dir: String,
    
    // the client
    pub owner: PeerId,

    pub status: Status,

    pub kind: Kind,

    pub input_blobs: BTreeMap<usize, Vec<u8>>,

    // must be satisfied before proving can start
    // <index, token>
    pub prerequisites: BTreeMap<usize, Token>,
    // inverse helper map to put received blobs
    pub pending_blobs: HashMap<u128, usize>,

    // result of the job
    pub proof: Option<Proof>
}

