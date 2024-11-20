use libp2p::PeerId;
use comms::compute::ComputeType;
use anyhow;

#[derive(Debug)]
pub struct Residue {
    pub receipt_cid: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Status {
    Running,
    ExecutionSucceeded,     // execution finished successfully but blobs need to be uploaded
    ExecutionFailed(String),
    HarvestReady,           // blobs are upload and the job is ready to be harvested 
}

// maintain lifecycle of a job
#[derive(Debug)]
pub struct Job {
    pub id: String,
    pub owner: PeerId,       // the client
    pub status: Status,
    pub residue: Residue,    // cids for stderr, output, receipt, ...
    pub compute_type: ComputeType,
}

