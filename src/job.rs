use libp2p::PeerId;
use home;

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
    ExecutionFailed,
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

// get base residue path of the host
pub fn get_residue_path() -> anyhow::Result<String> {
    let err_msg = "Home dir is not available";
    let binding = home::home_dir()
        .ok_or_else(|| anyhow::Error::msg(err_msg))?;
    let home_dir = binding.to_str()
        .ok_or_else(|| anyhow::Error::msg(err_msg))?;
    Ok(format!("{home_dir}/.wholesum/jobs"))
}
