use libp2p::PeerId;
use home;

use comms::compute::ComputeType;
use anyhow;

#[derive(Debug)]
pub struct Residue {
    pub fd12_cid: Option<String>,
    pub receipt_cid: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Status {
    DockerWarmingUp = 0,    // docker initializing
    Negotiating,            // hammering deals
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
    let binding = home::home_dir()
        .ok_or_else(|| anyhow::Error::msg("Home dir is not available"))?;
    let home_dir = binding.to_str()
        .ok_or_else(|| anyhow::Error::msg("Home dir is not available"))?;
        // .ok_or_else(|| Box::<dyn Error>::from("Home dir is not available."))?
        // .into_os_string().into_string()
        // .or_else(|_| Err(Box::<dyn Error>::from("OS_String conversion failed.")))?;
    Ok(format!("{home_dir}/.wholesum/jobs"))
}
