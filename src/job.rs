use std::error::Error;

use libp2p::PeerId;
use home;

#[derive(Debug)]
pub struct Residue {
    pub fd12_cid: Option<String>,
    pub receipt_cid: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Status {
    DockerWarmingUp = 0,    // docker initializing
    Negotiating,            // making offers
    Running,
    ExecutionSucceeded,     // and ready to be verified or to be harvested
    ExecutionFailed,
}

// maintain lifecycle of a job
#[derive(Debug)]
pub struct Job {
    pub id: String,
    pub owner: PeerId,       // the client
    pub status: Status,
    pub residue: Residue,    // cids for stderr, output, receipt, ...
}

// get base residue path of the host
pub fn get_residue_path() -> Result<String, Box<dyn Error>> {
    let home_dir = home::home_dir()
        .ok_or_else(|| Box::<dyn Error>::from("Home dir is not available."))?
        .into_os_string().into_string()
        .or_else(|_| Err(Box::<dyn Error>::from("OS_String conversion failed.")))?;
    Ok(format!("{home_dir}/.wholesum/jobs"))
}
