use std::collections::{
    HashMap, BTreeMap
};
use libp2p::PeerId;
use zkvm::JobKind;

#[derive(Debug, Clone)]
pub struct Token {
    // xxh3_128
    pub hash: u128,

    pub owner: Vec<u8>
}

#[derive(Debug, Clone)]
pub struct Job {
    // job id as specified by the client
    pub id: u128,

    // pub working_dir: String,
    
    // the client
    pub owner: PeerId,

    pub kind: JobKind,

    pub input_blobs: BTreeMap<usize, Vec<u8>>,

    // must be satisfied before proving can start
    // <index, token>
    pub prerequisites: BTreeMap<usize, Token>,
    // inverse helper map to put received blobs
    pub pending_blobs: HashMap<u128, usize>

}
