use std::collections::{
    HashMap, BTreeMap
};
use libp2p::PeerId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    // param: batch id

    Segment(u128),
    Join(u128),

    Keccak(u128),
    Union(u128),

    Groth16(u128),
}

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

    pub kind: Kind,

    pub input_blobs: BTreeMap<usize, Vec<u8>>,

    // must be satisfied before proving can start
    // <index, token>
    pub prerequisites: BTreeMap<usize, Token>,
    // inverse helper map to put received blobs
    pub pending_blobs: HashMap<u128, usize>

}
