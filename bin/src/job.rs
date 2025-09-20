use std::collections::{
    BTreeMap, HashSet
};
use libp2p::PeerId;
use zkvm::JobKind;
use log::warn;

#[derive(Debug, Clone)]
pub struct Job {
    // job id as specified by the client
    pub id: u128,

    // the client
    pub owner: PeerId,

    pub kind: JobKind,

    // must be pulled before proving can start
    // <index, hash>
    prerequisites: BTreeMap<usize, u128>,
    // helper map to manage prerequisites
    pending_prerequisites: HashSet<u128>,    
}

impl Job {
    //
    pub fn new(id: u128, owner: PeerId, kind: JobKind) -> Self {
        Self {
            id: id,
            owner: owner,
            kind: kind,
            prerequisites: BTreeMap::new(),
            pending_prerequisites: HashSet::new(),
        }
    }

    pub fn get_batch_id(&self) -> u128 {
        match self.kind {
            JobKind::R0(_, batch_id) => batch_id,

            JobKind::SP1(_, batch_id) => batch_id
        }
    }

    pub fn prerequisites(&self) -> Vec<u128> {
        self.prerequisites.values().cloned().collect()
    }

    // are all prerequisites fullfilled?
    pub fn is_ready(&self) -> bool {
        self.pending_prerequisites.is_empty()
    }

    pub fn add_prerequisite(&mut self, index: usize, hash: u128) {
        self.prerequisites.insert(index, hash);
        let _ = self.pending_prerequisites.insert(hash);        
    }

    pub fn is_a_prerequisite(&self, hash: u128) -> bool {
        self.pending_prerequisites.contains(&hash)
    }    

    pub fn set_prerequisite_as_fulfilled(&mut self, hash: u128) {
        if !self.pending_prerequisites.remove(&hash) {
            warn!("Blob(`{hash}`) is not a prerequisite.");
        }
    }
}
