use std::collections::{
    HashMap, BTreeMap
};
use libp2p::PeerId;
use zkvm::JobKind;
use xxhash_rust::xxh3::xxh3_128;
use anyhow;
use log::{info, warn};

#[derive(Debug, Clone)]
pub struct Token {
    // xxh3_128
    pub hash: u128,

    pub owner: PeerId
}

#[derive(Debug, Clone)]
pub struct Job {
    // job id as specified by the client
    pub id: u128,

    // the client
    pub owner: PeerId,

    pub kind: JobKind,

    input_blobs: BTreeMap<usize, Vec<u8>>,

    // must be satisfied before proving can start
    // <index, token>
    prerequisites: BTreeMap<usize, Token>,
    // inverse helper map to put received blobs
    pending_blobs: HashMap<u128, usize>,
    // blobs being pulled
    // <hash, blob>
    incomplete_blobs: HashMap<u128, Blob>,
}

impl Job {
    //
    pub fn new(id: u128, owner: PeerId, kind: JobKind) -> Self {
        Self {
            id: id,
            owner: owner,
            kind: kind,
            input_blobs: BTreeMap::new(),
            prerequisites: BTreeMap::new(),
            pending_blobs: HashMap::new(),
            incomplete_blobs: HashMap::new(),
        }
    }

    pub fn get_batch_id(&self) -> u128 {
        match self.kind {
            JobKind::R0(_, batch_id) => batch_id,

            JobKind::SP1(_, batch_id) => batch_id
        }
    }

    // are all prerequisites fullfilled and ready for running?
    pub fn is_ready(&self) -> bool {
        self.prerequisites.is_empty()
    }

    pub fn get_inputs(&self) -> Vec<Vec<u8>>{
        self.input_blobs.values().cloned().collect()
    }

    pub fn add_pending_blob(&mut self, index: usize, token: Token) {
        self.pending_blobs.insert(token.hash, index);
        self.incomplete_blobs.insert(
            token.hash,
            Blob {
                num_expected_chunks: 0usize,
                chunks: BTreeMap::new()
            }
        );
        self.prerequisites.insert(index, token);
    }

    pub fn has_blob(&self, hash: &u128) -> bool {
        self.pending_blobs.contains_key(hash)
    }

    pub fn add_blob_info(&mut self, hash: &u128, num_chunks: usize) {
        let prereq_index = self.pending_blobs.get(hash).unwrap();
        let owner_peer_id = self.prerequisites.get(&prereq_index).unwrap().owner;
        let incomplete_blob = self.incomplete_blobs.get_mut(&hash).unwrap();
        incomplete_blob.num_expected_chunks = num_chunks;
    }

    pub fn add_blob_chunk(
        &mut self,
        blob_hash: u128,
        index: usize,
        chunk_data: Vec<u8>,
        chunk_hash: u128
    ) {
        let calculated_hash = xxh3_128(&chunk_data);
        if calculated_hash != chunk_hash {            
            warn!("Chunk is corrupted: hash mistmatch: `{calculated_hash}` != `{chunk_hash}`");
            return
        }
        let blob = self.incomplete_blobs.get_mut(&blob_hash).unwrap();
        if blob.chunks.contains_key(&index) {
            warn!("Duplicate chunk, index: `{index}`.");
            return
        }
        if index >= blob.num_expected_chunks {
            warn!("Out of bounds chunk, index: `{index}`.");
            return
        }
        blob.chunks.insert(
            index,
            BlobChunk {
                _hash: chunk_hash,
                data: chunk_data
            }
        );
        if blob.chunks.len() == blob.num_expected_chunks {
            self.reconstruct_pending_blob(blob_hash);
        }
    }

    fn reconstruct_pending_blob(&mut self, hash: u128) {
        let blob = self.incomplete_blobs.get_mut(&hash).unwrap();        
        let mut data = Vec::new();
        for chunk in blob.chunks.values() {
            data.extend_from_slice(&chunk.data);            
        }
        let reconstructed_hash = xxh3_128(&data);
        if hash != reconstructed_hash {
            warn!("Error in blob `{hash}` reconstruction: hash mismatch.");
            return
        }
        info!("Blob `{hash}` reconstruction succeded.");
        let blob_index = self.pending_blobs.remove(&hash).unwrap();
        let _ = self.prerequisites.remove(&blob_index);
        self.incomplete_blobs.clear();
        self.input_blobs.insert(blob_index, data);
    }

    pub fn get_next_blob_chunk_index(&self, hash: &u128) -> usize {
        self.incomplete_blobs.get(hash).unwrap().chunks.len()
    }
}

#[derive(Debug, Clone)]
pub struct Blob {
    // the number of chunks to receive 
    pub num_expected_chunks: usize,

    pub chunks: BTreeMap<usize, BlobChunk>,
}

#[derive(Debug, Clone)]
pub struct BlobChunk {
    pub _hash: u128,

    pub data: Vec<u8>,
}