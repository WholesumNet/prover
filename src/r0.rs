use risc0_zkvm::{    
    sha::Digestible,
    ApiClient,
    ProverOpts, 
    SuccinctReceipt, ReceiptClaim, Unknown,
    Groth16Receipt, Groth16ReceiptVerifierParameters,
    Asset, AssetRequest,
    ProveKeccakRequest, ProveZkrRequest
};

use std::{
    time::{Instant},
};

use log::info;
use anyhow;

use comms::protocol::{
    KeccakRequestObject, ZkrRequestObject
};

#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub job_id: String,
    pub blob: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ExecutionError {
    pub job_id: String,
    pub err_msg: String,
}

// prove and lift the segment
pub async fn prove_and_lift_segment(
    job_id: String,
    index: u32,
    po2: u32,
    blob: Vec<u8>
) -> Result<ExecutionResult, ExecutionError> {
    info!("Proving `segment-{index}` with po2(`{po2}`) for `{job_id}`, length: `{} bytes`",
        blob.len()
    );
    ApiClient::from_env()
    .and_then(|r0_client| {   
        let opts = ProverOpts::succinct();
        let now = Instant::now();      
        r0_client
        .prove_segment(
            &opts,
            Asset::Inline(blob.into()),
            AssetRequest::Inline,
        )
        .and_then(|segment_receipt|
            r0_client
            .lift(
                &opts,
                segment_receipt.try_into()?,
                AssetRequest::Inline
            )
            .and_then(|lift_receipt| {                        
                let segment_dur = now.elapsed().as_secs();
                info!("Done in `{segment_dur} secs`");  
                Ok(ExecutionResult {
                    job_id: job_id.clone(),
                    blob: bincode::serialize(&lift_receipt)?
                })    
            })
        )        
    })    
    .map_err(|e| ExecutionError {
        job_id: job_id.clone(),
        err_msg: e.to_string()
    })
} 

pub async fn join(
    job_id: String,
    left_proof: Vec<u8>,
    right_proof: Vec<u8>,
) -> Result<ExecutionResult, ExecutionError> {
    info!("Joining proofs for `{job_id}`", );
    ApiClient::from_env()
    .and_then(|r0_client| {
        let now = Instant::now();     
        r0_client
        .join(
            &ProverOpts::succinct(),
            Asset::Inline(left_proof.into()),
            Asset::Inline(right_proof.into()),
            AssetRequest::Inline,
        )
        .and_then(|join_receipt| {
            let join_dur = now.elapsed().as_secs();
            info!("Done in `{join_dur} secs`");  
            Ok(ExecutionResult {
                job_id: job_id.clone(),
                blob: bincode::serialize(&join_receipt)?
            })        
        })        
    })
    .map_err(|e| ExecutionError {
        job_id: job_id.clone(),
        err_msg: e.to_string()
    })    
}

pub async fn prove_keccak(
    job_id: String,
    blob: Vec<u8>
) -> Result<ExecutionResult, ExecutionError> {    
    info!("Proving keccak for `{job_id}`");
    ApiClient::from_env()
    .and_then(|r0_client| {        
        let keccack_request_object: KeccakRequestObject = bincode::deserialize(&blob)?;
        let prove_keccak_request = ProveKeccakRequest {
            claim_digest: keccack_request_object.claim_digest.into(),
            po2: keccack_request_object.po2,
            control_root: keccack_request_object.control_root.into(),
            input: keccack_request_object.input
        };
        let now = Instant::now();     
        r0_client
            .prove_keccak(
                prove_keccak_request,
                AssetRequest::Inline,
            )
            .and_then(|keccak_receipt: SuccinctReceipt<Unknown>| {
                let keccak_dur = now.elapsed().as_secs();
                info!("Done in `{keccak_dur} secs`");  
                Ok(ExecutionResult {
                    job_id: job_id.clone(),
                    blob: bincode::serialize(&keccak_receipt)?
                })        
            })        
    })
    .map_err(|e| ExecutionError {
        job_id: job_id.clone(),
        err_msg: e.to_string()
    })    
}

pub async fn prove_zkr(
    job_id: String,
    blob: Vec<u8>
) -> Result<ExecutionResult, ExecutionError> {
    info!("Proving zkr for `{job_id}`");
    ApiClient::from_env()
    .and_then(|r0_client| {        
        let now = Instant::now();     
        let zkr_request_object: ZkrRequestObject = bincode::deserialize(&blob)?;
        let prove_zkr_request = ProveZkrRequest {
            claim_digest: zkr_request_object.claim_digest.into(),
            control_id: zkr_request_object.control_id.into(),
            input: zkr_request_object.input
        };
        r0_client
            .prove_zkr(
                prove_zkr_request,
                AssetRequest::Inline,
            )
            .and_then(|zkr_receipt: SuccinctReceipt<Unknown>| {
                let zkr_dur = now.elapsed().as_secs();
                info!("Done in `{zkr_dur} secs`");  
                Ok(ExecutionResult {
                    job_id: job_id.clone(),
                    blob: bincode::serialize(&zkr_receipt)?
                })        
            })        
    })
    .map_err(|e| ExecutionError {
        job_id: job_id.clone(),
        err_msg: e.to_string()
    })    
}

pub async fn to_groth16(
    job_id: String,
    blob: Vec<u8>
) -> anyhow::Result<ExecutionResult, ExecutionError> {
    ApiClient::from_env()
    .and_then(|r0_client| {
        info!("Extracting Groth16 proof for `{job_id}`");
        let now = Instant::now();
        let sr: SuccinctReceipt<ReceiptClaim> = bincode::deserialize(
            &blob
        )?;
        // 1- transform via identity_p254
        let ident_receipt = r0_client.identity_p254(
            &ProverOpts::succinct(),
            Asset::Inline(blob.into()),
            AssetRequest::Inline
        )?;
        let seal_bytes = ident_receipt.get_seal_bytes();
        // 2- stark to snark
        let seal = risc0_zkvm::stark_to_snark(&seal_bytes)?.to_vec();
        let groth16_proof = Groth16Receipt::new(
            seal,
            sr.claim.clone(),
            Groth16ReceiptVerifierParameters::default().digest()
        );
        let groth16_dur = now.elapsed().as_secs();
        info!("Done in `{groth16_dur} secs`"); 

        Ok(ExecutionResult {
            job_id: job_id.clone(),
            blob: bincode::serialize(&groth16_proof)?,
        })
    })
    .map_err(|e| ExecutionError {
        job_id: job_id.clone(),
        err_msg: e.to_string()
    })
}