use risc0_zkvm::{    
    sha::Digestible,
    ApiClient,
    ProverOpts, 
    SuccinctReceipt, ReceiptClaim,
    Groth16Receipt, Groth16ReceiptVerifierParameters,
    Asset, AssetRequest,
};

use std::{
    time::{Instant},
};

use log::info;
use anyhow;

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
pub async fn prove_segment(
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
                    info!("Done, segment prove + lift took `{segment_dur} secs`.");  
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
            info!("Done, join took `{join_dur} secs`.");  
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
        info!("Done, groth16 took `{groth16_dur} secs`."); 

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