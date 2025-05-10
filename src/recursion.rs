use risc0_zkvm::{
    ProverOpts, 
    ApiClient,
    Asset,
    AssetRequest,
    SuccinctReceipt,
    SuccinctReceiptVerifierParameters, 
    ReceiptClaim,
    VerifierContext, 
    Groth16Receipt, 
    Groth16ReceiptVerifierParameters,
    sha::Digestible,
};

use std::{
    fs,
    path::PathBuf,
    time::{Instant},
};

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

pub async fn prove_and_lift(
    job_id: String,
    seg_path: PathBuf
) -> Result<ExecutionResult, ExecutionError> {
    println!("[info] Proving segment `{job_id}`...");
    let now = Instant::now();      
    ApiClient::from_env()
    .and_then(|r0_client|    
        r0_client
        .prove_segment(
            &ProverOpts::succinct(),
            Asset::Path(seg_path),
            AssetRequest::Inline,
        )
        .and_then(|segment_receipt| {
            let _ = segment_receipt
            .verify_integrity_with_context(
                &VerifierContext::with_succinct_verifier_parameters(
                    VerifierContext::default(),
                    SuccinctReceiptVerifierParameters::default(),
                )
            )?;
            Ok(segment_receipt)
        })
        .and_then(|verified_segment_receipt|
            r0_client
            .lift(
                &ProverOpts::succinct(),
                verified_segment_receipt.try_into()?,
                AssetRequest::Inline
            )
            .and_then(|lift_receipt| {
                let _ = lift_receipt.verify_integrity()?;
                Ok(lift_receipt)
            })
            .and_then(|verified_lift_receipt| {    
                let blob: Vec<u8> = {
                    let asset: Asset = verified_lift_receipt.try_into()?;
                    asset.as_bytes()?.into()
                };
                let prove_dur = now.elapsed().as_secs();
                println!("[info](DUR) prove took `{prove_dur} secs`.");  
                Ok(ExecutionResult {
                    job_id: job_id.clone(),
                    blob: blob
                })    
            })
        )        
    )    
    .map_err(|e| ExecutionError {
        job_id: job_id.clone(),
        err_msg: e.to_string()
    })
} 

pub async fn join(
    job_id: String,
    left_sr_path: PathBuf,
    right_sr_path: PathBuf,
) -> Result<ExecutionResult, ExecutionError> {
    println!("[info] Joining proofs `{job_id}`...", );
    ApiClient::from_env()
    .and_then(|r0_client| {
        let now = Instant::now();     
        r0_client
        .join(
            &ProverOpts::succinct(),
            Asset::Path(left_sr_path),
            Asset::Path(right_sr_path),
            AssetRequest::Inline,
        )
        .and_then(|join_receipt| {
            let _ = join_receipt.verify_integrity()?;
            let blob: Vec<u8> = {
            let asset: Asset = join_receipt.try_into()?;
                asset.as_bytes()?.into()
            };
            let join_dur = now.elapsed().as_secs();
            println!("[info](DUR) join took `{join_dur} secs`.");  
            Ok(ExecutionResult {
                job_id: job_id.clone(),
                blob: blob
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
    in_sr_path: PathBuf
) -> anyhow::Result<ExecutionResult, ExecutionError> {
    ApiClient::from_env()
    .and_then(|r0_client| {
        println!("[info] Extracting Groth16 proof for `{job_id}`...");
        let now = Instant::now();
        let sr_bytes = fs::read(&in_sr_path)?;
        let sr: SuccinctReceipt<ReceiptClaim> = bincode::deserialize(
            &sr_bytes
        )?;
        // 1. transform via identity_p254
        let ident_receipt = r0_client.identity_p254(
            &ProverOpts::succinct(),
            Asset::Inline(sr_bytes.into()),
            AssetRequest::Inline
        )?;
        let seal_bytes = ident_receipt.get_seal_bytes();
        let seal = risc0_zkvm::stark_to_snark(&seal_bytes)?.to_vec();
        let groth16_proof = Groth16Receipt::new(
            seal,
            sr.claim.clone(),
            Groth16ReceiptVerifierParameters::default().digest()
        );
        let groth16_dur = now.elapsed().as_secs();
        println!("[info](DUR) Groth16 took `{groth16_dur} secs`."); 

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
