use risc0_zkvm::{
    ProverOpts, 
    ApiClient,
    Asset, AssetRequest,
    // SuccinctReceipt,
    // Receipt, ReceiptClaim, InnerReceipt,
    // Journal,
    VerifierContext, SuccinctReceiptVerifierParameters, 
    recursion::MerkleGroup,
};
use risc0_circuit_recursion::control_id::{
    ALLOWED_CONTROL_ROOT, BN254_IDENTITY_CONTROL_ID
};
use risc0_zkp::core::hash::poseidon_254::Poseidon254HashSuite;

use std::{
    // fs,
    path::PathBuf,
    time::{Instant},
    collections::BTreeMap,
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
    ApiClient::from_env()
    .and_then(|r0_client| {   
        let now = Instant::now();      
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
    })    
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
        println!("[info] Transforming proof via identity_p254 `{job_id}`...");
        let now = Instant::now();
        // 1. transform via identity_p254
        r0_client
        .identity_p254(
            &ProverOpts::succinct(),
            Asset::Path(in_sr_path),
            AssetRequest::Inline,
        )
        .and_then(|p254_receipt| {
            let dur = now.elapsed().as_secs();
            println!("[info](DUR) identity_p254 took `{dur} secs`");

            let verifier_parameters = SuccinctReceiptVerifierParameters {
                control_root: MerkleGroup::new(vec![BN254_IDENTITY_CONTROL_ID])?
                    .calc_root(Poseidon254HashSuite::new_suite().hashfn.as_ref()),
                inner_control_root: Some(ALLOWED_CONTROL_ROOT),
                ..Default::default()
            };
            let _ = p254_receipt.verify_integrity_with_context(
                &VerifierContext::empty()
                    .with_suites(BTreeMap::from([(
                        "poseidon_254".to_string(),
                        Poseidon254HashSuite::new_suite(),
                    )]))
                    .with_succinct_verifier_parameters(verifier_parameters),
            )?;
            Ok(p254_receipt)
        })
        .and_then(|p254_receipt| {
            // 2. extract the compressed snark(Groth16)
            let asset: Asset = p254_receipt.try_into()?;
            println!("[info] compressing the p254 proof to get a groth16 proof...`");    
            let now = Instant::now();    
            let groth16_receipt = r0_client
                .compress(
                    &ProverOpts::groth16(),
                    Asset::Inline(asset.as_bytes()?),
                    AssetRequest::Inline,
                )?;
            let dur = now.elapsed().as_secs();
            println!("[info](DUR) compress took `{dur} secs`");
            let blob: Vec<u8> = {
                let asset: Asset = groth16_receipt.try_into()?;
                asset.as_bytes()?.into()
            };
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
