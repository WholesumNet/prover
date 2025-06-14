use risc0_zkvm::{    
    ApiClient,ProverOpts, 
    SuccinctReceipt, ReceiptClaim, Unknown,
    Groth16Receipt, Groth16ReceiptVerifierParameters,
    Asset, AssetRequest,
    ProveKeccakRequest, ProveZkrRequest,
    sha::Digestible,
};

use log::info;
use anyhow;

use peyk::protocol::{
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
fn prove_and_lift_segment(
    blob: Vec<u8>
) -> anyhow::Result<SuccinctReceipt<ReceiptClaim>> {
    let r0_client = ApiClient::from_env()?;
    let opts = ProverOpts::succinct();
    let segment_receipt = r0_client.prove_segment(
        &opts,
        Asset::Inline(blob.into()),
        AssetRequest::Inline,
    )?;
    r0_client.lift(
        &opts,
        segment_receipt.try_into()?,
        AssetRequest::Inline
    )
} 

fn join_receipts(
    left_proof: SuccinctReceipt<ReceiptClaim>,
    right_proof: SuccinctReceipt<ReceiptClaim>,
) -> anyhow::Result<SuccinctReceipt<ReceiptClaim>> {
    ApiClient::from_env()?
    .join(
        &ProverOpts::succinct(),
        left_proof.try_into()?,
        right_proof.try_into()?,
        AssetRequest::Inline,
    )
}

// the 2nd param is of type Vec<u8> to remove the need for serializing the output of join
fn join_proofs(
    left_proof: SuccinctReceipt<ReceiptClaim>,
    right_proof: Vec<u8>,
) -> anyhow::Result<SuccinctReceipt<ReceiptClaim>> {
    ApiClient::from_env()?    
    .join(
        &ProverOpts::succinct(),
        left_proof.try_into()?,
        Asset::Inline(right_proof.into()),
        AssetRequest::Inline,
    )
}

// given a list of segment blobs, aggregate them into a final proof
// input: n segments
// output: 1 proof
async fn aggregate_proofs(
    job_id: String,
    blobs: Vec<Vec<u8>>
) -> Result<ExecutionResult, ExecutionError> {
    let first = match 
        bincode::deserialize::<SuccinctReceipt<ReceiptClaim>>(
            &blobs[0]
        )
    {
        Ok(sr) => sr,

        Err(e) => {
            return Err(
                ExecutionError {
                    job_id: job_id.clone(),
                    err_msg: e.to_string()
                }
            )
        }
    };    
    blobs
        .into_iter()
        .skip(1)
        .try_fold(first, |agg, r| join_proofs(agg, r))            
        .and_then(|proof|
            Ok(
                ExecutionResult {
                    job_id: job_id.clone(),
                    blob: bincode::serialize(&proof)?
                }
            )
        )
        .map_err(|e| ExecutionError {
            job_id: job_id.clone(),
            err_msg: e.to_string()
        })    
}

// given a list of segment blobs, aggregate them into a final proof
// input: n segments
// output: 1 proof
async fn aggregate_segments(
    job_id: String,
    blobs: Vec<Vec<u8>>
) -> Result<ExecutionResult, ExecutionError> {
    let mut receipts = Vec::new();
    for (i, blob) in blobs.into_iter().enumerate() {
        match prove_and_lift_segment(blob) {
            Ok(sr) => {
                info!("`{i}th` segment was proved with success.");
                receipts.push(sr)
            },

            Err(e) => {
                return Err(
                    ExecutionError {
                        job_id: job_id.clone(),
                        err_msg: e.to_string()
                    }
                )
            }
        };        
    }
    info!(
        "Joining receipts, `{}` operation(s) in total.",
        receipts.len() - 1
    );
    let first = receipts.remove(0);
    receipts
        .into_iter()
        .try_fold(first, |agg, r| join_receipts(agg, r))
        .and_then(|proof|
            Ok(
                ExecutionResult {
                    job_id: job_id.clone(),
                    blob: bincode::serialize(&proof)?
                }
            )
        )
        .map_err(|e| ExecutionError {
            job_id: job_id.clone(),
            err_msg: e.to_string()
        })    
}

pub async fn aggregate(
    job_id: String,
    blobs: Vec<Vec<u8>>,
    blobs_are_segment: bool
) -> Result<ExecutionResult, ExecutionError> {
    info!(
        "Aggregating: {}",
        if blobs_are_segment {
            format!("`{}` segment(s) to prove.", blobs.len())
        } else {
            format!("`{}` receipt(s) to join.", blobs.len())
        }
    );
    if blobs_are_segment {
        aggregate_segments(job_id, blobs).await
    } else {
        aggregate_proofs(job_id, blobs).await
    }
}

pub async fn prove_keccak(
    job_id: String,
    blob: Vec<u8>
) -> Result<ExecutionResult, ExecutionError> { 
    info!("Proving keccak request for `{job_id}`");   
    ApiClient::from_env()
    .and_then(|r0_client| {        

        let keccack_request_object: KeccakRequestObject = bincode::deserialize(&blob)?;
        let prove_keccak_request = ProveKeccakRequest {
            claim_digest: keccack_request_object.claim_digest.into(),
            po2: keccack_request_object.po2,
            control_root: keccack_request_object.control_root.into(),
            input: keccack_request_object.input
        };
        r0_client
            .prove_keccak(
                prove_keccak_request,
                AssetRequest::Inline,
            )
            .and_then(|keccak_receipt: SuccinctReceipt<Unknown>| {
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
    info!("Proving zkr request for `{job_id}`");
    ApiClient::from_env()
    .and_then(|r0_client| {        
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
    info!("Extracting Groth16 proof for `{job_id}`");
    ApiClient::from_env()
    .and_then(|r0_client| {
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