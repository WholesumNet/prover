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

use crate::job;
use job::Kind;

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
                info!("`{i}th` segment is proved with success.");
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

fn prove_keccak(
    keccak_req: ProveKeccakRequest,
) -> anyhow::Result<SuccinctReceipt<Unknown>> { 
    ApiClient::from_env()?
        .prove_keccak(keccak_req, AssetRequest::Inline)
}

fn prove_zkr(
    zkr_req: ProveZkrRequest
) -> anyhow::Result<SuccinctReceipt<Unknown>> {
    ApiClient::from_env()?
        .prove_zkr(zkr_req, AssetRequest::Inline)
}

async fn prove_assumption(
    job_id: String,
    blob: Vec<u8>,
) -> anyhow::Result<ExecutionResult, ExecutionError> {
    if let Ok(k) =  bincode::deserialize::<KeccakRequestObject>(&blob) {
        // it's keccak
        let keccak_req = ProveKeccakRequest {
            claim_digest: k.claim_digest.into(),
            po2: k.po2,
            control_root: k.control_root.into(),
            input: k.input
        };
        info!(
            "Proving Keccak request `{:?}` for `{}`",
            keccak_req.claim_digest,
            job_id
        );
        prove_keccak(keccak_req)
            .and_then(|proof| Ok(
                ExecutionResult {
                    job_id: job_id.clone(),
                    blob: bincode::serialize(&proof)?
                })
            )
            .map_err(|e| ExecutionError {
                job_id: job_id.clone(),
                err_msg: e.to_string()
            })
    } else if let Ok(z) = bincode::deserialize::<ZkrRequestObject>(&blob) {
        // or zkr
        let zkr_req = ProveZkrRequest {
            claim_digest: z.claim_digest.into(),
            control_id: z.control_id.into(),
            input: z.input
        };
        info!(
            "Proving Zkr request `{:?}` for `{}`",
            zkr_req.claim_digest,
            job_id
        );
        prove_zkr(zkr_req)
            .and_then(|proof| Ok(
                ExecutionResult {
                    job_id: job_id.clone(),
                    blob: bincode::serialize(&proof)?
                })
            )
            .map_err(|e| ExecutionError {
                job_id: job_id.clone(),
                err_msg: e.to_string()
            })
    } else {
        // or an invalid blob
        Err(
            ExecutionError {
                job_id: job_id.clone(),
                err_msg: "Invalid blob".to_string()
            }
        )
    }
}

async fn to_groth16(
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
            blob: bincode::serialize(&groth16_proof)?
        })
    })
    .map_err(|e| ExecutionError {
        job_id: job_id.clone(),
        err_msg: e.to_string()
    })
}

pub async fn prove(
    job_id: String,
    blobs: Vec<Vec<u8>>,
    kind: Kind
) -> anyhow::Result<ExecutionResult, ExecutionError> {    
    match kind {
        Kind::Segment(_) => {
            aggregate_segments(job_id, blobs).await
        },

        Kind::Join(_) => {
            aggregate_proofs(job_id, blobs).await
        },        

        Kind::Assumption(_) => {            
            let first = blobs.into_iter().next().unwrap();
            prove_assumption(job_id, first).await
        },

        Kind::Groth16(_) => {
            let first = blobs.into_iter().next().unwrap();
            to_groth16(job_id, first).await
        }
    }
}