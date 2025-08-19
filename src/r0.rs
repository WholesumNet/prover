use risc0_zkvm::{    
    ApiClient,ProverOpts, 
    SuccinctReceipt, ReceiptClaim,
    Unknown, UnionClaim,
    Groth16Receipt, Groth16ReceiptVerifierParameters,
    Asset, AssetRequest,
    ProveKeccakRequest,
    Digest, sha::Digestible
};

use log::info;
use anyhow;

use crate::job;
use job::Kind;

use peyk::protocol::KeccakRequestObject;

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

// given a list of proofs, aggregate them into a final proof
// input: n proofs
// output: 1 proof
fn aggregate_proofs(blobs: Vec<Vec<u8>>) -> anyhow::Result<Vec<u8>> {
    info!(
        "Aggregating proofs, `{}` operations in total.",
        blobs.len() - 1
    );
    let first: SuccinctReceipt<ReceiptClaim> = bincode::deserialize(&blobs[0])?;    
    blobs
        .into_iter()
        .skip(1)
        .try_fold(first, |agg, r| join_proofs(agg, r))            
        .and_then(|proof| Ok(bincode::serialize(&proof)?))        
}

// given a list of segment blobs, aggregate them into a final proof
// input: n segments
// output: 1 proof
fn aggregate_segments(
    blobs: Vec<Vec<u8>>
) -> anyhow::Result<Vec<u8>> {
    info!(
        "Proving segments, `{}` operation(s) in total.",
        blobs.len()
    );
    let mut receipts = Vec::new();
    for (i, blob) in blobs.into_iter().enumerate() {
        receipts.push(prove_and_lift_segment(blob)?);
        info!("Proved `segment-{i}` with success.");
    }
    info!(
        "Joining receipts, `{}` operation(s) in total.",
        receipts.len() - 1
    );
    let first = receipts.remove(0);
    receipts
        .into_iter()
        .try_fold(first, |agg, r| join_receipts(agg, r))
        .and_then(|proof| Ok(bincode::serialize(&proof)?))
}

fn prove_keccak(
    blob: Vec<u8>,
) -> anyhow::Result<SuccinctReceipt<Unknown>> {
    let kecak_obj = bincode::deserialize::<KeccakRequestObject>(&blob)?;    
    let claim_digest: Digest = kecak_obj.claim_digest.into();
    info!("Proving Keccak request `{claim_digest:?}`");        
    ApiClient::from_env()?
        .prove_keccak(
            ProveKeccakRequest {
                claim_digest: claim_digest,
                po2: kecak_obj.po2,
                control_root: kecak_obj.control_root.into(),
                input: kecak_obj.input
            },
            AssetRequest::Inline
        )
}

fn union_keccak(
    left_proof: SuccinctReceipt<Unknown>,
    right_proof: SuccinctReceipt<Unknown>,
) -> anyhow::Result<SuccinctReceipt<Unknown>> {        
    let r = ApiClient::from_env()?
        .union(
            &ProverOpts::succinct(),
            left_proof.try_into()?,
            right_proof.try_into()?,
            AssetRequest::Inline
        )?;
    Ok(r.into_unknown())
}

fn union_proofs(
    left_proof: SuccinctReceipt<Unknown>,
    right_proof: Vec<u8>,
) -> anyhow::Result<SuccinctReceipt<Unknown>> {    
    let r = ApiClient::from_env()?
        .union(
            &ProverOpts::succinct(),
            left_proof.try_into()?,
            Asset::Inline(right_proof.into()),
            AssetRequest::Inline
        )?;
    Ok(r.into_unknown())
}

// given a list of keccak prove request blobs, aggregate them into a final proof
// input: n segments
// output: 1 proof
fn aggregate_keccaks(
    blobs: Vec<Vec<u8>>
) -> anyhow::Result<Vec<u8>> {
    info!(
        "Proving keccak requests, `{}` operation(s) in total.",
        blobs.len()
    );
    let mut receipts = Vec::new();
    for blob in blobs.into_iter() {
        receipts.push(prove_keccak(blob)?);
    }
    info!(
        "Union keccak receipts, `{}` operation(s) in total.",
        receipts.len() - 1
    );
    let first = receipts.remove(0);
    receipts
        .into_iter()
        .try_fold(first, |agg, r| union_keccak(agg, r))
        .and_then(|proof| Ok(bincode::serialize(&proof)?))
}

// given a list of keccak proofs, aggregate them into a final proof
// input: n proofs
// output: 1 proof
fn aggregate_assumptions(blobs: Vec<Vec<u8>>) -> anyhow::Result<Vec<u8>> {
    info!(
        "Aggregating keccak proofs, `{}` operations in total.",
        blobs.len() - 1
    );
    let first: SuccinctReceipt<Unknown> = bincode::deserialize(&blobs[0])?;    
    blobs
        .into_iter()
        .skip(1)
        .try_fold(first, |agg, r| union_proofs(agg, r))            
        .and_then(|proof| Ok(bincode::serialize(&proof)?))        
}

fn to_groth16(
    blob: Vec<u8>
) -> anyhow::Result<Vec<u8>> {
    info!("Extracting Groth16 proof...");
    let sr: SuccinctReceipt<ReceiptClaim> = bincode::deserialize(&blob)?;
    let ident_receipt = ApiClient::from_env()?
        .identity_p254(
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
    Ok(bincode::serialize(&groth16_proof)?)
}

pub fn prove(
    blobs: Vec<Vec<u8>>,
    kind: Kind
) -> anyhow::Result<Vec<u8>> {    
    match kind {
        Kind::Segment(_) => {
            aggregate_segments(blobs)
        },

        Kind::Join(_) => {
            aggregate_proofs(blobs)
        },        

        Kind::Keccak(_) => {            
            aggregate_keccaks(blobs)
        },

        Kind::Union(_) => {            
            aggregate_assumptions(blobs)
        },

        Kind::Groth16(_) => {
            let first = blobs.into_iter().next().unwrap();
            to_groth16(first)
        }
    }
}