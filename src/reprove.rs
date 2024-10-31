use risc0_zkvm::{
    ProverOpts, 
    ApiClient,
    ExecutorEnv,
    Asset, AssetRequest,
    SuccinctReceipt,
    Receipt, ReceiptClaim, InnerReceipt,
    Journal,
    VerifierContext, SuccinctReceiptVerifierParameters, 
    recursion::MerkleGroup,
};
use risc0_circuit_recursion::control_id::{
    ALLOWED_CONTROL_ROOT, BN254_IDENTITY_CONTROL_ID
};
use risc0_zkp::core::hash::poseidon_254::Poseidon254HashSuite;

use std::{
    fs,
    path::PathBuf,
    time::{Instant},
    collections::BTreeMap,
};

use anyhow;

async fn prove_and_lift(
    in_seg_path: PathBuf
) -> anyhow::Result<Vec<u8>> {
    let r0_client = ApiClient::from_env()?;
    // fisrt prove
    let mut now = Instant::now();
    let segment_receipt = r0_client
        .prove_segment(
            &ProverOpts::succinct(),
            Asset::Path(in_seg_path),
            AssetRequest::Inline,
    )?; 
    let prove_dur = now.elapsed().as_secs();
    let _ = segment_receipt.verify_integrity_with_context(
        &VerifierContext::with_succinct_verifier_parameters(
            VerifierContext::default(),
            SuccinctReceiptVerifierParameters::default(),
        )
    )?;
    // and then lift
    now = Instant::now();
    let lift_receipt = r0_client
        .lift(
            &ProverOpts::succinct(),
            segment_receipt.try_into()?,
            AssetRequest::Inline
        )?;
    let lift_dur = now.elapsed().as_secs();
    println!("prove took `{} secs`, and lift `{} secs`.", prove_dur, lift_dur);
    let _ = lift_receipt.verify_integrity()?;    
    let blob: Vec<u8> = {
        let asset: Asset = lift_receipt.try_into()?;
        asset.as_bytes()?.into()
    };
    Ok(blob)
}

fn join(
    left_sr_path: PathBuf,
    right_sr_path: PathBuf,
) -> anyhow::Result<Vec<u8>> {
    let r0_client = ApiClient::from_env()?;
    let now = Instant::now();
    let join_receipt = r0_client
        .join(
            &ProverOpts::succinct(),
            Asset::Path(left_sr_path),
            Asset::Path(right_sr_path),
            AssetRequest::Inline,
        )?;
    let _ = join_receipt.verify_integrity()?;
    let dur = now.elapsed().as_secs();
    println!("join took `{dur} secs`");
    let blob: Vec<u8> = {
        let asset: Asset = join_receipt.try_into()?;
        asset.as_bytes()?.into()
    };
    Ok(blob)
}

fn stark_to_snark(
    in_sr_path: PathBuf,
    out_receipt_path: PathBuf,
) -> anyhow::Result<Receipt> {
    let r0_client = ApiClient::from_env()?;
    // fist transform via identity_p254
    let now = Instant::now();
    let p254_receipt = r0_client
        .identity_p254(
            &ProverOpts::succinct(),
            Asset::Path(in_sr_path),
            AssetRequest::Inline,
        )?;
    let dur = now.elapsed().as_secs();
    println!("identity_p254 took `{dur} secs`");
    //
    let verifier_parameters = SuccinctReceiptVerifierParameters {
        control_root: MerkleGroup::new(vec![BN254_IDENTITY_CONTROL_ID])
            .unwrap()
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
    ).unwrap();
    // std::fs::write("segs/snark/p254", bincode::serialize(&p254_receipt)?);
    // and then extract the compressed snark(Groth16)
    let asset: Asset = p254_receipt.try_into()?;    
    let now = Instant::now();    
    let receipt = r0_client
        .compress(
            &ProverOpts::groth16(),
            Asset::Inline(asset.as_bytes()?),
            AssetRequest::Inline,
        )?;
    let dur = now.elapsed().as_secs();
    println!("compress took `{dur} secs`");
    let _ = fs::write(
        &out_receipt_path,
        bincode::serialize(&receipt)?
    );
    println!("your Groth16 receipt is ready!`");
    Ok(receipt)
}