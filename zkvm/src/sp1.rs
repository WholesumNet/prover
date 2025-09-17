use sp1_sdk::{
    include_elf, SP1Stdin,
    Prover, ProverClient,
    SP1ProvingKey, SP1VerifyingKey, 
    HashableKey, 
};
use log::info;
use anyhow;
use bincode;
use std::fs;

pub fn execute_elf(
    elf_path: &str,
    input: Vec<u8>,
) -> anyhow::Result<Vec<u8>> {
    info!("Executing ELF...");
    let client = ProverClient::builder().cpu().build();
    let stdin: SP1Stdin = bincode::deserialize(&input)?;    
    let elf = fs::read(elf_path)?;
    let (public_values, report) = client
        .execute(&elf, &stdin)
        .deferred_proof_verification(false)
        .run()
        .unwrap();
    info!("Subblock instruction count: {}", report.total_instruction_count());
    Ok(public_values.to_vec())
}

pub fn prove_compressed_subblock(
    elf_path: &str,
    stdin_blob: Vec<u8>,
) -> anyhow::Result<Vec<u8>> {    
    info!("Proving Subblock ELF...");
    let client = ProverClient::builder().cpu().build();
    let stdin: SP1Stdin = bincode::deserialize(&stdin_blob)?;    
    let elf = fs::read(elf_path)?;
    let (pk, vk) = client.setup(&elf);
    let proof = client
        .prove(&pk, &stdin)
        .deferred_proof_verification(false)
        .compressed()
        .run()?;
    Ok(bincode::serialize(&proof)?)
}

pub fn prove_compressed_agg(
    elf_path: &str,
    stdin_blob: Vec<u8>,
) -> anyhow::Result<Vec<u8>> {    
    info!("Proving Agg ELF...");
    let client = ProverClient::builder().cpu().build();
    let stdin: SP1Stdin = bincode::deserialize(&stdin_blob)?;    
    let elf = fs::read(elf_path)?;
    let (pk, vk) = client.setup(&elf);
    let proof = client
        .prove(&pk, &stdin)
        .compressed()
        .run()?;
    Ok(bincode::serialize(&proof)?)
}
