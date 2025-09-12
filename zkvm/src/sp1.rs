use sp1_sdk::{
    include_elf, HashableKey, Prover, ProverClient, SP1ProvingKey, SP1Stdin, SP1VerifyingKey,
};

use log::info;
use anyhow;
use bincode;
use std::fs;

pub fn execute_elf(
    elf_path: &str,
    blob: Vec<u8>,
) -> anyhow::Result<Vec<u8>> {
    info!("Executing ELF...");
    let client = ProverClient::builder().cpu().build();
    let stdin: SP1Stdin = bincode::deserialize(&blob)?;    
    let elf = fs::read(elf_path)?;
    let (public_values, report) = client
        .execute(&elf, &stdin)
        .deferred_proof_verification(false)
        .run()
        .unwrap();
    info!("Subblock instruction count: {}", report.total_instruction_count());
    Ok(public_values.to_vec())
} 
