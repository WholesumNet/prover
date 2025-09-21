use sp1_sdk::{
    include_elf, SP1Stdin,
    Prover, ProverClient,
    SP1ProofWithPublicValues,
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
    info!("Execution instruction count: {}", report.total_instruction_count());
    Ok(public_values.to_vec())
}

pub fn prove_compressed_subblock(
    elf_path: &str,
    stdin_blob: Vec<u8>,
) -> anyhow::Result<Vec<u8>> {    
    info!("Proving Subblock ELF...");    
    let client = ProverClient::builder().cuda().build();
    let stdin: SP1Stdin = bincode::deserialize(&stdin_blob)?;    
    let elf = fs::read(elf_path)?;
    let (pk, _vk) = client.setup(&elf);
    let proof = client
        .prove(&pk, &stdin)
        .compressed()
        .run()?;
    Ok(bincode::serialize(&proof)?)
}

pub fn prove_compressed_agg(
    elf_path: &str,
    stdin_blob: Vec<u8>,
    subblock_proofs: Vec<Vec<u8>>,
) -> anyhow::Result<Vec<u8>> {    
    info!("Proving Agg ELF...");
    let elf = fs::read(elf_path)?;
    let client = ProverClient::builder().cuda().build();
    let (agg_pk, _agg_vk) = client.setup(&elf);
    let subblock_vk = bincode::deserialize::<SP1VerifyingKey>(&fs::read("./elfs/subblock_vk.bin")?)?;
    let mut stdin: SP1Stdin = bincode::deserialize(&stdin_blob)?;
    for proof_blob in subblock_proofs.into_iter() {
        let proof: SP1ProofWithPublicValues = bincode::deserialize(&proof_blob)?;
        let reduced_proof = proof.proof.try_as_compressed()
            .ok_or_else(|| anyhow::anyhow!("Proof is not reduced."))?;
        stdin.write_proof(*reduced_proof, subblock_vk.clone().vk);
    }
    let proof = client
        .prove(&agg_pk, &stdin)
        .compressed()
        .run()?;
    Ok(bincode::serialize(&proof)?)
}
