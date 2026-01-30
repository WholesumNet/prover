use std::{
    fs,
    io::Write,
    time::Instant,
    process::Command
};
use log::info;
use anyhow::{
    Result,
    Context
};
use bincode;
use sp1_sdk::{
    SP1Stdin, 
    SP1VerifyingKey, 
    SP1ProofWithPublicValues,
};

pub struct SP1Handle {
    subblock_vk: SP1VerifyingKey,    
}

impl SP1Handle {
    pub fn new() -> Result<Self> { 
        let subblock_vk_path = "../elfs/subblock_vk.bin";      
        let vk: SP1VerifyingKey = bincode::deserialize(
            &fs::read(subblock_vk_path)
                .with_context(||
                    format!(
                        "Subblock VK file was not found in `{}`.",
                        subblock_vk_path
                    )
                )?
        )?;
        Ok(Self {            
            subblock_vk: vk,
        })
    }

    // prove on sp1 cluster
    pub fn prove_subblock_on_cluster(
        &self,
        batch_id: u128,
        stdin_blob: Vec<u8>,
    ) -> Result<Vec<u8>> {            
        info!("Proving subblock is initiated.");                
        let start = Instant::now();
        let stdin_filename = format!(
            "../sp1-cluster/subblock-stdin-{}.bin",
            batch_id
        );            
        fs::write(&stdin_filename, &stdin_blob)?;
        let status = Command::new("../sp1-cluster/sp1-cluster-cli")
            .arg("bench")
            .arg("input")
            .arg("--cluster-rpc")
            .arg("http://localhost:50051")
            .arg("--redis-nodes")
            .arg("redis://:redispassword@localhost:6379/0")
            .arg(batch_id.to_string())
            .arg("../elfs/subblock_elf.bin")
            .arg(stdin_filename)
            .status()?;
        if !status.success() {
            anyhow::bail!("Proving failed: {status:?}")
        }
        let proof_path = format!(
            "../sp1-cluster/proof-{}.bin",
            batch_id
        );
        let proof = fs::read(&proof_path)
            .with_context(||
                format!(
                    "Subblock proof file was not found in `{}`.",
                    proof_path
                )
            )?;
        let dur = start.elapsed();
        info!("Proving is finished in `{:.3}s`.", dur.as_secs_f64());
        Ok(proof)
    }

    // prove on sp1 cluster
    pub fn prove_aggregation_on_cluster(
        &self,
        batch_id: u128,
        stdin_blob: Vec<u8>,
        subblock_proofs: Vec<Vec<u8>>,
    ) -> Result<Vec<u8>> {    
        info!("Proving aggregation is initiated.");
        let start = Instant::now();        
        let mut stdin: SP1Stdin = bincode::deserialize(&stdin_blob)?;
        for (i, proof_blob) in subblock_proofs.into_iter().enumerate() {
            // fs::write("./temp-proof.bin", &proof_blob)?;
            let tmp_filename = {
                let tmp_filename = format!("./temp-proof-{}.bin", i);
                let mut f = fs::File::create(&tmp_filename)?;
                f.write_all(&proof_blob)?;
                f.sync_all()?;
                tmp_filename
            };
            let proof = SP1ProofWithPublicValues::load(tmp_filename)?;
            let reduced_proof = proof.proof.try_as_compressed()
                .ok_or_else(|| anyhow::anyhow!("The input subblock proof is not of `reduced` type."))?;
            stdin.write_proof(*reduced_proof, self.subblock_vk.clone().vk);
        }
        let stdin_filename = format!(
            "../sp1-cluster/agg-stdin-{}.bin",
            batch_id
        );
        fs::write(&stdin_filename, &bincode::serialize(&stdin)?)?;
        let status = Command::new("../sp1-cluster/sp1-cluster-cli")
            .arg("bench")
            .arg("input")
            .arg("--cluster-rpc")
            .arg("http://localhost:50051")
            .arg("--redis-nodes")
            .arg("redis://:redispassword@localhost:6379/0")
            .arg(batch_id.to_string())
            .arg("../elfs/agg_elf.bin")
            .arg(stdin_filename)
            .status()?;
        if !status.success() {
            anyhow::bail!("Proving failed: {status:?}")
        }        
        info!(
            "Proving is finished in `{:.3}s`.",
            start.elapsed().as_secs_f64()
        );         
        let proof_path = format!(
            "../sp1-cluster/proof-{}.bin",
            batch_id
        );       
        let proof_bytes = fs::read(&proof_path)
            .with_context(||
                format!(
                    "Agg proof file was not found in `{}`.",
                    proof_path
                )
            )?;        
        Ok(proof_bytes)
    }
}
