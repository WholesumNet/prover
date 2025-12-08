use std::{
    fs,
    time::Instant,
    process::Command
};
use log::info;
use anyhow;
use bincode;
use sp1_sdk::{
    SP1Stdin, Prover,
    ProverClient, CpuProver,
    SP1ProvingKey, SP1VerifyingKey, 
    SP1ProofWithPublicValues,
};

#[allow(unused)]
pub struct SP1Handle {
    client: CpuProver,
    
    subblock_pk: SP1ProvingKey,
    subblock_vk: SP1VerifyingKey,
    
    agg_pk: SP1ProvingKey,
    agg_vk: SP1VerifyingKey,
}

impl SP1Handle {
    pub fn new() -> anyhow::Result<Self> {
        let cpu_client = ProverClient::builder().cpu().build();
        // subblock
        let subblock_elf = fs::read("../elfs/subblock_elf.bin")?;
        let (subblock_pk, subblock_vk) = cpu_client.setup(&subblock_elf);
        // agg
        let agg_elf = fs::read("../elfs/agg_elf.bin")?;
        let (agg_pk, agg_vk) = cpu_client.setup(&agg_elf);

        Ok(Self {
            client: cpu_client,
            subblock_pk: subblock_pk,
            subblock_vk: subblock_vk,
            agg_pk: agg_pk,
            agg_vk: agg_vk
        })
    }

    // prove on sp1 cluster
    pub fn prove_subblock_on_cluster(
        &self,
        stdin_blob: Vec<u8>,
    ) -> anyhow::Result<Vec<u8>> {            
        info!("Proving subblock is initiated.");                
        let start = Instant::now();
        fs::write("../sp1-cluster/stdin.bin", stdin_blob)?;
        let status = Command::new("../sp1-cluster/sp1-cluster-cli")
            .arg("bench")
            .arg("input")
            .arg("--cluster-rpc")
            .arg("http://localhost:50051")
            .arg("--redis-nodes")
            .arg("redis://:redispassword@localhost:6379/0")
            .arg("../elfs/subblock_elf.bin")
            .arg("../sp1-cluster/stdin.bin")
            .status()?;
        if !status.success() {
            anyhow::bail!("Proving failed: {status:?}")
        }
        let proof = fs::read("../sp1-cluster/proof.bin")?;
        let dur = start.elapsed();
        info!("Proving is finished in `{:.3}s`.", dur.as_secs_f64());
        Ok(proof)
    }

    // prove on sp1 cluster
    pub fn prove_aggregation_on_cluster(
        &self,
        stdin_blob: Vec<u8>,
        subblock_proofs: Vec<Vec<u8>>,
    ) -> anyhow::Result<Vec<u8>> {    
        info!("Proving aggregation is initiated.");
        let start = Instant::now();        
        let mut stdin: SP1Stdin = bincode::deserialize(&stdin_blob)?;
        for proof_blob in subblock_proofs.into_iter() {
            let proof: SP1ProofWithPublicValues = bincode::deserialize(&proof_blob)?;
            let reduced_proof = proof.proof.try_as_compressed()
                .ok_or_else(|| anyhow::anyhow!("The input subblock proof is not of `reduced` type."))?;
            stdin.write_proof(*reduced_proof, self.subblock_vk.clone().vk);
        }
        fs::write("../sp1-cluster/stdin.bin", &bincode::serialize(&stdin)?)?;
        let status = Command::new("../sp1-cluster/sp1-cluster-cli")
            .arg("bench")
            .arg("input")
            .arg("--cluster-rpc")
            .arg("http://localhost:50051")
            .arg("--redis-nodes")
            .arg("redis://:redispassword@localhost:6379/0")
            .arg("../elfs/agg_elf.bin")
            .arg("../sp1-cluster/stdin.bin")
            .status()?;
        if !status.success() {
            anyhow::bail!("Proving failed: {status:?}")
        }
        let proof = fs::read("../sp1-cluster/proof.bin")?;
        let dur = start.elapsed();
        info!("Proving is finished in `{:.3}s`.", dur.as_secs_f64());
        info!("Verifying the aggregation proof.");
        let agg_proof: SP1ProofWithPublicValues = bincode::deserialize(&proof)?;
        self.client.verify(&agg_proof, &self.agg_vk)?;
        info!("Viola, verified!");
        Ok(proof)
    }
}
