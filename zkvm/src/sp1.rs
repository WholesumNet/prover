use std::{
    fs,
    time::Instant,
};
use log::info;
use anyhow;
use bincode;
use sp1_sdk::{
    SP1Stdin,
    CudaProver, Prover, ProverClient,
    SP1ProvingKey, SP1VerifyingKey, 
    SP1ProofWithPublicValues,
};

pub struct SP1CudaHandle {
    client: CudaProver,
    
    subblock_pk: SP1ProvingKey,
    subblock_vk: SP1VerifyingKey,
    
    agg_pk: SP1ProvingKey,
    agg_vk: SP1VerifyingKey,
}

impl SP1CudaHandle {
    pub fn new() -> anyhow::Result<Self> {
        let cuda_client = ProverClient::builder().cuda().build();
        // subblock
        let subblock_elf = fs::read("./elfs/subblock_elf.bin")?;
        let (subblock_pk, subblock_vk) = cuda_client.setup(&subblock_elf);
        // agg
        let agg_elf = fs::read("./elfs/agg_elf.bin")?;
        let (agg_pk, agg_vk) = cuda_client.setup(&agg_elf);

        Ok(Self {
            client: cuda_client,
            subblock_pk: subblock_pk,
            subblock_vk: subblock_vk,
            agg_pk: agg_pk,
            agg_vk: agg_vk
        })
    }

    pub fn prove_subblock(
        &self,
        stdin_blob: Vec<u8>,
    ) -> anyhow::Result<Vec<u8>> {    
        info!("Proving Subblock ELF...");
        let start = Instant::now();
        let stdin: SP1Stdin = bincode::deserialize(&stdin_blob)?;    
        let proof = self.client
            .prove(&self.subblock_pk, &stdin)
            .compressed()
            .run()?;
        let dur = start.elapsed();
        info!("Subblock proof generation took `{:.3}s`", dur.as_secs_f64());
        Ok(bincode::serialize(&proof)?)
    }

    pub fn prove_agg(
        &self,
        stdin_blob: Vec<u8>,
        subblock_proofs: Vec<Vec<u8>>,
    ) -> anyhow::Result<Vec<u8>> {    
        info!("Proving Agg ELF...");
        let start = Instant::now();
        let mut stdin: SP1Stdin = bincode::deserialize(&stdin_blob)?;
        for proof_blob in subblock_proofs.into_iter() {
            let proof: SP1ProofWithPublicValues = bincode::deserialize(&proof_blob)?;
            let reduced_proof = proof.proof.try_as_compressed()
                .ok_or_else(|| anyhow::anyhow!("Subblock proof is not reduced."))?;
            stdin.write_proof(*reduced_proof, self.subblock_vk.clone().vk);
        }
        let proof = self.client
            .prove(&self.agg_pk, &stdin)
            .compressed()
            .run()?;
        let dur = start.elapsed();
        info!("Agg proof generation took `{:.3}s`", dur.as_secs_f64());
        info!("Verifying Agg proof...");
        self.client.verify(&proof, &self.agg_vk)?;
        info!("Viola, verified!");
        Ok(bincode::serialize(&proof)?)
    }
}