use std::fs;
use log::info;
use anyhow;
use bincode;
use sp1_sdk::{
    SP1Stdin,
    CudaProver, Prover, ProverClient,
    SP1ProvingKey, SP1VerifyingKey, 
    SP1ProofWithPublicValues,
};

mod r0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobKind {
    // param: zkvm kind, batch id
    R0(R0Op, u128),

    SP1(SP1Op, u128)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum R0Op {
    Segment,
    Join,

    Keccak,
    Union,

    Groth16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SP1Op {
    ProveSubblock,

    ProveAgg,
}

pub struct SP1Handle {
    client: CudaProver,
    
    subblock_pk: SP1ProvingKey,
    subblock_vk: SP1VerifyingKey,
    
    agg_pk: SP1ProvingKey
}

impl SP1Handle {
    pub fn new() -> anyhow::Result<Self> {
        let cuda_client = ProverClient::builder().cuda().build();
        // subblock
        let subblock_elf = fs::read("./elfs/subblock_elf.bin")?;
        let (subblock_pk, subblock_vk) = cuda_client.setup(&subblock_elf);
        // agg
        let agg_elf = fs::read("./elfs/agg_elf.bin")?;
        let (agg_pk, _agg_vk) = cuda_client.setup(&agg_elf);

        Ok(Self {
            client: cuda_client,
            subblock_pk: subblock_pk,
            subblock_vk: subblock_vk,
            agg_pk: agg_pk
        })
    }

    pub fn prove_subblock(
        &self,
        stdin_blob: Vec<u8>,
    ) -> anyhow::Result<Vec<u8>> {    
        info!("Proving Subblock ELF...");
        let stdin: SP1Stdin = bincode::deserialize(&stdin_blob)?;    
        let proof = self.client
            .prove(&self.subblock_pk, &stdin)
            .compressed()
            .run()?;
        Ok(bincode::serialize(&proof)?)
    }

    pub fn prove_agg(
        &self,
        stdin_blob: Vec<u8>,
        subblock_proofs: Vec<Vec<u8>>,
    ) -> anyhow::Result<Vec<u8>> {    
        info!("Proving Agg ELF...");
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
        Ok(bincode::serialize(&proof)?)
    }    
}

// pub fn run(
//     inputs: Vec<Vec<u8>>,
//     kind: JobKind,
// ) -> anyhow::Result<Vec<u8>> {    
//     match kind {
//         JobKind::R0(r0_job, _bid) => {
//             match r0_job {
//                 R0Op::Segment => {
//                     r0::aggregate_segments(inputs)
//                 },

//                 R0Op::Join => {
//                     r0::aggregate_proofs(inputs)
//                 },        

//                 R0Op::Keccak => {            
//                     r0::aggregate_keccaks(inputs)
//                 },

//                 R0Op::Union => {            
//                     r0::aggregate_assumptions(inputs)
//                 },

//                 R0Op::Groth16 => {
//                     let first = inputs.into_iter().next().unwrap();
//                     r0::to_groth16(first)
//                 }
//             }
//         },        

//         JobKind::SP1(_sp1_job_kind, _bid) => {}
//     }
// }