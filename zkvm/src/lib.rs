mod r0;
mod sp1;

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
    Execute(ELFKind),

    ProveCompressed(ELFKind),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ELFKind {
    Subblock,

    Agg
}

pub fn run(
    inputs: Vec<Vec<u8>>,
    kind: JobKind,
) -> anyhow::Result<Vec<u8>> {    
    match kind {
        JobKind::R0(r0_job, _bid) => {
            match r0_job {
                R0Op::Segment => {
                    r0::aggregate_segments(inputs)
                },

                R0Op::Join => {
                    r0::aggregate_proofs(inputs)
                },        

                R0Op::Keccak => {            
                    r0::aggregate_keccaks(inputs)
                },

                R0Op::Union => {            
                    r0::aggregate_assumptions(inputs)
                },

                R0Op::Groth16 => {
                    let first = inputs.into_iter().next().unwrap();
                    r0::to_groth16(first)
                }
            }
        },        

        JobKind::SP1(sp1_job_kind, _bid) => {
            match sp1_job_kind {
                SP1Op::Execute(elf_kind) => {
                    let stdin = inputs.into_iter().next().unwrap();
                    let elf_name = match elf_kind {
                        ELFKind::Subblock => "subblock",

                        ELFKind::Agg => "agg"
                    };
                    let elf_path = format!("./elfs/{elf_name}_elf.bin");
                    sp1::execute_elf(&elf_path, stdin)
                },

                SP1Op::ProveCompressed(elf_kind) => {
                    let mut iter = inputs.into_iter();
                    let stdin = iter.next().unwrap();                    
                    match elf_kind {
                        ELFKind::Subblock => {                        
                            sp1::prove_compressed_subblock(
                                "./elfs/subblock_elf.bin",
                                stdin
                            )
                        },

                        ELFKind::Agg => {
                            sp1::prove_compressed_agg(
                                "./elfs/agg_elf.bin",
                                stdin,
                                iter.collect()
                            )
                        }
                    }                    
                },
            }
        }
    }
}