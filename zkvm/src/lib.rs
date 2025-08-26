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
    Shard,

    Join,
}

pub fn run(
    blobs: Vec<Vec<u8>>,
    kind: JobKind,
) -> anyhow::Result<Vec<u8>> {    
    match kind {
        JobKind::R0(r0_job, _bid) => {
            match r0_job {
                R0Op::Segment => {
                    r0::aggregate_segments(blobs)
                },

                R0Op::Join => {
                    r0::aggregate_proofs(blobs)
                },        

                R0Op::Keccak => {            
                    r0::aggregate_keccaks(blobs)
                },

                R0Op::Union => {            
                    r0::aggregate_assumptions(blobs)
                },

                R0Op::Groth16 => {
                    let first = blobs.into_iter().next().unwrap();
                    r0::to_groth16(first)
                }
            }
        },        

        JobKind::SP1(sp1_job_kind, _bid) => {
            match sp1_job_kind {
                SP1Op::Shard => todo!{},

                SP1Op::Join => todo!{},
            }
        }
    }
}