use std::io::Error;
use core::pin::Pin;
use futures::stream::{Stream, FusedStream};
use futures::task::{Context, Poll};
use async_std::process::{Child, Command, Stdio};
use libp2p::PeerId;

#[derive(Debug)]
pub struct Residue {
    pub pod_cid: Option<String>,
    pub receipt_cid: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Status {
    DockerWarmingUp = 0,     // docker initializing
    Negotiating,             // making offers
    Running,
    ExecutionFinished,       // execution finished
    ReadyForVerification,    // receipt is uploaded and can be verified
    ReadyToHarvest,          // verified and ready to harvest
    ExecutionFailed,         
    VerificationFailed,      
    VerificationSucceeded,
}

// maintains lifecycle for a job
#[derive(Debug)]
pub struct Job {
    pub id: String,
    pub owner: PeerId,                      // the client
    pub status: Status,
    pub residue: Residue,                   // cids for stderr, output, receipt, ...
}

pub struct ProcessHandle {
    pub job_id: String,
    pub child: Child,
}

pub struct DockerProcessStream {
    process_handles: Vec<ProcessHandle>,
}

impl DockerProcessStream {
    pub fn new () -> DockerProcessStream {
        DockerProcessStream { process_handles: Vec::<ProcessHandle>::new() }
    }

    pub fn add(&mut self, job_id: String, image: String, cmd: String) -> Result<(), Error>{
        // let cmd_args = vec![
        //     "run", "--name", "abcd",
        //     "--mount src=aloha-vol,dst=/root/proofs",
        //     "test-risc0", "sh", "-c",
        //     "/root/risc0-0.17.0/examples/target/release/factors"
        // ];

        let container_name = format!("--name={job_id}");
        let mount = format!("--mount=src={job_id},dst=/root/residue");
        let cmd_args = vec![
            "run",
            "--rm",
            container_name.as_str(),
            mount.as_str(),
            image.as_str(),
            "sh",
            "-c",
            cmd.as_str(),
        ];
        println!("running docker with command `{:?}`", cmd_args);
        match Command::new("docker")
        // .current_dir(dir)
            .args(cmd_args)
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .stdout(Stdio::null())
            .spawn() {

            Ok(child) => {
                self.process_handles.push(
                    ProcessHandle { job_id, child }
                );
                return Ok(());
            },
            Err(e) => return Err(e),
        };
    }

    pub fn is_running(&self, job_id: &String) -> bool {
        // if in the list, then it's running
        self.process_handles.iter().any(|jh| jh.job_id == *job_id)
    }
}

impl Stream for DockerProcessStream {
    type Item = ProcessHandle;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> { 
        for (index, jh) in self.process_handles.iter_mut().enumerate() {
            if let Ok(Some(_)) = jh.child.try_status() {
                return Poll::Ready(Some(self.process_handles.remove(index)));
            }
        }
        Poll::Pending        
    }
}

impl FusedStream for DockerProcessStream {
    fn is_terminated(&self) -> bool { false } 
}
