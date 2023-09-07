use std::io::Error;
use core::pin::Pin;
use futures::stream::{Stream, FusedStream};
use futures::task::{Context, Poll};
use async_std::process::{Child, Command, Stdio};
use libp2p::PeerId;
use comms::compute::JobDetails;

// running objects
#[derive(Debug)]
pub struct JobInfo {
    pub owner: PeerId,             // the client
    pub job_details: JobDetails,   // the contract
}


pub struct JobHandle {
    pub id: String,
    pub child: Child,
}

pub struct DockerJob {
    job_handles: Vec<JobHandle>,
}

impl DockerJob {
    pub fn new () -> DockerJob {
        DockerJob { job_handles: Vec::<JobHandle>::new() }
    }

    pub fn add_job(&mut self, id: String, image: String, cmd: String) -> Result<(), Error>{
        // let cmd_args = vec!["run", "test-risc0", "sh", "-c", "/root/risc0-0.17.0/examples/target/release/factors"];
            let mut cmd_args = vec!["run", image.as_str()];
            cmd_args.extend(cmd.split(' '));
            println!("running docker with command `{:?}`", cmd_args);
        match Command::new("docker")
        // .current_dir(dir)
            .args(cmd_args)
            .stdin(Stdio::null())
            .stderr(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn() {

            Ok(child) => {
                self.job_handles.push(
                    JobHandle { id, child }
                );
                return Ok(());
            },
            Err(e) => return Err(e),
        };
    }

    pub fn is_job_running(&self, id: &String) -> bool {
        // if in the list, then it's running
        self.job_handles.iter().any(|jh| jh.id == *id)
    }
}

impl Stream for DockerJob {
    type Item = JobHandle;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> { 
        for (index, jh) in self.job_handles.iter_mut().enumerate() {
            if let Ok(Some(_)) = jh.child.try_status() {
                return Poll::Ready(Some(self.job_handles.remove(index)));
            }
        }
        Poll::Pending        
    }
}

impl FusedStream for DockerJob {
    fn is_terminated(&self) -> bool { false } 
}
