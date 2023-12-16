use std::io::{Error, ErrorKind};
use core::pin::Pin;
use futures::stream::{Stream, FusedStream};
use futures::task::{Context, Poll};
use async_std::process::{Child, Command, Stdio};
use libp2p::PeerId;
use home;
use std::fs;

#[derive(Debug)]
pub struct Residue {
    pub fd12_cid: Option<String>,
    pub receipt_cid: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Status {
    DockerWarmingUp = 0,    // docker initializing
    Negotiating,            // making offers
    Running,
    ExecutionSucceeded,     // and ready to be verified or to be harvested
    ExecutionFailed,
}

// maintain lifecycle of a job
#[derive(Debug)]
pub struct Job {
    pub id: String,
    pub owner: PeerId,                      // the client
    pub status: Status,
    pub residue: Residue,                   // cids for stderr, output, receipt, ...
}

impl Job {
    pub fn get_residue_path(job_id: &String) -> Result<String, Error> {
        let home_dir = home::home_dir()
            .ok_or_else(|| Error::new(ErrorKind::Other, "Home dir is not available."))?
            .into_os_string().into_string()
            .or_else(|_| Err(Error::new(ErrorKind::Other, "OS_String conversion failed.")))?;
        Ok(format!("{home_dir}/.wholesum/jobs/compute/{job_id}/residue"))
    }
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

    // run the docker job
    pub fn add(&mut self, job_id: String, image: String, cmd: String) -> Result<(), Error>{
        // create job's directory 
        let src_vol_path = Job::get_residue_path(&job_id)?;
        fs::create_dir_all(src_vol_path.clone())?;

        let container_name = format!("--name={job_id}");
        let mount = format!("--mount=type=bind,src={src_vol_path},dst=/home/prince/residue");
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
