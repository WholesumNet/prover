#![doc = include_str!("../README.md")]

use futures::{
    prelude::*, select, FutureExt,
    stream::FuturesUnordered,
};
use libp2p::{
    gossipsub, mdns, request_response,
    swarm::{SwarmEvent},
    // PeerId,
};
use std::os::unix::process::ExitStatusExt;
use std::collections::HashMap;
use std::error::Error;
// use std::time::Duration;
use clap::Parser;
use uuid::Uuid;
use reqwest;

use comms::{
    p2p::{MyBehaviourEvent}, notice, compute
};

mod job;
mod dstore;

// CLI
#[derive(Parser, Debug)]
#[command(name = "Server CLI for Wholesum: p2p verifiable computing marketplace.")]
#[command(author = "Wholesum team")]
#[command(version = "0.1")]
#[command(about = "Yet another verifiable compute marketplace.", long_about = None)]
struct Cli {
    #[arg(short, long)]
    verify: bool,
}

#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
    
    let cli = Cli::parse();
    if cli.verify == true {
        println!("Will also do verification.");
    }
    
    // running jobs(docker containers for compute and verify)
    let mut compute_job_stream = job::DockerProcessStream::new();
    let mut verification_job_stream = job::DockerProcessStream::new();

    let mut compute_jobs = HashMap::<String, job::Job>::new();
    let mut verification_jobs = HashMap::<String, job::Job>::new();

    // because every closure is a unique type
    let mut stdout_residue_upload_futures = FuturesUnordered::new();
    let mut stderr_residue_upload_futures = FuturesUnordered::new();
    let mut receipt_residue_upload_futures = FuturesUnordered::new();

    // FairOSD-dfs http client
    let dfs_client = reqwest::Client::new();
    // Libp2p swarm 
    let mut swarm = comms::p2p::setup_local_swarm();

    // read full lines from stdin
    // let mut input = io::BufReader::new(io::stdin()).lines().fuse();

    // listen on all interfaces and whatever port the os assigns
    swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    // oh shit here we go again
    loop {
        select! {
            // line = input.select_next_some() => {
            //   if let Err(e) = swarm
            //     .behaviour_mut().gossipsub
            //     .publish(topic.clone(), line.expect("Stdin not to close").as_bytes()) {
            //       println!("Publish error: {e:?}")
            //     }
            // },
            event = swarm.select_next_some() => match event {
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discovered a new peer: {peer_id}");
                        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                    }
                },

                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discovered peer has expired: {peer_id}");
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                    }
                },

                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Local node is listening on {address}");
                },

                SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source: peer_id,
                    message,
                    ..
                })) => {
                    // let msg_str = String::from_utf8_lossy(&message.data);
                    // println!("Got message: '{}' with id: {id} from peer: {peer_id}",
                    //          msg_str);
                    println!("received gossip message: {:#?}", message);
                    // first byte is message identifier                
                    let notice_req = notice::Notice::try_from(message.data[0])?;
                    match notice_req {
                        notice::Notice::Compute => {
                            println!("`need compute` request from client: `{peer_id}`");
                            // engage with the client through a direct p2p channel
                            // and express interest in getting the compute done
                            let offer = compute::Offer {
                                price: 1,
                                hw_specs: compute::ServerSpecs {
                                    gflops: 100,
                                    ram_amount: 16_000,
                                    cpu_model: "core i7-5500u".to_string(),
                                },
                            };
                            let sw_req_id = swarm
                                .behaviour_mut().req_resp
                                .send_request(
                                    &peer_id,
                                    notice::Request::ComputeOffer(offer),
                                );
                            println!("compute offer was sent to client, id: {sw_req_id}");
                        },

                        notice::Notice::Verify => {
                            if false == cli.verify {
                                continue;
                            }
                            println!("`need verify` request from client: `{peer_id}`");
                            // engage with the client through a direct p2p channel
                            let sw_req_id = swarm
                                .behaviour_mut().req_resp
                                .send_request(
                                    &peer_id,
                                    notice::Request::VerifyOffer,
                                );
                            println!("verification offer was sent, id: {sw_req_id}");
                        },

                        notice::Notice::JobStatus => {
                            // job status inquiry
                            // servers are lazy and clients need to query job status

                            // bytes [1-16] determine th job id 
                            let bytes_id = match message.data[1..=17].try_into() {
                                Ok(b) => b,
                                Err(e) => {
                                    println!("Invalid job id for `job-status` request, {e:?}");
                                    continue;
                                },
                            };
                            let job_id = Uuid::from_bytes(bytes_id).to_string();
                            println!("`job-status` request from client: `{}`, job_id: `{}`",
                                peer_id, job_id);
                            // map internal status to network-wide status
                            // check in compute jobs
                            if let Some(compute_job) = compute_jobs.get(&job_id) {
                                if peer_id != compute_job.owner {
                                    continue;
                                }
                                let job_status = match compute_job.status {
                                    job::Status::ExecutionFailed => {
                                        compute::JobStatus::ExecutionFailed(
                                            compute_job.residue.stdout_cid.clone(),
                                            compute_job.residue.stderr_cid.clone(),
                                        )
                                    },
                                    job::Status::ReadyForVerification => {
                                        compute::JobStatus::ReadyForVerification(
                                            compute_job.residue.receipt_cid.clone()
                                        )
                                    },
                                    //@ payment must be made before harvest
                                    job::Status::ReadyToHarvest => {
                                        compute::JobStatus::ReadyToHarvest
                                    },
                                    // all the rest are trivial status
                                    _ => compute::JobStatus::Running,
                                };
                                let sw_req_id = swarm
                                    .behaviour_mut().req_resp
                                    .send_request(
                                        &peer_id,
                                        notice::Request::UpdateForJob(compute::JobUpdate{
                                            id: job_id.clone(),
                                            status: job_status,
                                        }),
                                    );
                                println!("compute job status was sent to the client. req_id: `{sw_req_id}`");
                            }
                            // check in verification jobs
                            if let Some(verification_job) = verification_jobs.get(&job_id) {
                                // if false == cli.verify {
                                //     continue;
                                // }
                                // relax and let non-owners to query a verification job as well
                                // if peer_id != verification_job.owner {
                                //     continue;
                                // }
                                let job_status = match verification_job.status {
                                    job::Status::VerificationSucceeded => {
                                        compute::JobStatus::VerificationSucceeded
                                    },                                    
                                    job::Status::VerificationFailed => {
                                        compute::JobStatus::VerificationFailed(
                                            verification_job.residue.stderr_cid.clone(),
                                        )
                                    },
                                    // anything else is not-important for verification queries
                                    _ => compute::JobStatus::Running,
                                };
                                let sw_req_id = swarm
                                    .behaviour_mut().req_resp
                                    .send_request(
                                        &peer_id,
                                        notice::Request::UpdateForJob(compute::JobUpdate{
                                            id: job_id.clone(),
                                            status: job_status,
                                        }),
                                    );
                                println!("Verification job status was sent to the client. req_id: `{sw_req_id}`");
                            }

                            
                        }
                    };
                },
                
                // incoming response to an earlier compute/verify offer
                SwarmEvent::Behaviour(MyBehaviourEvent::ReqResp(request_response::Event::Message{
                    peer: peer_id,
                    message: request_response::Message::Response {
                        response,
                        ..
                    }
                })) => {                    
                    match response {
                        notice::Response::DeclinedOffer => {
                            println!("Offer decliend by the client: `{peer_id}`");
                        },                        

                        notice::Response::ComputeJob(compute_details) => {
                            println!("received `compute job` request from client: `{}`, job: `{:#?}`",
                                peer_id, compute_details);                           
                            // no duplicate job_ids are allowed
                            if compute_jobs.contains_key(&compute_details.job_id) {
                                println!("Duplicate job, ignored.");
                                continue;
                            }
                            // schedule the job to run                            
                            if let Err(e) = compute_job_stream.add(
                                compute_details.job_id.clone(),
                                compute_details.docker_image.clone(),
                                compute_details.command.clone()
                            ) {

                                println!("Job spawn error: `{:?}`", e);
                                continue;
                            }
                            // keep track of running jobs
                            compute_jobs.insert(
                                compute_details.job_id.clone(),
                                job::Job {
                                    id: compute_details.job_id,
                                    owner: peer_id,
                                    status: job::Status::DockerWarmingUp,
                                    residue: job::Residue {
                                        stdout_cid: None,
                                        stderr_cid: None,
                                        receipt_cid: None,
                                    },
                                },
                            );
                        },

                        notice::Response::VerificationJob(verification_details) => {
                            if false == cli.verify {
                                continue;
                            }
                            println!("received `verification job` request from client: `{}`, job: `{:#?}`",
                                peer_id, verification_details);                           
                            // no duplicate job_ids are allowed
                            if verification_jobs.contains_key(&verification_details.job_id) {
                                println!("Duplicate job, ignored.");
                                continue;
                            }
                            // schedule the job to run 
                            let cmd = format!(
                                "sh -c /root/verifier/target/verifier --image-id {} --receipt-file {}",
                                verification_details.image_id,
                                verification_details.receipt_cid,
                            );
                            if let Err(e) = verification_job_stream.add(
                                verification_details.job_id.clone(),
                                "test-risc0".to_string(),
                                cmd,
                            ) {

                                println!("Job spawn error: `{e:?}`");
                                continue;
                            }
                            // keep track of running jobs
                            verification_jobs.insert(
                                verification_details.job_id.clone(),
                                job::Job {
                                    id: verification_details.job_id,
                                    owner: peer_id,
                                    status: job::Status::DockerWarmingUp,
                                    residue: job::Residue {
                                        stdout_cid: None,
                                        stderr_cid: None,
                                        receipt_cid: None,
                                    },
                                },
                            );
                        }
                    }
                },

                _ => {}

            },

            // docker verification job is finished
            mut process_handle = verification_job_stream.select_next_some() => {
                println!("Docker process for verification job `{}` has been finished.",
                    process_handle.job_id);
                //@ collect any relevant objects before terminating process
                let exit_code = match process_handle.child.status().await {
                    Ok(status) => status.code().unwrap_or_else(
                        || status.signal().unwrap_or_else(
                            || {
                                println!("Docker process was terminated by a signal but \
                                    the signal is not available.");
                                99
                            })
                    ),
                    Err(e) => {
                        println!("Failed to retrieve docker process's exit status: {e:?}");
                        99
                    }
                };                
                // notify the client that the job has been finished and ready to be verified
                if false == verification_jobs.contains_key(&process_handle.job_id) {
                    println!("Critical error: job is missing.");
                    //@ what to do here?
                    continue;
                }
                let job = verification_jobs.get_mut(&process_handle.job_id).unwrap();
                job.status = if exit_code == 0 { 
                    job::Status::VerificationSucceeded
                } else {
                    job::Status::VerificationFailed 
                };                

            },

            // docker compute job is finished
            mut process_handle = compute_job_stream.select_next_some() => {
                println!("Docker process for compute job `{}` has been finished.",
                    process_handle.job_id);
                //@ collect any relevant objects before terminating process
                let exit_code = match process_handle.child.status().await {
                    Ok(status) => status.code().unwrap_or_else(
                        || status.signal().unwrap_or_else(
                            || {
                                println!("Docker process was terminated by a signal but \
                                    the signal is not available.");
                                99
                            })
                    ),
                    Err(e) => {
                        println!("Failed to retrieve docker process's exit status: {e:?}");
                        99
                    }
                };                
                // notify the client that the job has been finished and ready to be verified
                if false == compute_jobs.contains_key(&process_handle.job_id) {
                    println!("Critical error: job is missing.");
                    //@ what to do here?
                    continue;
                }
                let job = compute_jobs.get_mut(&process_handle.job_id).unwrap();

                if exit_code != 0 {
                    job.status = job::Status::ExecutionFailed;
                }
                // store stdout, stderr, ... in dfs and get cid
                //@ should get /var/lib.... path from a config file
                let docker_vol_path = format!("/var/lib/docker/volumes/{}/_data",
                    process_handle.job_id);
                // persist stdout
                let job_id_for_stdout = process_handle.job_id.clone();                  
                let stdout_fut = dfs_client.get(
                    format!("{docker_vol_path}/residue/stdout")
                    )
                    .send()
                    .then(|resp| async move {
                        let cid = match resp {
                            Ok(_resp) => {
                                // extract cid
                                "cid was generated".to_string()                                    
                            },
                            Err(e) => {
                                println!("job_id: `{}`, residue: `stdout`, upload error: {:?}",
                                    job_id_for_stdout, e);
                                //@ this is a criticial error
                                "".to_string()
                            },
                        };
                        
                        dstore::UploadResult {
                            job_id: job_id_for_stdout,
                            cid: cid,
                        }
                    });
                stdout_residue_upload_futures.push(stdout_fut);
                // persist stderr
                let job_id_for_stderr = process_handle.job_id.clone();
                let stderr_fut = dfs_client.get(
                    format!("{docker_vol_path}/residue/stderr")
                    )
                    .send()
                    .then(|resp| async move {
                        let cid = match resp {
                            Ok(_resp) => {
                                // extract cid
                                "cid was generated".to_string()                                    
                            },
                            Err(e) => {
                                println!("job_id: `{}`, residue: `stderr`, upload error: {:?}",
                                    job_id_for_stderr, e);
                                //@ this is a criticial error
                                "".to_string()
                            },
                        };
                        
                        dstore::UploadResult {
                            job_id: job_id_for_stderr,
                            cid: cid,
                        }
                    });
                stderr_residue_upload_futures.push(stderr_fut);                    
                // persist receipt
                let job_id_for_receipt = process_handle.job_id.clone();
                let receipt_fut = dfs_client.get(
                    format!("{docker_vol_path}/residue/receipt")
                    )
                    .send()
                    .then(|resp| async move {
                        let cid = match resp {
                            Ok(_resp) => {
                                // extract cid
                                "cid was generated".to_string()                                    
                            },
                            Err(e) => {
                                println!("job_id: `{}`, residue: `receipt`, upload error: {:?}",
                                    job_id_for_receipt, e);
                                //@ this is a criticial error
                                "".to_string()
                            },
                        };
                        
                        dstore::UploadResult {
                            job_id: job_id_for_receipt,
                            cid: cid,
                        }
                    });
                receipt_residue_upload_futures.push(receipt_fut);
            },
            
            // handle stdout objects that have been uploaded to dfs
            stdout_upload_result = stdout_residue_upload_futures.select_next_some() => {
                if false == compute_jobs.contains_key(&stdout_upload_result.job_id) {
                    println!("Critical error: job is missing.");
                    //@ what to do here?
                    continue;
                }
                let job = compute_jobs.get_mut(&stdout_upload_result.job_id).unwrap();
                job.residue.stdout_cid = Some(stdout_upload_result.cid);
            },
            
            // handle stdout objects that have been uploaded to dfs
            stderr_upload_result = stderr_residue_upload_futures.select_next_some() => { 
                if false == compute_jobs.contains_key(&stderr_upload_result.job_id) {
                    println!("Critical error: job is missing.");
                    //@ what to do here?
                    continue;
                }
                let job = compute_jobs.get_mut(&stderr_upload_result.job_id).unwrap();
                job.residue.stderr_cid = Some(stderr_upload_result.cid);              
            },

            // handle receipt objects that have been uploaded to dfs
            receipt_upload_result = receipt_residue_upload_futures.select_next_some() => {
                // notify the client that the job has been finished and ready to be verified
                if false == compute_jobs.contains_key(&receipt_upload_result.job_id) {
                    println!("Critical error: job is missing.");
                    //@ what to do here?
                    continue;
                }
                let job = compute_jobs.get_mut(&receipt_upload_result.job_id).unwrap();
                job.status = job::Status::ReadyForVerification;
                job.residue.receipt_cid = Some(receipt_upload_result.cid.clone()); 
            },
        }
    }
}
