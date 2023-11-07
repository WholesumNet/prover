#![doc = include_str!("../README.md")]

use futures::{
    prelude::*, select,
    stream::FuturesUnordered,
};
use std::os::unix::process::ExitStatusExt;
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;

use clap::Parser;
use reqwest;
use libp2p::{
    gossipsub, mdns, request_response,
    swarm::{SwarmEvent},
    PeerId,
};

use comms::{
    p2p::{MyBehaviourEvent}, notice, compute
};
use dstorage::dfs;

mod job;

#[derive(Debug)]
struct PodShareResult {
    job_id: String,
    cid: String,
}

// CLI
#[derive(Parser, Debug)]
#[command(name = "Server CLI for Wholesum: p2p verifiable computing marketplace.")]
#[command(author = "Wholesum team")]
#[command(version = "0.1")]
#[command(about = "Yet another verifiable compute marketplace.", long_about = None)]
struct Cli {
    #[arg(short, long)]
    dfs_config_file: Option<String>,
}

#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("<-> `Server` agent for Wholesum network <->");
    
    let cli = Cli::parse();
    
    
    // FairOS-dfs http client
    let dfs_config_file = cli.dfs_config_file
        .ok_or_else(|| "FairOS-dfs config file is missing.")?;
    let dfs_config = toml::from_str(&std::fs::read_to_string(dfs_config_file)?)?;

    // let dfs_endpoint = String::from("http://localhost:9090");
    // let dfs_username = String::from("whole2");
    // let dfs_password = String::from("wholewhole00");
    let dfs_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60)) //@ how much timeout is enough?
        .build()
        .expect("FairOS-dfs server should be available and be running to continue.");
    let dfs_cookie = dfs::login(
        &dfs_client, 
        &dfs_config
    ).await
    .expect("Login failed, shutting down.");
    assert_ne!(
        dfs_cookie, String::from(""),
        "Cookie from FairOS-dfs cannot be empty."
    );

    // running jobs(docker containers for compute and verify)
    let mut compute_job_stream = job::DockerProcessStream::new();

    let mut jobs = HashMap::<String, job::Job>::new();

    let mut fd12_upload_futures = FuturesUnordered::new();
    let mut receipt_upload_futures = FuturesUnordered::new();
        
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
                    // println!("received gossip message: {:#?}", message);
                    // first byte is message identifier                
                    let notice_req = notice::Notice::try_from(message.data[0])?;
                    match notice_req {
                        notice::Notice::Compute => {
                            println!("`need compute` request from client: `{peer_id}`");
                            // engage with the client through a direct p2p channel
                            // and express interest in getting the compute job done
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

                        notice::Notice::JobStatus => {
                            // job status inquiry
                            // servers are lazy with job updates so clients need to query for their job's status every so often

                            // bytes [1-16] determine th job id 
                            // let bytes_id = match message.data[1..=17].try_into() {
                            //     Ok(b) => b,
                            //     Err(e) => {
                            //         println!("Invalid job id for `job-status` request, {e:?}");
                            //         continue;
                            //     },
                            // };
                            // let job_id = Uuid::from_bytes(bytes_id).to_string();
                            // println!("`job-status` request from client: `{}`",
                            //     peer_id);
                            let updates = job_status_of_peer(
                                &jobs,
                                peer_id
                            ); 
                            if updates.len() > 0 {
                                let _sw_req_id = swarm
                                    .behaviour_mut().req_resp
                                    .send_request(
                                        &peer_id,
                                        notice::Request::UpdateForJobs(updates),
                                    );
                                // println!("jobs' status was sent to the client. req_id: `{sw_req_id}`");                            
                            }
                        },

                        notice::Notice::Harvest => {
                            let updates = harvest_jobs_of_peer(
                                &jobs,
                                peer_id
                            );                            
                            if updates.len() > 0 {
                                let _sw_req_id = swarm
                                    .behaviour_mut().req_resp
                                    .send_request(
                                        &peer_id,
                                        notice::Request::UpdateForJobs(updates),
                                    );
                            }
                        },

                        _ => (),
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
                            if jobs.contains_key(&compute_details.job_id) {
                                println!("Duplicate compute job, ignored.");
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
                            jobs.insert(
                                compute_details.job_id.clone(),
                                job::Job {
                                    id: compute_details.job_id.clone(),                                        
                                    owner: peer_id,
                                    status: job::Status::DockerWarmingUp,
                                    residue: job::Residue {
                                        fd12_cid: None,
                                        receipt_cid: None,
                                    },
                                },
                            );
                        },

                        _ => (),
                    }
                },

                _ => {}

            },

            // compute job is finished
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
                // job has been finished and ready to be verified
                if false == jobs.contains_key(&process_handle.job_id) {
                    println!("Critical error: job is missing.");
                    //@ what to do here?
                    continue;
                }
                let job = jobs.get_mut(&process_handle.job_id).unwrap();

                if exit_code != 0 { 
                    job.status = job::Status::ExecutionFailed;
                    println!("Execution was a failure.");
                    continue;                  
                } 
                job.status = job::Status::ExecutionSucceeded;
                println!("Execution was a success.");
                // each job has at least two pods to store its residue:
                // - "stderr" and "stdout" are stored in "/" of a private pod
                //   to be revealed later once job is ready to be harvested
                // - "receipt" is stored in "/" of a public pod and for verification purposes
                
                //@ should get /var/lib.... path from a config file
                let docker_vol_path = format!("/var/lib/docker/volumes/{}/_data",
                    process_handle.job_id);                
                // persist and share receipt                              
                receipt_upload_futures.push(
                    persist_receipt(
                        &dfs_client, &dfs_config, &dfs_cookie,
                        format!("receipt_{}", process_handle.job_id),
                        String::from("/"), 
                        format!("{}/receipt", docker_vol_path),
                        process_handle.job_id.clone(),
                    )
                );
            },
            
            // handle stdout/err objects that have been uploaded to dfs
            fd12_upload_result = fd12_upload_futures.select_next_some() => {
                let pod_share_result: PodShareResult = match fd12_upload_result {
                    Ok(psr) => psr,
                    Err(e) => {
                        println!("Missing cid for the fd12 pod: `{e:?}`");
                        //@ what to do here
                        continue
                    }
                };
                println!("fd12 pod is now public: {:#?}", pod_share_result);
                if false == jobs.contains_key(&pod_share_result.job_id) {
                    println!("Residue pod's job data is missing.");
                    //@ what to do here?
                    continue;
                }
                let job = jobs.get_mut(&pod_share_result.job_id).unwrap();
                job.residue.fd12_cid = Some(pod_share_result.cid); 

            },

            // handle receipt objects that have been uploaded to dfs
            receipt_upload_result = receipt_upload_futures.select_next_some() => {
                let pod_share_result = match receipt_upload_result {
                    Ok(psr) => psr,
                    Err(e) => {
                        println!("Missing cid for the receipt pod: `{e:?}`");
                        //@ what to do here, what if client accepted un-verified jobs?
                        continue
                    }
                };
                println!("receipt is now public: {:#?}", pod_share_result);
                // job has been finished and ready to be verified
                if false == jobs.contains_key(&pod_share_result.job_id) {
                    println!("Receipt pod's job data is missing.");
                    //@ what to do here?
                    continue;
                }
                let job = jobs.get_mut(&pod_share_result.job_id).unwrap();
                job.residue.receipt_cid = Some(pod_share_result.cid); 
                // persist stdout and stderr too
                let docker_vol_path = format!("/var/lib/docker/volumes/{}/_data",
                    job.id);
                fd12_upload_futures.push(
                    persist_fd12(
                        &dfs_client, &dfs_config, &dfs_cookie,
                        job.id.clone(),
                        String::from("/"),
                        docker_vol_path.clone(),
                        job.id.clone(),
                    )
                ); 
            },
        }
    }
}

// retrieve all status of jobs owned by the peer_id
fn job_status_of_peer(
    jobs: &HashMap::<String, job::Job>,
    peer_id: PeerId
) -> Vec<compute::JobUpdate> {
    let mut updates = Vec::<compute::JobUpdate>::new();
    let iter = jobs.values().filter(|&j| j.owner == peer_id);
    for job in iter {
        let status = match job.status {
            job::Status::ExecutionSucceeded => 
                compute::JobStatus::ExecutionSucceeded(
                    job.residue.receipt_cid.clone()
            ),

            //@ how about sharing cid of stderr?
            job::Status::ExecutionFailed => 
                compute::JobStatus::ExecutionFailed(None),
            
            // all the rest are trivial status
            _ => compute::JobStatus::Running,
        };
        updates.push(compute::JobUpdate {
            id: job.id.clone(),
            status: status,
        });
    }
    updates
}

// retrieve all harvest jobs of the peer
fn harvest_jobs_of_peer(
    jobs: &HashMap::<String, job::Job>,
    peer_id: PeerId
) -> Vec<compute::JobUpdate> {
    let mut updates = Vec::<compute::JobUpdate>::new();
    let iter = jobs.values().filter(
        |&j| 
        j.owner == peer_id &&
        j.status == job::Status::ExecutionSucceeded
    );
    for job in iter {      
        if true == job.residue.fd12_cid.is_none() {
            println!("Warning: missing fd12 cid for the job `{}` that is being harvested.",
                job.id);
        }  
        updates.push(compute::JobUpdate {
            id: job.id.clone(),
            status: compute::JobStatus::Harvested(
                compute::HarvestDetails {
                    fd12_cid: job.residue.fd12_cid.clone(),
                    receipt_cid: job.residue.receipt_cid.clone(),
                }
            ),
        });
    }
    updates
}

async fn persist_fd12(
    dfs_client: &reqwest::Client,
    dfs_config: &dfs::Config,
    dfs_cookie: &String,
    pod_name: String,
    pod_dest_dir: String,
    local_base_path: String,
    job_id: String,
) -> Result<PodShareResult, Box<dyn Error>> {
    // upload stdout(1) and stderr(2) to a private pod
    // create pod
    dfs::new_pod(
        dfs_client, dfs_config, dfs_cookie,
        pod_name.clone()
    ).await?;
    // open it
    dfs::open_pod(
        dfs_client, dfs_config, dfs_cookie,
        pod_name.clone()
    ).await?;    
    // upload stdout
    dfs::upload_file(
        dfs_client, dfs_config, dfs_cookie, 
        pod_name.clone(), pod_dest_dir.clone(), 
        format!("{}/stdout", local_base_path)
    ).await?; 
    // upload stderr
    dfs::upload_file(
        dfs_client, dfs_config, dfs_cookie, 
        pod_name.clone(), pod_dest_dir, 
        format!("{}/stderr", local_base_path)
    ).await?;
    // share it
    let shared_pod = dfs::share_pod(
        dfs_client, dfs_config, dfs_cookie,
        pod_name
    ).await?;
    
    Ok(
        PodShareResult {
            job_id: job_id,
            cid: shared_pod.podSharingReference,            
        }
    )
}

async fn persist_receipt(
    dfs_client: &reqwest::Client,
    dfs_config: &dfs::Config,
    dfs_cookie: &String,
    pod_name: String,
    pod_dest_dir: String,
    local_receipt_path: String,
    job_id: String,
) -> Result<PodShareResult, Box<dyn Error>>  {
    // create pod
    dfs::new_pod(
        dfs_client, dfs_config, dfs_cookie,
        pod_name.clone()
    ).await?;
    // open it
    dfs::open_pod(
        dfs_client, dfs_config, dfs_cookie,
        pod_name.clone()
    ).await?;
    // upload receipt
    dfs::upload_file(
        dfs_client, dfs_config, dfs_cookie, 
        pod_name.clone(), pod_dest_dir, 
        local_receipt_path
    ).await?;
      
    // share it
    let shared_pod = dfs::share_pod(
        dfs_client, dfs_config, dfs_cookie,
        pod_name.clone()
    ).await?;
    
    Ok(
        PodShareResult {
            job_id: job_id,
            cid: shared_pod.podSharingReference,            
        }
    )
}
