#![doc = include_str!("../README.md")]

use futures::{
    select,
    stream::{
        FuturesUnordered,
        StreamExt,
    },
};

use std::{
    error::Error,
    time::Duration,
    collections::{
        HashMap, 
        HashSet,
    },
};

use bollard::Docker;
use jocker::exec::{
    import_docker_image,
    run_docker_job,
};

use clap::Parser;
use reqwest;
use libp2p::{
    swarm::{
        SwarmEvent
    },
    gossipsub, mdns, request_response,
    PeerId,
};

use comms::{
    p2p::{
        LocalBehaviourEvent
    },
    notice, compute,
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
#[command(name = "Server CLI for Wholesum")]
#[command(author = "Wholesum team")]
#[command(version = "0.1")]
#[command(about = "Wholesum is a P2P verifiable computing marketplace and \
                   this program is a CLI for server nodes.",
          long_about = None)]
struct Cli {
    #[arg(short, long)]
    dfs_config_file: Option<String>,
}

#[async_std::main]
async fn main() -> Result<(), Box<dyn Error + 'static>> {
    println!("<-> `Server` agent for Wholesum network <->");
    
    let cli = Cli::parse();
    
    
    // FairOS-dfs http client
    let dfs_config_file = cli.dfs_config_file
        .ok_or_else(|| "FairOS-dfs config file is missing.")?;
    let dfs_config = toml::from_str(&std::fs::read_to_string(dfs_config_file)?)?;

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

    println!("Connecting to docker daemon...");
    let docker_con = Docker::connect_with_socket_defaults()?;

    // let's maintain a list of jobs
    let mut jobs = HashMap::<String, job::Job>::new();
    // job execution futures
    let mut job_execution_futures = FuturesUnordered::new();
    // advanced image pull futures
    let mut advanced_image_import_futures = FuturesUnordered::new();


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
                SwarmEvent::Behaviour(LocalBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discovered a new peer: {peer_id}");
                        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                    }
                },

                SwarmEvent::Behaviour(LocalBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discovered peer has expired: {peer_id}");
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                    }
                },

                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Local node is listening on {address}");
                },

                SwarmEvent::Behaviour(LocalBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source: peer_id,
                    message,
                    ..
                })) => {
                    // let msg_str = String::from_utf8_lossy(&message.data);
                    // println!("Got message: '{}' with id: {id} from peer: {peer_id}",
                    //          msg_str);
                    // println!("received gossip message: {:#?}", message);
                    // first byte is message identifier
                    if message.data.len() == 0 {
                        continue;
                    }              
                    let notice_req = notice::Notice::try_from(message.data[0])?;
                    match notice_req {
                        notice::Notice::Compute => {
                            // advanced image pull requested
                            if message.data.len() > 1 {
                                let docker_image = String::from_utf8_lossy(&message.data[1..]).to_string();
                                advanced_image_import_futures.push(
                                    import_docker_image(
                                        &docker_con,
                                        docker_image.clone()
                                    )
                                );
                                println!("Advanced docker image import request for `{docker_image}`");
                            }

                            println!("`Need compute` request from client: `{peer_id}`");
                            // engage with the client through a direct p2p channel
                            // and express interest in getting the compute job done
                            let offer = compute::Offer {
                                price: 1,
                                hw_specs: compute::ServerSpecs {
                                    gflops: 100,
                                    memory_capacity: 16,
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
                SwarmEvent::Behaviour(LocalBehaviourEvent::ReqResp(request_response::Event::Message{
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

                            let command = vec![
                                String::from("/bin/sh"),
                                String::from("-c"),
                                compute_details.command
                            ];
                            // create directory for residue
                            let residue_path = format!(
                                "{}/compute/{}/residue",
                                job::get_residue_path()?,
                                compute_details.job_id
                            );
                            std::fs::create_dir_all(residue_path.clone())?;

                            job_execution_futures.push(
                                run_docker_job(
                                    &docker_con,
                                    compute_details.job_id.clone(),
                                    compute_details.docker_image.clone(),
                                    command.clone(),
                                    residue_path.clone()
                                )
                            );

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

            // docker image import result is ready
            image_import_result = advanced_image_import_futures.select_next_some() => {
                if let Err(err) = image_import_result {
                    eprintln!("Advanced image import failed: `{err:#?}`");
                    //@ what to do with err.who(image name)?
                    continue;
                }
                let _image = image_import_result.unwrap();
            },

            // compute job is finished
            job_exec_res = job_execution_futures.select_next_some() => {                
                if let Err(failed) = job_exec_res {
                    eprintln!("Failed to run the job: `{:#?}`", failed);       
                    //@ what to to with job id?
                    let _job_id = failed.who;
                    continue;
                }
                let result = job_exec_res.unwrap();
                if false == jobs.contains_key(&result.job_id) {
                    println!("Critical error: job `{}` is missing.", result.job_id);
                    //@ what to do here?
                    continue;
                }
                let job = jobs.get_mut(&result.job_id).unwrap();
                if result.exit_status_code != 0 { 
                    job.status = job::Status::ExecutionFailed;
                    println!("Job `{}`'s execution finished with error: `{}`",
                        result.job_id,
                        result.error_message.unwrap_or_else(|| String::from("")),
                    );
                    continue;                 
                } 
                job.status = job::Status::ExecutionSucceeded;
                println!("Execution was a success.");
                // a job has at least two pods to store its residue:
                //   - "fd12": a private pod to store "stderr" and "stdout", harvested once the payment is processed
                //   - "receipt": a public pod to store Risc0-receipt of the execution, used for verification
                
                //@ if err, then program exits, must be handled properly
                let residue_path = format!(
                    "{}/compute/{}/residue",
                    job::get_residue_path()?,
                    result.job_id
                );
                // persist and share receipt                              
                receipt_upload_futures.push(
                    persist_receipt(
                        &dfs_client, &dfs_config, &dfs_cookie,
                        format!("receipt_{}", result.job_id),
                        String::from("/"), 
                        format!("{}/receipt", residue_path),
                        result.job_id.clone(),
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
                //@ if err, then program exits, must be handled properly
                let residue_path = format!(
                    "{}/compute/{}/residue",
                    job::get_residue_path()?,
                    job.id
                );
                fd12_upload_futures.push(
                    persist_fd12(
                        &dfs_client, &dfs_config, &dfs_cookie,
                        job.id.clone(),
                        String::from("/"),
                        residue_path.clone(),
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


// upload stdout(fd 1) and stderr(fd 2) to a private pod
async fn persist_fd12(
    dfs_client: &reqwest::Client,
    dfs_config: &dfs::Config,
    dfs_cookie: &String,
    pod_name: String,
    pod_dest_dir: String,
    local_base_path: String,
    job_id: String,
) -> Result<PodShareResult, Box<dyn Error>> {
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
