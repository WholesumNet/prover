#![doc = include_str!("../README.md")]

use futures::{
    select,
    stream::{
        FuturesUnordered,
        StreamExt,
    },
};

use async_std::stream;

use std::{
    fs,
    error::Error,
    time::Duration,
    collections::{
        HashMap, 
    },
};

use bincode;
use chrono::{DateTime, Utc};

use bollard::Docker;
use jocker::exec::{
    import_docker_image,
    run_docker_job,
};

use clap::Parser;
use reqwest;

use libp2p::{
    gossipsub, mdns, request_response,
    identity, identify, kad,  
    swarm::{SwarmEvent},
    PeerId,
};

use tracing_subscriber::EnvFilter;

use comms::{
    p2p::{
        MyBehaviourEvent
    },
    notice, compute,
};
use dstorage::lighthouse;

use uuid::Uuid;
use anyhow;

mod job;
mod benchmark;
mod recursion;

#[derive(Debug)]
struct ResidueUploadResult {
    job_id: String,
    cid: String,
}

// CLI
#[derive(Parser, Debug)]
#[command(name = "Prover CLI for Wholesum")]
#[command(author = "Wholesum team")]
#[command(version = "0.1")]
#[command(about = "Wholesum is a P2P verifiable computing marketplace and \
                   this program is a CLI for prover nodes.",
          long_about = None)
]
struct Cli {
    #[arg(short, long)]
    dstorage_key_file: Option<String>,

    #[arg(long, action)]
    dev: bool,

    #[arg(short, long)]
    key_file: Option<String>,
}

#[async_std::main]
async fn main() -> Result<(), Box<dyn Error + 'static>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();
    
    let cli = Cli::parse();
    println!("<-> `Prover` agent for Wholesum network <->");
    println!("Operating mode: `{}` network",
        if false == cli.dev { "global" } else { "local(development)" }
    ); 
    
    let ds_key_file = cli.dstorage_key_file
        .ok_or_else(|| "dStorage key file is missing.")?;
    let lighthouse_config: dstorage::lighthouse::Config = 
        toml::from_str(&std::fs::read_to_string(ds_key_file)?)?;
    let ds_key = lighthouse_config.apiKey;

    let ds_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60)) //@ how much timeout is enough?
        .build()?;

    println!("Connecting to docker daemon...");
    let docker_con = Docker::connect_with_socket_defaults()?;
    // docker_con.ping()?.map_ok(|_| Ok::<_, ()>(println!("Connection succeeded.")));

    // let's maintain a list of jobs
    let mut jobs = HashMap::<String, job::Job>::new();
    // pull till job requirements are met, ie cids are downloaded
    let mut prepare_job_reqs_futures = FuturesUnordered::new();
    // pull till cids are download
    // let mut blob_download_futures = FuturesUnordered::new();
    // pull jobs' execution
    let mut prove_execution_futures = FuturesUnordered::new();
    let mut join_execution_futures = FuturesUnordered::new();
    let mut snark_execution_futures = FuturesUnordered::new();
    

    // benchmark maintenance
    let mut benchmarks = HashMap::<String, benchmark::Benchmark>::new();
    // let mut benchmark_exec_futures = FuturesUnordered::new();
    // {
    //     let (bench_job_id, docker_image, bench_command, residue_path) = prepare_benchmark_job()?;
    //     benchmark_exec_futures.push(
    //         run_docker_job(
    //             &docker_con,
    //             bench_job_id.clone(),
    //             docker_image.clone(),
    //             bench_command.clone(),
    //             residue_path.clone()
    //         )
    //     );
    //     benchmarks.insert(bench_job_id.clone(), benchmark::Benchmark {
    //         id: bench_job_id.clone(),
    //         cid: None,
    //         timestamp: None,
    //     });
    // }
    let mut timer_benchmark_exec = stream::interval(
        Duration::from_secs(24 * 60 * 60)
    ).fuse();
    // let mut benchmark_upload_futures = FuturesUnordered::new();
        
    // key 
    let local_key = {
        if let Some(key_file) = cli.key_file {
            let bytes = std::fs::read(key_file).unwrap();
            identity::Keypair::from_protobuf_encoding(&bytes)?
        } else {
            // Create a random key for ourselves
            let new_key = identity::Keypair::generate_ed25519();
            let bytes = new_key.to_protobuf_encoding().unwrap();
            let _bw = std::fs::write("./key.secret", bytes);
            println!("No keys were supplied, so one has been generated for you and saved to `{}` file.", "./key.secret");
            new_key
        }
    };    
    println!("my peer id: `{:?}`", PeerId::from_public_key(&local_key.public()));    

    // Libp2p swarm 
    let mut swarm = comms::p2p::setup_swarm(&local_key).await?;
    let topic = gossipsub::IdentTopic::new("<-- p2p compute bazaar -->");
    let _ = 
        swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&topic);

    // bootstrap 
    if false == cli.dev {
        // get to know bootnodes
        const BOOTNODES: [&str; 1] = [
            "12D3KooWLVDsEUT8YKMbZf3zTihL3iBGoSyZnewWgpdv9B7if7Sn",
        ];
        for peer in &BOOTNODES {
            swarm.behaviour_mut()
                .kademlia
                .add_address(&peer.parse()?, "/ip4/80.209.226.9/tcp/20201".parse()?);
        }
        // find myself
        if let Err(e) = 
            swarm
                .behaviour_mut()
                .kademlia
                .bootstrap() {
            eprintln!("bootstrap failed to initiate: `{:?}`", e);

        } else {
            println!("self-bootstrap is initiated.");
        }
    }

    // if let Err(e) = swarm.behaviour_mut().kademlia.bootstrap() {
    //     eprintln!("failed to initiate bootstrapping: {:#?}", e);
    // }

    // read full lines from stdin
    // let mut input = io::BufReader::new(io::stdin()).lines().fuse();

    // listen on all interfaces and whatever port the os assigns
    //@ should read from the config file
    swarm.listen_on("/ip4/0.0.0.0/udp/20202/quic-v1".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/tcp/20202".parse()?)?;
    swarm.listen_on("/ip6/::/tcp/20202".parse()?)?;
    swarm.listen_on("/ip6/::/udp/20202/quic-v1".parse()?)?;

    let mut timer_peer_discovery = stream::interval(Duration::from_secs(5 * 60)).fuse();


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

            // try to discover new peers
            () = timer_peer_discovery.select_next_some() => {
                if true == cli.dev {
                    continue;
                }
                let random_peer_id = PeerId::random();
                println!("Searching for the closest peers to `{random_peer_id}`");
                swarm
                    .behaviour_mut()
                    .kademlia
                    .get_closest_peers(random_peer_id);
            },

            // recalulate benchmarks
            () = timer_benchmark_exec.select_next_some() => {
                //@ to be removed: max 1 job limit
                // if false == benchmark_exec_futures.is_empty() {
                //     println!("A benchmark is already being executed...");
                //     continue;
                // }
                // let (bench_job_id, docker_image, bench_command, residue_path) = prepare_benchmark_job()?;
                // benchmark_exec_futures.push(
                //     run_docker_job(
                //         &docker_con,
                //         bench_job_id.clone(),
                //         docker_image.clone(),
                //         bench_command.clone(),
                //         residue_path.clone()
                //     )
                // );
                // benchmarks.insert(bench_job_id.clone(), benchmark::Benchmark {
                //     id: bench_job_id.clone(),
                //     cid: None,
                //     timestamp: None,
                // });
            },

            // mdns events
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Local node is listening on {address}");
                },

                SwarmEvent::IncomingConnection { .. } => {
                },

                SwarmEvent::IncomingConnectionError { .. } => {
                },

                SwarmEvent::OutgoingConnectionError { .. } => {
                }

                // mdns events
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discovered a new peer: {peer_id}");
                        swarm
                            .behaviour_mut()
                            .gossipsub
                            .add_explicit_peer(&peer_id);
                    }
                },

                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discovered peer has expired: {peer_id}");
                        swarm
                            .behaviour_mut()
                            .gossipsub
                            .remove_explicit_peer(&peer_id);
                    }
                },

                // identify events
                SwarmEvent::Behaviour(MyBehaviourEvent::Identify(identify::Event::Received {
                    peer_id,
                    info,
                    ..
                })) => {
                    println!("Inbound identify event `{:#?}`", info);
                    if false == cli.dev {
                        for addr in info.listen_addrs {
                            // if false == addr.iter().any(|item| item == &"127.0.0.1" || item == &"::1"){
                            swarm
                            .behaviour_mut()
                            .kademlia
                            .add_address(&peer_id, addr);
                            // }
                        }
                    }

                },

                // kademlia events
                // SwarmEvent::Behaviour(
                //     MyBehaviourEvent::Kademlia(
                //         kad::Event::OutboundQueryProgressed {
                //             result: kad::QueryResult::GetClosestPeers(Ok(ok)),
                //             ..
                //         }
                //     )
                // ) => {
                //     // The example is considered failed as there
                //     // should always be at least 1 reachable peer.
                //     if ok.peers.is_empty() {
                //         eprintln!("Query finished with no closest peers.");
                //     }

                //     println!("Query finished with closest peers: {:#?}", ok.peers);
                // },

                // SwarmEvent::Behaviour(MyBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                //     result:
                //         kad::QueryResult::GetClosestPeers(Err(kad::GetClosestPeersError::Timeout {
                //             ..
                //         })),
                //     ..
                // })) => {
                //     eprintln!("Query for closest peers timed out");
                // },

                // SwarmEvent::Behaviour(
                //     MyBehaviourEvent::Kademlia(
                //         kad::Event::OutboundQueryProgressed {
                //             result: kad::QueryResult::Bootstrap(Ok(ok)),
                //             ..
                //         }
                //     )
                // ) => {                    
                //     println!("bootstrap inbound: {:#?}", ok);
                // },

                // SwarmEvent::Behaviour(
                //     MyBehaviourEvent::Kademlia(
                //         kad::Event::OutboundQueryProgressed {
                //             result: kad::QueryResult::Bootstrap(Err(e)),
                //             ..
                //         }
                //     )
                // ) => {                    
                //     println!("bootstrap error: {:#?}", e);
                // },

                // SwarmEvent::Behaviour(
                //     MyBehaviourEvent::Kademlia(
                //         kad::Event::RoutingUpdated{
                //             peer,
                //             is_new_peer,
                //             addresses,
                //             ..
                //         }
                //     )
                // ) => {
                //     println!("Routing updated:\npeer: `{:?}`\nis new: `{:?}`\naddresses: `{:#?}`",
                //         peer, is_new_peer, addresses
                //     );
                // },

                // SwarmEvent::Behaviour(MyBehaviourEvent::Kademlia(kad::Event::UnroutablePeer{
                //     peer: peer_id
                // })) => {
                //     eprintln!("unroutable peer: {:?}", peer_id);
                // },

                // gossipsub events
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
                    let need = bincode::deserialize(&message.data).unwrap();
                    // let notice_req = notice::Notice::try_from(message.data[0])?;
                    match need {
                        notice::Notice::Compute(compute::NeedCompute {
                            criteria
                        }) => {
                            println!("`Need compute` request from client: `{peer_id}`");
                            // validate available benchmarks
                            let bench_is_valid = {
                                if let Some(mr_bench) = benchmarks.values().last() { 
                                    println!("Benchmark timestamp: {:?}", mr_bench.timestamp);
                                    let utc_now = Utc::now();
                                    let bench_timestamp = mr_bench.timestamp.unwrap_or_else(
                                        || DateTime::from_timestamp(0, 0).unwrap()
                                    );
                                    mr_bench.cid.is_some() &&
                                    utc_now.timestamp() - bench_timestamp.timestamp() < criteria.benchmark_expiry_secs.unwrap_or_else(|| 1_000_000i64) as i64
                                } else {
                                    println!("No benchmarks at all.");
                                    false
                                }
                            };

                            if false == bench_is_valid {
                                // re-run the benchmark 
                                println!("Benchmark is invalid, need to re-calculate it!");                                                                
                                // continue;
                            }

                            // advanced image pull requested
                            // if message.data.len() > 1 {
                            //     let docker_image = String::from_utf8_lossy(&message.data[1..]).to_string();
                            //     advanced_image_import_futures.push(
                            //         import_docker_image(
                            //             &docker_con,
                            //             docker_image.clone()
                            //         )
                            //     );
                            //     println!("Advanced docker image import request for `{docker_image}`");
                            // }

                            // engage with the client through a direct p2p channel
                            // and express interest in getting the compute job done
                            // let bench = benchmarks.values().last().unwrap();
                            //@ needed compute mathces with my skills?
                            let offer = compute::Offer {
                                compute_type: criteria.compute_type,
                                hw_specs: compute::ServerSpecs {
                                    gflops: 100,
                                    memory_capacity: 16,
                                    cpu_model: "core i7-5500u".to_string(),
                                },
                                price: 1,
                                server_benchmark: compute::ServerBenchmark{
                                    //@ clone?
                                    cid: String::from(""), //bench.cid.clone().unwrap(),
                                    pod_name: String::from("")//format!("benchmark_{}", bench.id.clone())
                                }
                            };
                            let sw_req_id = swarm
                                .behaviour_mut().req_resp
                                .send_request(
                                    &peer_id,
                                    notice::Request::ComputeOffer(offer),
                                );
                            println!("compute offer was sent to client, id: {sw_req_id}");
                        },

                        // status update inquiry
                        notice::Notice::StatusUpdate(_) => {
                            println!("`job-status` request from client: `{}`", peer_id);
                            let updates = job_status_of_peer(
                                &jobs,
                                peer_id
                            ); 
                            if updates.len() > 0 {
                                let _ = swarm
                                    .behaviour_mut().req_resp
                                    .send_request(
                                        &peer_id,
                                        notice::Request::UpdateForJobs(updates),
                                    );
                                // println!("jobs' status was sent to the client. req_id: `{_sw_resp_id}`");                            
                            }
                        },

                        // notice::Notice::Harvest(_) => {
                        //     let updates = harvest_jobs_of_peer(
                        //         &jobs,
                        //         peer_id
                        //     );                            
                        //     if updates.len() > 0 {
                        //         let _sw_req_id = swarm
                        //             .behaviour_mut().req_resp
                        //             .send_request(
                        //                 &peer_id,
                        //                 notice::Request::UpdateForJobs(updates),
                        //             );
                        //     }
                        // },

                        _ => (),
                    };
                },

                // process responses
                SwarmEvent::Behaviour(MyBehaviourEvent::ReqResp(request_response::Event::Message{
                    peer: peer_id,
                    message: request_response::Message::Request {
                        request,
                        channel,
                        ..
                    }
                })) => {                    
                    match request {
                        // notice::Response::DeclinedOffer => {
                        //     println!("Offer decliend by the client: `{peer_id}`");
                        // },                        
                        notice::Request::ComputeJob(compute_details) => {
                            println!("[info] New job from client: `{}`: `{:#?}`",
                                peer_id, compute_details
                            );
                            // no duplicate job_ids are allowed
                            if jobs.contains_key(&compute_details.job_id) {
                                println!("[warn] Duplicate compute job, ignored.");
                                continue;
                            }
                            prepare_job_reqs_futures.push(
                                prepare_job_reqs(
                                    &ds_client,
                                    compute_details,
                                    peer_id
                                )
                            );                            
                        },

                        // job status inquiry
                        // notice::Request::JobStatus(_to_be_updated) => {
                        //     // servers are lazy with job updates so clients need to query for their job's status every so often
                        //     println!("`job-status` request from client: `{}`", peer_id);
                        //     let updates = job_status_of_peer(
                        //         &jobs,
                        //         peer_id
                        //     ); 
                        //     if updates.len() > 0 {
                        //         let _ = swarm
                        //             .behaviour_mut().req_resp
                        //             .send_response(
                        //                 // &peer_id,
                        //                 channel,
                        //                 notice::Response::UpdateForJobs(updates),
                        //             );
                        //         // println!("jobs' status was sent to the client. req_id: `{_sw_resp_id}`");                            
                        //     }
                        // },

                        _ => (),
                    }
                },

                // responses
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

                        // // jobs that do not need benchmarks are wrapped inside responses
                        // notice::Response::ComputeJob(compute_details) => {
                        //     println!("received `compute job` request from client: `{}`, job: `{:#?}`",
                        //         peer_id, compute_details);                           
                        //     // no duplicate job_ids are allowed
                        //     if jobs.contains_key(&compute_details.job_id) {
                        //         println!("Duplicate compute job, ignored.");
                        //         continue;
                        //     }                            

                        //     let command = vec![
                        //         String::from("/bin/sh"),
                        //         String::from("-c"),
                        //         compute_details.command
                        //     ];
                        //     // create directory for residue
                        //     let residue_path = format!(
                        //         "{}/compute/{}/residue",
                        //         job::get_residue_path()?,
                        //         compute_details.job_id
                        //     );
                        //     std::fs::create_dir_all(residue_path.clone())?;

                        //     job_execution_futures.push(
                        //         run_docker_job(
                        //             &docker_con,
                        //             compute_details.job_id.clone(),
                        //             compute_details.docker_image.clone(),
                        //             command.clone(),
                        //             residue_path.clone()
                        //         )
                        //     );

                        //     // keep track of running jobs
                        //     jobs.insert(
                        //         compute_details.job_id.clone(),
                        //         job::Job {
                        //             id: compute_details.job_id.clone(),                                        
                        //             owner: peer_id,
                        //             status: job::Status::DockerWarmingUp,
                        //             residue: job::Residue {
                        //                 fd12_cid: None,
                        //                 receipt_cid: None,
                        //             },
                        //         },
                        //     );
                        // },

                        _ => (),
                    }
                },

                _ => {
                    // println!("{:#?}", event)
                },

            },

            // docker image import result is ready
            // image_import_result = advanced_image_import_futures.select_next_some() => {
            //     if let Err(err) = image_import_result {
            //         eprintln!("Advanced image import failed: `{err:#?}`");
            //         //@ what to do with err.who(image name)?
            //         continue;
            //     }
            //     let _image = image_import_result.unwrap();
            // },

            // benchmark execution is finished
            // job_exec_res = benchmark_exec_futures.select_next_some() => {
                // if let Err(failed) = job_exec_res {
                //     eprintln!("Failed to run the benchmark job: `{:#?}`", failed);       
                //     continue;
                // }
                // let result = job_exec_res.unwrap();
                // if false == benchmarks.contains_key(&result.job_id) {
                //     println!("Critical error: benchmark job `{}` is missing.", result.job_id);
                //     //@ what to do here?
                //     continue;
                // }
                // let bench = benchmarks.get_mut(&result.job_id).unwrap();
                // if result.exit_status_code != 0 { 
                //     // job.status = job::Status::ExecutionFailed;
                //     println!("Benchmark job `{}`'s execution finished with error: `{}`",
                //         result.job_id,
                //         result.error_message.unwrap_or_else(|| String::from("")),
                //     );
                //     continue;                 
                // } 
                // // job.status = job::Status::ExecutionSucceeded;
                // //@ beware of the time difference between now and the actual timestamp
                // bench.timestamp = Some(Utc::now());
                // println!("Benchmark execution was a success.");

                // a benchmark job has outputs a binary json blob to '~/residue/benchmark'
                
                // //@ if err, then program exits, must be handled properly
                // let residue_path = format!(
                //     "{}/compute/{}/residue",
                //     job::get_residue_path()?,
                //     result.job_id
                // );
                // // persist and share benchmark                              
                // benchmark_upload_futures.push(
                //     persist_benchmark(
                //         &ds_client,
                //         &ds_key,
                //         format!("{residue_path}/benchmark", ),
                //         result.job_id.clone(),
                //     )
                // );
            // },

            // job is ready to start execution
            prep_res = prepare_job_reqs_futures.select_next_some() => {
                if let Err(failed) = prep_res {
                    eprintln!("[warn] Failed to prepare job requirements: {:#?}", failed);
                    //@ wtd here?
                    continue;
                }
                let prepared_job = prep_res.unwrap();
                // keep track of running jobs
                jobs.insert(
                    prepared_job.compute_details.job_id.clone(),
                    job::Job {
                        id: prepared_job.compute_details.job_id.clone(),                                        
                        owner: prepared_job.owner,
                        status: job::Status::Running,
                        residue: job::Residue {
                            receipt_cid: None,
                        },
                        compute_type: prepared_job.compute_details.compute_type.clone()
                    },
                );
                // run it
                match prepared_job.compute_details.compute_type {
                    compute::ComputeType::ProveAndLift => {
                        prove_execution_futures.push(
                            recursion::prove_and_lift(
                                prepared_job.compute_details.job_id.clone(),
                                prepared_job.inputs[0].clone().into()
                            )
                        );
                    },

                    compute::ComputeType::Join => {
                        join_execution_futures.push(
                            recursion::join(
                                prepared_job.compute_details.job_id.clone(),
                                prepared_job.inputs[0].clone().into(),
                                prepared_job.inputs[1].clone().into()
                            )
                        );
                    },

                    compute::ComputeType::Snark => {
                        snark_execution_futures.push(
                            recursion::stark_to_snark(
                                prepared_job.compute_details.job_id.clone(),
                                prepared_job.inputs[0].clone().into()
                            )
                        );
                    },
                };                
            },

            // prove is finished
            er = prove_execution_futures.select_next_some() => {                
                if let Err(failed) = er {
                    eprintln!("[warn] Failed to run the job: `{:#?}`", failed);       
                    //@ what to to with job id?
                    let job = jobs.get_mut(&failed.job_id).unwrap();
                    job.status = job::Status::ExecutionFailed(failed.err_msg);
                    continue;
                }
                let res = er.unwrap();             
                println!("[info] Prove finished for `{}`", res.job_id);   
                let job = jobs.get_mut(&res.job_id).unwrap();
                let residue_path = format!(
                    "{}/.wholesum/jobs/prover/prove/{}-upload",
                    get_home_dir()?,
                    &job.id
                );
                fs::create_dir_all(residue_path.clone())?;
                let _ = post_job_execution(
                    &ds_client,
                    &ds_key,
                    job,
                    res.blob,
                    &residue_path
                ).await?;
            },

            // join is finished
            er = join_execution_futures.select_next_some() => {
                if let Err(failed) = er {
                    eprintln!("[warn] Failed to run the job: `{:#?}`", failed);       
                    let job = jobs.get_mut(&failed.job_id).unwrap();
                    job.status = job::Status::ExecutionFailed(failed.err_msg);
                    continue;
                }
                let res = er.unwrap();     
                println!("[info] Join finished for `{}`", res.job_id);              
                let job = jobs.get_mut(&res.job_id).unwrap();
                let residue_path = format!(
                    "{}/.wholesum/jobs/prover/join/{}-upload",
                    get_home_dir()?,
                    &job.id
                );
                fs::create_dir_all(residue_path.clone())?;
                let _ = post_job_execution(
                    &ds_client,
                    &ds_key,
                    job,
                    res.blob,
                    &residue_path
                ).await?;                                
            },

            // stark_to_snark is finished
            er = snark_execution_futures.select_next_some() => {
                if let Err(failed) = er {
                    eprintln!("[warn] Failed to run the job: `{:#?}`", failed);       
                    //@ what to to with job id?
                    // let _job_id = failed.who;
                    // job.status = job::Status::ExecutionFailed;
                    //     eprintln!("[warn] Job `{}`'s execution finished with error: `{}`",
                    //         exec_res.job_id,
                    //         exec_res.error_message.unwrap_or_else(|| String::from("")),
                    //     );


                    continue;
                }
                let res = er.unwrap();                
                let job = jobs.get_mut(&res.job_id).unwrap();
                let _ = post_job_execution(&ds_client, &ds_key, job, res.blob, "").await?;                
            },
            
            // handle benchmark has been uploaded to dStorage
            // bench_upload_result = benchmark_upload_futures.select_next_some() => {
            //     let upload_result: ResidueUploadResult = match bench_upload_result {
            //         Ok(ur) => ur,
            //         Err(e) => {
            //             println!("Missing cid for the benchmark pod: `{e:?}`");
            //             //@ what to do here
            //             continue
            //         }
            //     };
            //     println!("Benchmakr pod is now public: {:#?}", upload_result);
            //     if false == benchmarks.contains_key(&upload_result.job_id) {
            //         println!("Residue pod's benchmark job data is missing.");
            //         //@ what to do here?
            //         continue;
            //     }
            //     let bench = benchmarks.get_mut(&upload_result.job_id).unwrap();
            //     bench.cid = Some(upload_result.cid); 
            // },
        }
    }
}

pub fn get_home_dir() -> anyhow::Result<String> {
    let err_msg = "Home dir is not available";
    let binding = home::home_dir()
        .ok_or_else(|| anyhow::Error::msg(err_msg))?;
    let home_dir = binding.to_str()
        .ok_or_else(|| anyhow::Error::msg(err_msg))?;
    Ok(home_dir.to_string())
}

// prepare benchmark job details
fn _prepare_benchmark_job()
 -> Result<(String, String, Vec<String>, String), Box<dyn Error>> {

    let bench_job_id = Uuid::new_v4().simple().to_string()[..7].to_string();
    let docker_image = String::from("rezahsnz/ntt512-benchmark:0.21");
    let bench_command = vec![
        String::from("/bin/sh"),
        String::from("-c"),
        String::from("/home/prince/ntt512")
    ];
    // create directory for residue
    let residue_path = format!(
        "{}/compute/{}/residue",
        get_home_dir()?,
        bench_job_id
    );
    std::fs::create_dir_all(residue_path.clone())?;
    Ok(
        (bench_job_id, docker_image, bench_command, residue_path)
    )
}

#[derive(Debug, Clone)]
struct PreparedJob {
    pub compute_details: compute::ComputeDetails,
    owner: PeerId,
    inputs: Vec<String>,
}

// set job up for execution
async fn prepare_job_reqs(
    ds_client: &reqwest::Client,
    compute_details: compute::ComputeDetails,
    owner: PeerId,
) -> anyhow::Result<PreparedJob> {
    // download blob from dstorage and store it on docker volume of the job
    let base_location = format!(
        "{}/.wholesum/jobs/prover",
        get_home_dir()?
    );
    let blob_file_paths = match &compute_details.input_type {        
        compute::ComputeJobInputType::Prove(cid) => {
            let action_dir = format!("{base_location}/prove");
            fs::create_dir_all(action_dir.clone())?;
            let blob_file_path = format!("{action_dir}/{}", &compute_details.job_id);
            lighthouse::download_file(
                ds_client,
                &cid,
                blob_file_path.clone()
            ).await?;
            vec![blob_file_path]
        },

        compute::ComputeJobInputType::Join(left_cid, right_cid) => {
            let action_dir = format!("{base_location}/join");
            fs::create_dir_all(action_dir.clone())?;
            // left cid
            let left_blob_file_path = format!("{action_dir}/{}-left", &compute_details.job_id);
            lighthouse::download_file(
                ds_client,
                &left_cid,
                left_blob_file_path.clone()
            ).await?;
            // right cid
            let right_blob_file_path = format!("{action_dir}/{}-right", &compute_details.job_id);
            lighthouse::download_file(
                ds_client,
                &right_cid,
                right_blob_file_path.clone()
            ).await?;
            vec![left_blob_file_path, right_blob_file_path]
        }
    };
    
    Ok(PreparedJob{
        compute_details: compute_details,
        owner: owner,
        inputs: blob_file_paths
    })
}

async fn post_job_execution(
    ds_client: &reqwest::Client,
    ds_key: &str,
    job: &mut job::Job,
    blob: Vec<u8>,
    residue_path: &str,
) -> anyhow::Result<()> {
    job.status = job::Status::ExecutionSucceeded;
    println!("[info] Execution was a success, now upload the receipt to dstorage.");    
    //@ stream the blob instead of writing and reading    
    let out_file = format!("{residue_path}/out_receipt");
    let _bytes_written = std::fs::write(
        &out_file,
        blob
    );
    // upload the receipt
    let upload_res = lighthouse::upload_file(
        &ds_client,
        &ds_key,
        out_file,
        format!("receipt-{}", &job.id)
    ).await;
    if let Err(failed) = upload_res {
        eprintln!("[warn] Receipt upload for `{}` failed: `{:#?}`",
            &job.id, failed
        );
        //@ remember to retry for successfully finished jobs
        return Err(failed);
    }
    let cid = upload_res.unwrap().cid;
    job.residue.receipt_cid = Some(cid);
    Ok(())
}

// retrieve all status of jobs owned by the peer_id
fn job_status_of_peer(
    jobs: &HashMap::<String, job::Job>,
    peer_id: PeerId
) -> Vec<compute::JobUpdate> {
    let mut updates = Vec::<compute::JobUpdate>::new();
    for job in jobs.values() {
        if job.owner != peer_id {
            continue;
        }
        let status = match &job.status {
            job::Status::ExecutionSucceeded => {
                if true == job.residue.receipt_cid.is_some() {
                    compute::JobStatus::ExecutionSucceeded(
                        job.residue.receipt_cid.clone().unwrap()
                    )
                } else {
                    compute::JobStatus::Running
                }
            },

            job::Status::ExecutionFailed(err) => 
                compute::JobStatus::ExecutionFailed(Some(err.clone())),
            
            // all the rest are trivial status
            _ => compute::JobStatus::Running,
        };
        updates.push(compute::JobUpdate {
            id: job.id.clone(),
            status: status,
            compute_type: job.compute_type.clone(),
        });
    }
    updates
}

// retrieve all harvest jobs of the peer
// fn harvest_jobs_of_peer(
//     jobs: &HashMap::<String, job::Job>,
//     peer_id: PeerId
// ) -> Vec<compute::JobUpdate> {
//     let mut updates = Vec::<compute::JobUpdate>::new();
//     let iter = jobs.values().filter(
//         |&j| 
//         j.owner == peer_id &&
//         j.status == job::Status::ExecutionSucceeded
//     );
//     for job in iter {      
//         if true == job.residue.fd12_cid.is_none() {
//             println!("Warning: missing fd12 cid for the job `{}` that is being harvested.",
//                 job.id);
//         }  
//         updates.push(compute::JobUpdate {
//             id: job.id.clone(),
//             status: compute::JobStatus::Harvested(
//                 compute::HarvestDetails {
//                     fd12_cid: job.residue.fd12_cid.clone(),
//                     receipt_cid: job.residue.receipt_cid.clone(),
//                 }
//             ),
//             compute_type: job.compute_type,
//         });
//     }
//     updates
// }


// upload stdout(fd 1) and stderr(fd 2) to a dStorage
async fn _persist_fd12(
    ds_client: &reqwest::Client,
    ds_key: &str,
    local_base_path: String,
    job_id: String,
) -> Result<ResidueUploadResult, Box<dyn Error>> {
    //@ combine stdout and stderr into one and upload once
    // upload stdout
    let upload_res = lighthouse::upload_file(
        ds_client, 
        ds_key,
        format!("{local_base_path}/stdout"),
        format!("stdout-{job_id}")
    ).await?; 
    // upload stderr
    let _ = lighthouse::upload_file(
        ds_client, 
        ds_key,
        format!("{local_base_path}/stderr"),
        format!("stderr-{job_id}")
    ).await?;

    Ok(
        ResidueUploadResult {
            job_id: job_id,
            cid: upload_res.cid            
        }
    )
}

async fn _persist_receipt(
    ds_client: &reqwest::Client,
    ds_key: &str,
    local_receipt_path: String,
    job_id: String,
) -> Result<ResidueUploadResult, Box<dyn Error>>  {
    // upload receipt
    let upload_res = lighthouse::upload_file(
        ds_client,
        ds_key,
        local_receipt_path,
        format!("receipt-{job_id}")
    ).await?;
      
    Ok(
        ResidueUploadResult {
            job_id: job_id,
            cid: upload_res.cid
        }
    )
}

async fn _persist_benchmark(
    ds_client: &reqwest::Client,
    ds_key: &str,
    local_benchmark_path: String,
    job_id: String,
) -> anyhow::Result<ResidueUploadResult>  {
    
    // upload receipt
    let upload_res = lighthouse::upload_file(
        ds_client,
        ds_key,
        local_benchmark_path, 
        format!("benchmark-{job_id}")
    ).await?;
    
    Ok(
        ResidueUploadResult {
            job_id: job_id,
            cid: upload_res.cid
        }
    )
}

