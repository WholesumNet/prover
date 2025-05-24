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
    error::Error,
    time::Duration,
    collections::{
        HashMap 
    },
    future::IntoFuture,
};

use env_logger::Env;
use log::{info, warn};
// use chrono::{DateTime, Utc};

use clap::Parser;
// use reqwest;
use xxhash_rust::xxh3;

use libp2p::{
    gossipsub, mdns, request_response,
    identity, identify,  
    swarm::{SwarmEvent},
    PeerId,
};
use anyhow;
use mongodb::{
    bson::{
        doc,
    },
    options::{
        ClientOptions,
        ServerApi,
        ServerApiVersion
    },
};

use comms::{
    p2p::{
        MyBehaviourEvent
    },
    protocol,
    protocol::{
        Need,
        ProofToken
    },
};

mod job;
mod recursion;
mod db;

// CLI
#[derive(Parser, Debug)]
#[command(name = "Prover CLI for Wholesum")]
#[command(author = "Wholesum team")]
#[command(version = "1.0")]
#[command(about = "Wholesum is a p2p prover network and \
                   this program is a CLI for prover nodes.",
          long_about = None)
]
struct Cli {
    #[arg(long, action)]
    dev: bool,

    #[arg(short, long)]
    key_file: Option<String>,
}

#[async_std::main]
async fn main() -> Result<(), Box<dyn Error + 'static>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .init();
    let cli = Cli::parse();
    info!("<-> `Prover` agent for Wholesum network <->");
    info!("Operating mode: `{}` network",
        if false == cli.dev { "global" } else { "local(development)" }
    ); 

    // setup mongodb
    let db_client = mongodb_setup("mongodb://localhost:27017").await?;

    // let's maintain a list of jobs
    let mut jobs = HashMap::<String, job::Job>::new();
    
    // pull jobs' execution
    let mut segment_prove_futures = FuturesUnordered::new();
    let mut join_prove_futures = FuturesUnordered::new();
    let mut groth16_prove_futures = FuturesUnordered::new();

    let col_proofs = db_client
        .database("wholesum_prover")
        .collection::<db::Proof>("proofs");
    
    // futures for mongodb progress saving 
    let mut db_insert_futures = FuturesUnordered::new();
    
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
            warn!("No keys were supplied, so one has been generated for you and saved to `{}` file.", "./key.secret");
            new_key
        }
    };    
    info!("my peer id: `{:?}`", PeerId::from_public_key(&local_key.public()));    

    // Libp2p swarm 
    let mut swarm = comms::p2p::setup_swarm(&local_key).await?;
    let topic = gossipsub::IdentTopic::new("<-- Wholesum p2p prover bazaar -->");
    let _ = 
        swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&topic);

    // bootstrap 
    if false == cli.dev {
        // get to know bootnodes
        const BOOTNODES: [&str; 1] = [
            "TBD",
        ];
        for peer in &BOOTNODES {
            swarm.behaviour_mut()
                .kademlia
                .add_address(&peer.parse()?, "/ip4/W.X.Y.Z/tcp/20201".parse()?);
        }
        // find myself
        if let Err(e) = 
            swarm
                .behaviour_mut()
                .kademlia
                .bootstrap() {
            warn!("Failed to bootstrap Kademlia: `{:?}`", e);

        } else {
            info!("Self-bootstraping is initiated.");
        }
    }

    // if let Err(e) = swarm.behaviour_mut().kademlia.bootstrap() {
    //     eprintln!("failed to initiate bootstrapping: {:#?}", e);
    // }

    // listen on all interfaces and whatever port the os assigns
    //@ should read from the config file
    swarm.listen_on("/ip4/0.0.0.0/udp/20202/quic-v1".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/tcp/20202".parse()?)?;
    swarm.listen_on("/ip6/::/tcp/20202".parse()?)?;
    swarm.listen_on("/ip6/::/udp/20202/quic-v1".parse()?)?;

    let mut timer_peer_discovery = stream::interval(Duration::from_secs(5 * 60)).fuse();
    loop {
        select! {
            // try to discover new peers
            () = timer_peer_discovery.select_next_some() => {
                if true == cli.dev {
                    continue;
                }
                let random_peer_id = PeerId::random();
                info!("Searching for the closest peers to `{random_peer_id}`");
                swarm
                    .behaviour_mut()
                    .kademlia
                    .get_closest_peers(random_peer_id);
            },

            // libp2p events
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    info!("Local node is listening on {address}");
                },

                // mdns events
                SwarmEvent::Behaviour(
                    MyBehaviourEvent::Mdns(
                        mdns::Event::Discovered(list)
                    )
                ) => {
                    for (peer_id, _multiaddr) in list {
                        info!("mDNS discovered a new peer: {peer_id}");
                        swarm
                            .behaviour_mut()
                            .gossipsub
                            .add_explicit_peer(&peer_id);
                    }
                },

                SwarmEvent::Behaviour(
                    MyBehaviourEvent::Mdns(
                        mdns::Event::Expired(list)
                    )
                ) => {
                    for (peer_id, _multiaddr) in list {
                        info!("mDNS discovered peer has expired: {peer_id}");
                        swarm
                            .behaviour_mut()
                            .gossipsub
                            .remove_explicit_peer(&peer_id);
                    }
                },

                // identify events
                SwarmEvent::Behaviour(
                    MyBehaviourEvent::Identify(
                        identify::Event::Received {
                            peer_id,
                            info,
                            ..
                        }
                    )
                ) => {
                    // info!("Inbound identify event `{:#?}`", info);
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
            
                SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source: peer_id,
                    message,
                    ..
                })) => {
                    let need: Need = match bincode::deserialize(&message.data) {
                        Err(e) => {
                            warn!("Gossip(need) message decode error: `{e:?}`");
                            continue;
                        },

                        Ok(n) => n,
                    };
                    match need {
                        Need::Compute(_nonce, _need_type) => {
                            info!("Heard `{_need_type:#?}` need...");
                            let _req_id = swarm
                                .behaviour_mut().req_resp
                                .send_request(
                                    &peer_id,
                                    protocol::Request::WouldCompute,
                                );
                        },

                        // status update inquiry
                        Need::UpdateMe(req_job_id) => {
                            let mut proofs = Vec::new();
                            for job in jobs.values().filter(|j| j.base_id == req_job_id) {
                                if job.owner != peer_id {
                                    continue;
                                }                                
                                let proof_type = match job.job_type {
                                    job::JobType::Segment(segment_id) => 
                                        protocol::ProofType::Segment(segment_id),
                                    
                                    job::JobType::Join(pair_id) =>
                                        protocol::ProofType::Join(pair_id),

                                    job::JobType::Groth16 => 
                                        protocol::ProofType::Groth16(job.proof.clone().unwrap().blob),
                                };
                                proofs.push(ProofToken {
                                    job_id: job.base_id.clone(),
                                    proof_type: proof_type,
                                    blob_hash: job.proof.as_ref().unwrap().hash
                                });                                
                            }                            
                            if proofs.len() > 0 {
                                let _req_id = swarm
                                    .behaviour_mut().req_resp
                                    .send_request(
                                        &peer_id,
                                        protocol::Request::ProofsAreReady(proofs),
                                    );
                            }
                        },
                    };
                },

                SwarmEvent::Behaviour(MyBehaviourEvent::ReqResp(request_response::Event::Message {
                    peer: client_peer_id,
                    message: request_response::Message::Response {
                        response,
                        //response_id,
                        ..
                    }
                })) => {                
                    match response {
                        protocol::Response::Job(compute_job) => {                            
                            match compute_job.job_type {
                                protocol::JobType::Segment(segment_details) => {
                                    let prove_id = format!(
                                        "{}-{}",
                                        compute_job.job_id,
                                        segment_details.id
                                    );
                                    // keep track of running jobs                
                                    jobs.insert(                    
                                        prove_id.clone(),
                                        job::Job {
                                            base_id: compute_job.job_id,
                                            id: prove_id.clone(),
                                            owner: client_peer_id,
                                            status: job::Status::Running,
                                            job_type: job::JobType::Segment(segment_details.id),
                                            input_blobs: vec![segment_details.blob.clone()],
                                            proof: None,
                                        },
                                    );
                                    // run it
                                    //@ use reference of blobs instead
                                    segment_prove_futures.push(
                                        recursion::prove_segment(
                                            prove_id,
                                            segment_details.id,
                                            segment_details.po2,
                                            segment_details.blob.clone() 
                                        )
                                    );
                                },

                                protocol::JobType::Join(join_details) => {
                                    let join_id = format!(
                                        "{}-{}",
                                        compute_job.job_id,
                                        join_details.pair_id                                        
                                    );
                                    // keep track of running jobs                
                                    jobs.insert(
                                        join_id.clone(),
                                        job::Job {
                                            base_id: compute_job.job_id,
                                            id: join_id.clone(),
                                            owner: client_peer_id,
                                            status: job::Status::Running,
                                            job_type: job::JobType::Join(join_details.pair_id),
                                            input_blobs: vec![
                                                join_details.blob_pair.0.clone(),
                                                join_details.blob_pair.1.clone()
                                            ],
                                            proof: None,
                                        }
                                    );
                                    // run it
                                    //@ use reference of blobs instead
                                    join_prove_futures.push(
                                        recursion::join(
                                            join_id.clone(),
                                            join_details.blob_pair.0.clone(),
                                            join_details.blob_pair.1.clone(),
                                        )
                                    );
                                },

                                protocol::JobType::Groth16(groth16_details) => {
                                    let groth16_id = format!("{}-g16",compute_job.job_id);
                                    // keep track of running jobs                
                                    jobs.insert(                    
                                        groth16_id.clone(),
                                        job::Job {
                                            base_id: compute_job.job_id.clone(),
                                            id: groth16_id.clone(),
                                            owner: client_peer_id,
                                            status: job::Status::Running,
                                            job_type: job::JobType::Groth16,
                                            input_blobs: vec![groth16_details.blob.clone()],
                                            proof: None,
                                        },
                                    );
                                    // run it
                                    //@ use reference of blobs instead
                                    groth16_prove_futures.push(
                                        recursion::to_groth16(
                                            groth16_id,
                                            groth16_details.blob.clone(),
                                        )
                                    );
                                },
                            }
                        }
                    }
                },

                _ => {
                    // println!("{:#?}", event)
                },
            },                        

            // segment is proved
            er = segment_prove_futures.select_next_some() => {
                if let Err(failed) = er {
                    warn!("Failed to run the prove job: `{:#?}`", failed);       
                    //@ wtd with job?
                    let job = jobs.get_mut(&failed.job_id).unwrap();
                    job.status = job::Status::ExecutionFailed(failed.err_msg);
                    continue;
                }
                let res = er.unwrap();
                let job = jobs.get_mut(&res.job_id).unwrap();
                job.status = job::Status::ExecutionSucceded;
                info!("`prove segment` is finished for `{}`", res.job_id);
                let segment_id = if let job::JobType::Join(segment_id) = job.job_type { 
                    segment_id
                } else {
                    warn!("No segment-id is available for `{:?}`.", job.job_type);
                    u32::MAX
                };
                // record to db                
                db_insert_futures.push(
                    col_proofs.insert_one(
                        db::Proof {
                            client_job_id: job.base_id.clone(),
                            job_id: job.id.clone(),
                            job_type: db::JobType::Segment(segment_id),
                            owner: job.owner.to_string(),
                            input_blobs: job.input_blobs.clone(),
                            blob: res.blob.clone(),                    
                        }
                    )
                    .into_future()
                );

                let blob_hash = xxh3::xxh3_64(&res.blob);
                job.proof = Some(
                    job::Proof {
                        hash: blob_hash,
                        blob: res.blob
                    }
                );

                let _ = swarm
                    .behaviour_mut()
                    .req_resp
                    .send_request(
                        &job.owner,
                        protocol::Request::ProofsAreReady(vec![
                            protocol::ProofToken {
                                job_id: job.base_id.clone(),
                                proof_type: protocol::ProofType::Segment(segment_id),
                                blob_hash: blob_hash
                            }
                        ])
                    ); 
            },

            // join job is finished
            er = join_prove_futures.select_next_some() => {
                if let Err(failed) = er {
                    warn!("Failed to run the join job: `{:#?}`", failed);       
                    let job = jobs.get_mut(&failed.job_id).unwrap();
                    job.status = job::Status::ExecutionFailed(failed.err_msg);
                    continue;
                }
                let res = er.unwrap();
                let job = jobs.get_mut(&res.job_id).unwrap();
                job.status = job::Status::ExecutionSucceded;
                info!("`join` is finished for `{}`", res.job_id);
                let pair_id = if let job::JobType::Join(pair_id) = job.job_type { 
                    pair_id
                } else {
                    warn!("No pair-id is available for `{:?}`.", job.job_type);
                    u32::MAX
                };
                // record to db                
                db_insert_futures.push(
                    col_proofs.insert_one(
                        db::Proof {
                            client_job_id: job.base_id.clone(),
                            job_id: job.id.clone(),
                            job_type: db::JobType::Join(pair_id),
                            owner: job.owner.to_string(),
                            input_blobs: job.input_blobs.clone(),
                            blob: res.blob.clone(),                    
                        }
                    )
                    .into_future()
                );

                let blob_hash = xxh3::xxh3_64(&res.blob);
                job.proof = Some(
                    job::Proof {
                        hash: blob_hash,
                        blob: res.blob.clone() 
                    }
                );

                let _ = swarm
                    .behaviour_mut()
                    .req_resp
                    .send_request(
                        &job.owner,
                        protocol::Request::ProofsAreReady(vec![
                            protocol::ProofToken {
                                job_id: job.base_id.clone(),
                                proof_type: protocol::ProofType::Join(pair_id),
                                blob_hash: blob_hash
                            }
                        ])
                    );  
            },            

            // groth16 job is finished
            er = groth16_prove_futures.select_next_some() => {                
                if let Err(failed) = er {
                    warn!("Failed to run the groth16 job: `{:#?}`", failed);       
                    //@ wtd with job?
                    let job = jobs.get_mut(&failed.job_id).unwrap();
                    job.status = job::Status::ExecutionFailed(failed.err_msg);
                    continue;
                }
                let res = er.unwrap();
                let job = jobs.get_mut(&res.job_id).unwrap();
                job.status = job::Status::ExecutionSucceded;
                info!("`groth16` extraction is finished for `{}`.", res.job_id);
                // record to db                
                db_insert_futures.push(
                    col_proofs.insert_one(
                        db::Proof {
                            client_job_id: job.base_id.clone(),
                            job_id: job.id.clone(),
                            job_type: db::JobType::Groth16,
                            owner: job.owner.to_string(),
                            input_blobs: job.input_blobs.clone(),
                            blob: res.blob.clone(),                    
                        }
                    )
                    .into_future()
                );

                let blob_hash = xxh3::xxh3_64(&res.blob);
                job.proof = Some(
                    job::Proof {
                        hash: blob_hash,
                        blob: res.blob.clone() 
                    }
                );

                let _ = swarm
                    .behaviour_mut()
                    .req_resp
                    .send_request(
                        &job.owner,
                        protocol::Request::ProofsAreReady(vec![
                            protocol::ProofToken {
                                job_id: job.base_id.clone(),
                                proof_type: protocol::ProofType::Groth16(res.blob),
                                blob_hash: blob_hash,
                            }
                        ])
                    );                
            },            

            res = db_insert_futures.select_next_some() => {
                match res {
                    Ok(oid) => {
                        info!("DB insert was successful: `{oid:?}`");
                    },

                    Err(err_msg) => {
                        warn!("DB insert was failed`{err_msg:?}`");
                    }
                }                
            },
        }
    }
}

// fn get_home_dir() -> anyhow::Result<String> {
//     let err_msg = "Home dir is not available";
//     let binding = home::home_dir()
//         .ok_or_else(|| anyhow::Error::msg(err_msg))?;
//     let home_dir = binding.to_str()
//         .ok_or_else(|| anyhow::Error::msg(err_msg))?;
//     Ok(home_dir.to_string())
// }

async fn mongodb_setup(
    uri: &str,
) -> anyhow::Result<mongodb::Client> {
    info!("Connecting to the MongoDB daemon...");
    let mut client_options = ClientOptions::parse(
        uri
    ).await?;
    let server_api = ServerApi::builder().version(
        ServerApiVersion::V1
    ).build();
    client_options.server_api = Some(server_api);
    let client = mongodb::Client::with_options(client_options)?;
    // Send a ping to confirm a successful connection
    client
        .database("admin")
        .run_command(doc! { "ping": 1 })
        .await?;
    info!("Successfully connected to the MongoDB instance!");
    Ok(client)
}

// #[derive(Debug, Clone)]
// struct DBInsertResult {
//     job_id: String,

//     inserted_id: Bson,    
// }

// #[derive(Debug, Clone)]
// struct DBInsertError {
//     job_id: String,

//     err_msg: String,    
// }

// async fn insert_into_db(
//     col_proofs: &mongodb::Collection<db::Proof>,
//     db_proof: db::Proof
// ) -> anyhow::Result<DBInsertResult, DBInsertError> {
//     let job_id = db_proof.job_id.clone();
//     col_proofs
//     .insert_one(db_proof)
//     .await
//     .and_then(|insert_result| 
//         Ok(
//             DBInsertResult {
//                 job_id: job_id.clone(),
//                 inserted_id: insert_result.inserted_id
//             }
//         )
//     )
//     .map_err(|e|
//         DBInsertError {        
//             job_id: job_id,
//             err_msg: e.to_string()
//         }
//     )
// }