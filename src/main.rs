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
        HashMap 
    },
    future::IntoFuture,
};

// use chrono::{DateTime, Utc};

use clap::Parser;
use reqwest;

use libp2p::{
    gossipsub, mdns,
    identity, identify,  
    swarm::{SwarmEvent},
    PeerId,
};

use tracing_subscriber::EnvFilter;

// use uuid::Uuid;
use anyhow;
use bit_vec::BitVec;
use rand::seq::SliceRandom;
use rand::thread_rng;
use base64::{Engine as _, engine::{self, general_purpose}, alphabet};
use mongodb::{
    bson::{
        Bson,
        doc,
    },
    options::{
        ClientOptions,
        ServerApi,
        ServerApiVersion
    },
};

use comms::{
    p2p::{ MyBehaviourEvent },
    protocol,
    protocol::{ Need, Proof, JobType },
};
use dstorage::lighthouse;

mod job;
mod recursion;
mod db;

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

    // setup mongodb
    let db_client = mongodb_setup("mongodb://localhost:27017").await?;

    // let's maintain a list of jobs
    let mut jobs = HashMap::<String, job::Job>::new();
    // cids' download futures
    let mut prepare_prove_job_futures = FuturesUnordered::new();
    let mut prepare_join_job_futures = FuturesUnordered::new();
    let mut prepare_groth16_job_futures = FuturesUnordered::new();

    // pull jobs' execution
    let mut prove_execution_futures = FuturesUnordered::new();
    let mut join_execution_futures = FuturesUnordered::new();
    let mut groth16_execution_futures = FuturesUnordered::new();

    let mut proof_upload_futures = FuturesUnordered::new();

    let col_proofs = db_client
        .database("wholesum_prover")
        .collection::<db::Proof>("proofs");
    
    // futures for mongodb progress saving 
    let mut db_insert_futures = FuturesUnordered::new();
    let mut db_update_futures = FuturesUnordered::new();
    
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
            eprintln!("[warn] Failed to bootstrap Kademlia: `{:?}`", e);

        } else {
            println!("[info] Self-bootstraping is initiated.");
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
    let mut timer_retry_proof_upload = stream::interval(Duration::from_secs(1 * 60)).fuse();
    let mut rng = thread_rng();
    loop {
        select! {
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

            // libp2p events
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("[info] Local node is listening on {address}");
                },

                // mdns events
                SwarmEvent::Behaviour(
                    MyBehaviourEvent::Mdns(
                        mdns::Event::Discovered(list)
                    )
                ) => {
                    for (peer_id, _multiaddr) in list {
                        println!("[info] mDNS discovered a new peer: {peer_id}");
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
                        println!("[info] mDNS discovered peer has expired: {peer_id}");
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
                    // println!("[info] Inbound identify event `{:#?}`", info);
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
                            eprintln!("[warn] Gossip(need) message decode error: `{e:?}`");
                            continue;
                        },

                        Ok(n) => n,
                    };
                    match need {
                        Need::Compute(compute_job) => {
                            match compute_job.job_type {
                                JobType::ProveAndLift(prove_details) => {
                                    println!("[info] New prove job from `{peer_id}`: `{prove_details:#?}`");
                                    let mut progress_map = BitVec::from_bytes(&prove_details.progress_map);
                                    progress_map.truncate(prove_details.num_segments.try_into().unwrap());
                                    let mut unproved_segments = vec![];
                                    for (segment_id, is_proved) in progress_map.iter().enumerate() {
                                        if true == is_proved {
                                            continue;
                                        }
                                        //@ slow af
                                        let prove_id = format!("{}-{}", compute_job.job_id, segment_id);
                                        if let Some(job) = jobs.get(&prove_id) {
                                            if let job::Status::ExecutionFailed(_) = job.status {
                                                unproved_segments.push(segment_id);
                                            }
                                        } else {
                                            unproved_segments.push(segment_id);
                                        }                                                                                   
                                    }
                                    if unproved_segments.len() == 0 {
                                        println!("[warn] No more segments to prove.");
                                        continue;
                                    }
                                    let chosen_segment = *unproved_segments.choose(&mut rng).unwrap() as u32;
                                    println!("[info] Picked `segment {chosen_segment}` to prove.");
                                    prepare_prove_job_futures.push(
                                        prepare_prove_job(
                                            &ds_client,
                                            compute_job.job_id.clone(),                                                
                                            chosen_segment,
                                            format!(
                                                "{}/{}{}",
                                                prove_details.segments_base_cid,
                                                prove_details.segment_prefix_str,
                                                chosen_segment
                                            ),
                                            peer_id
                                        )
                                    );
                                },
    
                                JobType::Join(join_details) => {
                                    println!("[info] New join job from `{peer_id}`: `{join_details:#?}`");
                                    let mut progress_map = BitVec::from_bytes(&join_details.progress_map);
                                    progress_map.truncate(join_details.pairs.len());
                                    let mut unproved_pairs = vec![];
                                    for (index, is_proved) in progress_map.iter().enumerate() {
                                        let pair = &join_details.pairs[index];
                                        // if false == is_proved &&
                                        //    false == jobs.contains_key(
                                        //        &format!("{}-{}-{}", compute_job.job_id, pair.0, pair.1)
                                        //    ) //@ slow af, optimize it
                                        // {
                                        //     unproved_pairs.push(pair);
                                        // }
                                        if true == is_proved {
                                            continue;
                                        }
                                        //@ slow af
                                        let join_id = format!("{}-{}-{}", compute_job.job_id, pair.0, pair.1);
                                        if let Some(job) = jobs.get(&join_id) {
                                            if let job::Status::ExecutionFailed(_) = job.status {
                                                unproved_pairs.push(pair);
                                            }
                                        } else {
                                            unproved_pairs.push(pair);
                                        } 
                                    }
                                    if unproved_pairs.len() == 0 {
                                        println!("[warn] No unproved pairs to join.");
                                        continue;
                                    }
                                    let choosen_pair = *unproved_pairs.choose(&mut rng).unwrap();
                                    println!("[info] Picked `pair {choosen_pair:?}` to join.");
                                    prepare_join_job_futures.push(
                                        prepare_join_job(
                                            &ds_client,
                                            compute_job.job_id.clone(),                                                
                                            choosen_pair.0.clone(),
                                            choosen_pair.1.clone(),                                            
                                            peer_id
                                        )
                                    );
                                },

                                JobType::Groth16(groth16_details) => {
                                    println!("[info] New groth16 job from `{peer_id}`: `{groth16_details:#?}`");
                                    if jobs.contains_key(&format!("{}-g16", compute_job.job_id)) {
                                        continue;
                                    }                                                                        
                                    prepare_groth16_job_futures.push(
                                        prepare_groth16_job(
                                            &ds_client,
                                            compute_job.job_id.clone(),                                                
                                            groth16_details.cid.clone(),
                                            peer_id
                                        )
                                    );                                    
                                },                                
                            }
                        },

                        // status update inquiry
                        Need::UpdateMe(req_job_id) => {
                            let mut proofs = Vec::<Proof>::new();
                            for job in jobs.values().filter(|j| j.base_id == req_job_id) {
                                if job.owner != peer_id {
                                    continue;
                                }
                                let proof_cid = match &job.status {
                                    job::Status::ExecutionSucceeded(proof_cid) =>
                                        proof_cid.clone(),
                                    
                                    _ => continue,
                                };
                                let proof_type = match &job.job_type {
                                    job::JobType::Prove(segment_id) => 
                                        protocol::ProofType::ProveAndLift(*segment_id),
                                    
                                    job::JobType::Join(left_proof_cid, right_proof_cid) =>
                                        protocol::ProofType::Join(
                                            left_proof_cid.clone(),
                                            right_proof_cid.clone()
                                        ),

                                    job::JobType::Groth16 => 
                                        protocol::ProofType::Groth16,
                                };
                                proofs.push(Proof {
                                    job_id: job.base_id.clone(),
                                    proof_type: proof_type,
                                    cid: proof_cid,
                                });                                
                            }                            
                            if proofs.len() > 0 {
                                let _ = swarm
                                    .behaviour_mut().req_resp
                                    .send_request(
                                        &peer_id,
                                        protocol::Request::ProofIsReady(proofs),
                                    );
                            }
                        },
                    };
                },

                _ => {
                    // println!("{:#?}", event)
                },
            },            

            // prove job is ready to start
            prep_res = prepare_prove_job_futures.select_next_some() => {
                if let Err(failed) = prep_res {
                    eprintln!("[warn] Failed to prepare prove job: `{:#?}`", failed);
                    //@ wtd here?
                    continue;
                }
                let prepared_prove_job: PreparedProveJob = prep_res.unwrap();
                let prove_id = format!(
                    "{}-{}",
                    prepared_prove_job.job_id,
                    prepared_prove_job.segment_id
                );
                // keep track of running jobs                
                jobs.insert(                    
                    prove_id.clone(),
                    job::Job {
                        base_id: prepared_prove_job.job_id,
                        id: prove_id.clone(),
                        working_dir: prepared_prove_job.working_dir,
                        owner: prepared_prove_job.owner,
                        status: job::Status::Running,
                        job_type: job::JobType::Prove(prepared_prove_job.segment_id),
                        proof: None,
                        db_oid: None,
                    },
                );
                // run it
                prove_execution_futures.push(
                    recursion::prove_and_lift(
                        prove_id,
                        prepared_prove_job.segment_file_path.into()
                    )
                );             
            },

            // prove job is finished
            er = prove_execution_futures.select_next_some() => {                
                if let Err(failed) = er {
                    eprintln!("[warn] Failed to run the prove job: `{:#?}`", failed);       
                    //@ wtd with job?
                    let job = jobs.get_mut(&failed.job_id).unwrap();
                    job.status = job::Status::ExecutionFailed(failed.err_msg);
                    continue;
                }
                let res = er.unwrap();
                let job = jobs.get_mut(&res.job_id).unwrap();                
                println!("[info] `prove` is finished for `{}`, let's upload the proof.", res.job_id);
                // record to db                
                let mut db_proof = db::Proof {
                    client_job_id: job.base_id.clone(),
                    job_id: job.id.clone(),
                    job_type: db::JobType::Prove(
                        if let job::JobType::Prove(seg_id) = job.job_type { seg_id } else { u32::MAX }
                    ),
                    owner: job.owner.to_string(),
                    blob: res.blob.clone(),
                    blob_filepath: None,
                    blob_cid: None,
                };
                // save proof to disk
                let proof_filepath = format!("{}/proof", job.working_dir);
                if let Err(e) = fs::write(
                    &proof_filepath,
                    &res.blob
                ) {
                    eprintln!("[warn] Failed to save proof to disk: `{e:?}`, path: `{proof_filepath}`");
                    job.proof = Some(job::Proof {
                        filepath: None,
                        blob: res.blob.clone() //@ copying 230kb is inefficient
                    });
                    match col_proofs.insert_one(db_proof).await {
                        Ok(inserted_id) => {
                            println!("[info] DB insert was successful: `{:?}`", inserted_id);
                        },

                        Err(e) => {
                            eprintln!("[warn] DB insert was failed: `{e:?}`");
                        }
                    };
                    continue;
                }                
                job.proof = Some(job::Proof {
                    filepath: Some(proof_filepath.clone()),
                    blob: res.blob.clone() //@ copying 230kb is inefficient
                });
                db_proof.blob_filepath = Some(proof_filepath.clone());
                db_insert_futures.push(
                    insert_into_db(&col_proofs, db_proof)
                );
                proof_upload_futures.push(
                    upload_proof(
                        &ds_client,
                        &ds_key,
                        res.job_id,
                        proof_filepath, 
                    )
                );
            },

            // join job is ready to start
            prep_res = prepare_join_job_futures.select_next_some() => {
                if let Err(failed) = prep_res {
                    eprintln!("[warn] Failed to prepare join job: `{:#?}`", failed);
                    //@ wtd here?
                    continue;
                }
                let prepared_join_job: PreparedJoinJob = prep_res.unwrap();                
                let join_id = format!(
                    "{}-{}-{}",
                    prepared_join_job.job_id,
                    prepared_join_job.left_proof_cid,
                    prepared_join_job.right_proof_cid
                );
                // keep track of running jobs                
                jobs.insert(
                    join_id.clone(),
                    job::Job {
                        base_id: prepared_join_job.job_id,
                        id: join_id.clone(),
                        working_dir: prepared_join_job.working_dir,
                        owner: prepared_join_job.owner,
                        status: job::Status::Running,
                        job_type: job::JobType::Join(
                            prepared_join_job.left_proof_cid,
                            prepared_join_job.right_proof_cid
                        ),
                        proof: None,
                        db_oid: None,
                    }
                );
                // run it
                join_execution_futures.push(
                    recursion::join(
                        join_id.clone(),
                        prepared_join_job.left_proof_filepath.into(),
                        prepared_join_job.right_proof_filepath.into()
                    )
                );             
            },

            // join job is finished
            er = join_execution_futures.select_next_some() => {
                if let Err(failed) = er {
                    eprintln!("[warn] Failed to run the join job: `{:#?}`", failed);       
                    let job = jobs.get_mut(&failed.job_id).unwrap();
                    job.status = job::Status::ExecutionFailed(failed.err_msg);
                    continue;
                }
                let res = er.unwrap();
                let job = jobs.get_mut(&res.job_id).unwrap();                
                println!("[info] `join` is finished for `{}`, let's upload the proof.", res.job_id);
                // record to db                
                let mut db_proof = db::Proof {
                    client_job_id: job.base_id.clone(),
                    job_id: job.id.clone(),
                    job_type: db::JobType::Prove(
                        if let job::JobType::Prove(seg_id) = job.job_type { seg_id } else { u32::MAX }
                    ),
                    owner: job.owner.to_string(),
                    blob: res.blob.clone(),
                    blob_filepath: None,
                    blob_cid: None,
                };
                // save proof to disk
                let proof_filepath = format!("{}/proof", job.working_dir);
                if let Err(e) = fs::write(
                    &proof_filepath,
                    &res.blob
                ) {
                    eprintln!("[warn] Failed to save proof to disk: `{e:?}`, path: `{proof_filepath}`");
                    job.proof = Some(job::Proof {
                        filepath: None,
                        blob: res.blob.clone() //@ copying 230kb is inefficient
                    });
                    match col_proofs.insert_one(db_proof).await {
                        Ok(inserted_id) => {
                            println!("[info] DB insert was successful: `{:?}`", inserted_id);
                        },

                        Err(e) => {
                            eprintln!("[warn] DB insert was failed: `{e:?}`");
                        }
                    };
                    continue;
                }                
                job.proof = Some(job::Proof {
                    filepath: Some(proof_filepath.clone()),
                    blob: res.blob.clone() //@ copying 230kb is inefficient
                });
                db_proof.blob_filepath = Some(proof_filepath.clone());
                db_insert_futures.push(
                    insert_into_db(&col_proofs, db_proof)
                );
                proof_upload_futures.push(
                    upload_proof(
                        &ds_client,
                        &ds_key,
                        res.job_id,
                        proof_filepath, 
                    )
                );                     
            },

            // groth16 job is ready to start
            prep_res = prepare_groth16_job_futures.select_next_some() => {
                if let Err(failed) = prep_res {
                    eprintln!("[warn] Failed to prepare groth16 job: `{:#?}`", failed);
                    //@ wtd here?
                    continue;
                }
                let prepared_groth16_job: PreparedGroth16Job = prep_res.unwrap();
                let groth16_id = format!("{}-g16",prepared_groth16_job.job_id);
                // keep track of running jobs                
                jobs.insert(                    
                    groth16_id.clone(),
                    job::Job {
                        base_id: prepared_groth16_job.job_id,
                        id: groth16_id.clone(),
                        working_dir: prepared_groth16_job.working_dir,
                        owner: prepared_groth16_job.owner,
                        status: job::Status::Running,
                        job_type: job::JobType::Groth16,
                        proof: None,
                        db_oid: None,
                    },
                );
                // run it
                groth16_execution_futures.push(
                    recursion::to_groth16(
                        groth16_id,
                        prepared_groth16_job.input_proof_filepath.into()
                    )
                );             
            },

            // groth16 job is finished
            er = groth16_execution_futures.select_next_some() => {                
                if let Err(failed) = er {
                    eprintln!("[warn] Failed to run the groth16 job: `{:#?}`", failed);       
                    //@ wtd with job?
                    let job = jobs.get_mut(&failed.job_id).unwrap();
                    job.status = job::Status::ExecutionFailed(failed.err_msg);
                    continue;
                }
                let res = er.unwrap();
                let job = jobs.get_mut(&res.job_id).unwrap();
                println!("[info] `groth16` extraction is finished for `{}`.", res.job_id);
                // record to db                
                let mut db_proof = db::Proof {
                    client_job_id: job.base_id.clone(),
                    job_id: job.id.clone(),
                    job_type: db::JobType::Prove(
                        if let job::JobType::Prove(seg_id) = job.job_type { seg_id } else { u32::MAX }
                    ),
                    owner: job.owner.to_string(),
                    blob: res.blob.clone(),
                    blob_filepath: None,
                    blob_cid: None,
                };
                // save proof to disk
                let proof_filepath = format!("{}/proof", job.working_dir);
                if let Err(e) = fs::write(
                    &proof_filepath,
                    &res.blob
                ) {
                    eprintln!("[warn] Failed to save proof to disk: `{e:?}`, path: `{proof_filepath}`");
                    job.proof = Some(job::Proof {
                        filepath: None,
                        blob: res.blob.clone() //@ copying 230kb is inefficient
                    });
                } else {
                    job.proof = Some(job::Proof {
                        filepath: Some(proof_filepath.clone()),
                        blob: res.blob.clone()
                    });
                    db_proof.blob_filepath = Some(proof_filepath.clone());
                }
                const CUSTOM_ENGINE: engine::GeneralPurpose =
                    engine::GeneralPurpose::new(&alphabet::URL_SAFE, general_purpose::NO_PAD);
                let b64_encoded_proof = CUSTOM_ENGINE.encode(&res.blob);
                job.status = job::Status::ExecutionSucceeded(b64_encoded_proof.clone());
                db_proof.blob_cid = Some(b64_encoded_proof.clone());
                let _ = swarm
                    .behaviour_mut()
                    .req_resp
                    .send_request(
                        &job.owner,
                        protocol::Request::ProofIsReady(vec![
                            protocol::Proof {
                                job_id: job.base_id.clone(),
                                proof_type: protocol::ProofType::Groth16,
                                cid: b64_encoded_proof
                            }
                        ])
                    );
                db_insert_futures.push(
                    insert_into_db(&col_proofs, db_proof)
                );
            },

            // proof upload is ready
            upload_res = proof_upload_futures.select_next_some() => {
                match upload_res {
                    Err(failure) => {
                        eprintln!(
                            "[warn] Proof upload failed for `{}`: `{:#?}`",
                            failure.job_id,
                            failure.err_msg
                        );
                        let job = jobs.get_mut(&failure.job_id).unwrap();
                        job.status = match job.status {
                            job::Status::UploadFailed(attempts) => 
                                job::Status::UploadFailed(attempts + 1),

                            _ =>
                                job::Status::UploadFailed(0u32)
                        };                            
                    },

                    Ok(uploaded)=> {
                        let job = jobs.get_mut(&uploaded.job_id).unwrap();
                        job.status = job::Status::ExecutionSucceeded(uploaded.cid.clone());
                        let proof_type = match &job.job_type {
                            job::JobType::Prove(segment_id) => 
                                protocol::ProofType::ProveAndLift(*segment_id),

                            job::JobType::Join(left_cid, right_cid) => 
                                protocol::ProofType::Join(left_cid.clone(), right_cid.clone()),

                            _ => continue,
                        };
                        let _ = swarm
                            .behaviour_mut()
                            .req_resp
                            .send_request(
                                &job.owner,
                                protocol::Request::ProofIsReady(vec![
                                    protocol::Proof {
                                        job_id: job.base_id.clone(),
                                        proof_type: proof_type,
                                        cid: uploaded.cid.clone()
                                    }
                                ])
                            );
                        // update db
                        if let Some(oid) = &job.db_oid {
                            db_update_futures.push(
                                col_proofs.update_one(
                                    doc! {
                                        "_id": oid.clone()
                                    },
                                    doc! {
                                        "$set": doc! {
                                            "blob_cid": Some(uploaded.cid),
                                        }
                                    }
                                )
                                .into_future()
                            );
                        } else {
                            eprintln!("[warn] No record on db to update proof's cid.");
                        }

                    },
                };
            },

            // retry failed uploads
            () = timer_retry_proof_upload.select_next_some() => {
                for retry_job in jobs
                    .values()
                    .filter(|j| 
                        if let job::Status::UploadFailed(attempts) = j.status {
                            // retry upload for up to an hour
                            attempts <= 60
                        } else {
                            eprintln!("[warn] Job {} has more than 60 failed upload attempts.", j.id);
                            false
                        }
                    )
                {
                    println!("[info] Join finished for `{}`, let's upload the proof.", retry_job.id);
                    proof_upload_futures.push(
                        upload_proof(
                            &ds_client,
                            &ds_key,
                            retry_job.id.clone(),
                            retry_job.proof.as_ref().unwrap().filepath.clone().unwrap(),
                        )
                    ); 
                }
            },

            res = db_insert_futures.select_next_some() => {
                match res {
                    Ok(db_insert_result) => {
                        println!(
                            "[info] DB insert was successful for `{}`: `{:?}`",
                            db_insert_result.job_id,
                            db_insert_result.inserted_id
                        );
                        let job = jobs.get_mut(&db_insert_result.job_id).unwrap();
                        job.db_oid = Some(db_insert_result.inserted_id);
                    },

                    Err(db_insert_error) => {
                        eprintln!(
                            "[warn] DB insert was failed for `{}`: `{:?}`",
                            db_insert_error.job_id,
                            db_insert_error.err_msg
                        );
                    },
                }                
            },

            res = db_update_futures.select_next_some() => {
                match res {
                    Err(e) => eprintln!("[warn] DB update was failed: `{:#?}`", e),

                    Ok(oid) => println!("[info] DB update was successful: `{:?}`", oid)
                } 
            },
        }
    }
}

fn get_home_dir() -> anyhow::Result<String> {
    let err_msg = "Home dir is not available";
    let binding = home::home_dir()
        .ok_or_else(|| anyhow::Error::msg(err_msg))?;
    let home_dir = binding.to_str()
        .ok_or_else(|| anyhow::Error::msg(err_msg))?;
    Ok(home_dir.to_string())
}

async fn mongodb_setup(
    uri: &str,
) -> anyhow::Result<mongodb::Client> {
    println!("[info] Connecting to the MongoDB daemon...");
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
    println!("[info] Successfully connected to the MongoDB instance!");
    Ok(client)
}

#[derive(Debug, Clone)]
struct PreparedProveJob {    
    // client specified id
    job_id: String,

    segment_id: u32,
    
    working_dir: String,
    
    owner: PeerId,
    
    segment_file_path: String,

}

// set up prove job for execution:
//   - download segment and save it to job's folder
async fn prepare_prove_job(
    ds_client: &reqwest::Client,
    job_id: String,
    segment_id: u32,
    segment_cid: String,
    owner: PeerId,
) -> anyhow::Result<PreparedProveJob> {
    let working_dir = format!(
        "{}/.wholesum/prover/jobs/{}/prove/{}",
        get_home_dir()?,
        job_id,
        segment_id
    );
    fs::create_dir_all(working_dir.clone())?;
    let segment_file_path = format!("{working_dir}/segment");
    lighthouse::download_file(
        ds_client,
        &segment_cid,
        segment_file_path.clone()
    ).await?;
    
    Ok(PreparedProveJob {
        job_id: job_id.clone(),
        segment_id: segment_id,
        working_dir: working_dir,
        owner: owner,
        segment_file_path: segment_file_path
    })
}

#[derive(Debug, Clone)]
struct PreparedJoinJob {
    // client specified id    
    job_id: String,
    
    owner: PeerId,
    
    left_proof_cid: String,
    left_proof_filepath: String,

    right_proof_cid: String,
    right_proof_filepath: String,

    working_dir: String,
}

// set up join job for execution:
//   - download left and right proofs(succinct) and save them to the join folder of the job
async fn prepare_join_job(
    ds_client: &reqwest::Client,
    job_id: String,
    left_proof_cid: String,
    right_proof_cid: String,
    owner: PeerId,
) -> anyhow::Result<PreparedJoinJob> {
    let working_dir = format!(
        "{}/.wholesum/prover/jobs/{}/join/{}-{}",
        get_home_dir()?,
        job_id,
        left_proof_cid,
        right_proof_cid
    );
    fs::create_dir_all(working_dir.clone())?;
    let left_proof_filepath = format!("{working_dir}/left");
    lighthouse::download_file(
        ds_client,
        &left_proof_cid,
        left_proof_filepath.clone()
    ).await?;
    let right_proof_filepath = format!("{working_dir}/right");
    lighthouse::download_file(
        ds_client,
        &right_proof_cid,
        right_proof_filepath.clone()
    ).await?;
    
    Ok(PreparedJoinJob {
        job_id: job_id.clone(),
        owner: owner,
        left_proof_cid: left_proof_cid,
        left_proof_filepath: left_proof_filepath,
        right_proof_cid: right_proof_cid,
        right_proof_filepath: right_proof_filepath,
        working_dir: working_dir
    })
}

#[derive(Debug, Clone)]
struct PreparedGroth16Job {    
    // client specified id
    job_id: String,

    working_dir: String,
    
    owner: PeerId,
    
    input_proof_filepath: String,
}

// set up prove job for execution:
//   - download segment and save it to job's folder
async fn prepare_groth16_job(
    ds_client: &reqwest::Client,
    job_id: String,
    input_proof_cid: String,
    owner: PeerId,
) -> anyhow::Result<PreparedGroth16Job> {
    let working_dir = format!(
        "{}/.wholesum/prover/jobs/{}/groth16",
        get_home_dir()?,
        job_id,
    );
    fs::create_dir_all(working_dir.clone())?;
    let input_proof_filepath = format!("{working_dir}/input_proof");
    lighthouse::download_file(
        ds_client,
        &input_proof_cid,
        input_proof_filepath.clone()
    ).await?;
    
    Ok(PreparedGroth16Job {
        job_id: job_id.clone(),
        working_dir: working_dir,
        owner: owner,
        input_proof_filepath: input_proof_filepath
    })
}

#[derive(Debug, Clone)]
struct UploadProof {
    job_id: String,

    cid: String,    
}

#[derive(Debug, Clone)]
pub struct UploadProofFailure {
    pub job_id: String,

    pub err_msg: String,
}

async fn upload_proof(
    ds_client: &reqwest::Client,
    ds_key: &str,
    job_id: String,
    proof_filepath: String,
) -> anyhow::Result<UploadProof, UploadProofFailure> {    
    lighthouse::upload_file(
        &ds_client,
        &ds_key,
        proof_filepath.clone(),
        format!("{job_id}-proof")
    ).await
    .and_then(|upload_res|
        Ok(UploadProof {
            job_id: job_id.clone(),
            cid: upload_res.cid
        })        
    )
    .map_err(|e| UploadProofFailure {
        job_id: job_id.clone(),
        err_msg: e.to_string()
    })    
}

#[derive(Debug, Clone)]
struct DBInsertResult {
    job_id: String,

    inserted_id: Bson,    
}

#[derive(Debug, Clone)]
struct DBInsertError {
    job_id: String,

    err_msg: String,    
}

async fn insert_into_db(
    col_proofs: &mongodb::Collection<db::Proof>,
    db_proof: db::Proof
) -> anyhow::Result<DBInsertResult, DBInsertError> {
    let job_id = db_proof.job_id.clone();
    col_proofs
    .insert_one(db_proof)
    .await
    .and_then(|insert_result| 
        Ok(
            DBInsertResult {
                job_id: job_id.clone(),
                inserted_id: insert_result.inserted_id
            }
        )
    )
    .map_err(|e|
        DBInsertError {        
            job_id: job_id,
            err_msg: e.to_string()
        }
    )
}