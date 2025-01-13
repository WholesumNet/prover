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

use comms::{
    p2p::{ MyBehaviourEvent },
    protocol,
    protocol::{ Need, JobUpdate, JobType },
};
use dstorage::lighthouse;


mod job;
mod recursion;

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

    // let's maintain a list of jobs
    let mut jobs = HashMap::<String, job::Job>::new();
    // cids' download futures
    let mut prepare_prove_job_futures = FuturesUnordered::new();
    let mut prepare_join_job_futures = FuturesUnordered::new();
    // let mut prepare_groth16_job_futures = FuturesUnordered::new();

    // pull jobs' execution
    let mut prove_execution_futures = FuturesUnordered::new();
    let mut join_execution_futures = FuturesUnordered::new();
    // let mut groth16_execution_futures = FuturesUnordered::new();

    let mut proof_upload_futures = FuturesUnordered::new();
    
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
                    println!("[info] Inbound identify event `{:#?}`", info);
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
                                    println!("[info] New prove job from `{peer_id}`: `{prove_details:?}`");
                                    let mut progress_map = BitVec::from_bytes(&prove_details.progress_map);
                                    progress_map.truncate(prove_details.num_segments.try_into().unwrap());
                                    let mut unproved_segments = vec![];
                                    for (segment_id, is_proved) in progress_map.iter().enumerate() {
                                        if false == is_proved &&
                                           false == jobs.contains_key(
                                               &format!("{}-{}", compute_job.job_id, segment_id)
                                           ) //@ slow af, optimize it
                                        {
                                            unproved_segments.push(segment_id);
                                        }
                                    }
                                    if unproved_segments.len() == 0 {
                                        println!("[warn] No unproved segments to prove.");
                                        continue;
                                    }
                                    let chosen_segment = *unproved_segments.choose(&mut rng).unwrap() as u32;
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
                                    println!("[info] New join job from `{peer_id}`: `{join_details:?}`");
                                    let mut progress_map = BitVec::from_bytes(&join_details.progress_map);
                                    progress_map.truncate(join_details.pairs.len());
                                    let mut unproved_pairs = vec![];
                                    for (index, is_proved) in progress_map.iter().enumerate() {
                                        let pair = &join_details.pairs[index];
                                        if false == is_proved &&
                                           false == jobs.contains_key(
                                               &format!("{}-{}-{}", compute_job.job_id, pair.0, pair.1)
                                           ) //@ slow af, optimize it
                                        {
                                            unproved_pairs.push(pair);
                                        }
                                    }
                                    if unproved_pairs.len() == 0 {
                                        println!("[warn] No unproved pairs to join.");
                                        continue;
                                    }
                                    let choosen_pair = *unproved_pairs.choose(&mut rng).unwrap();
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

                                JobType::Groth16(_groth16_details) => {},                                
                            }
                        },

                        // status update inquiry
                        Need::UpdateMe(_) => {
                            let mut updates = Vec::<JobUpdate>::new();
                            for job in jobs.values() {
                                if job.owner != peer_id {
                                    continue;
                                }
                                let status = match &job.status {
                                    job::Status::ExecutionSucceeded(proof_cid) =>
                                        protocol::JobStatus::ExecutionSucceeded(proof_cid.clone()),

                                    job::Status::ExecutionFailed(err) => 
                                        protocol::JobStatus::ExecutionFailed(Some(err.clone())),
                                    
                                    // all the rest are trivial status
                                    _ => protocol::JobStatus::Running,
                                };
                                updates.push(JobUpdate {
                                    id: job.id.clone(),
                                    status: status,
                                    item: match &job.job_type {
                                        job::JobType::Prove(segment_id) => 
                                            protocol::Item::ProveAndLift(*segment_id),

                                        job::JobType::Join(left_proof_cid, right_proof_cid) =>
                                            protocol::Item::Join(left_proof_cid.clone(), right_proof_cid.clone()),

                                        job::JobType::Groth16 => 
                                            protocol::Item::Groth16,
                                    },
                                });
                            }                            
                            if updates.len() > 0 {
                                let _ = swarm
                                    .behaviour_mut().req_resp
                                    .send_request(
                                        &peer_id,
                                        protocol::Request::Update(updates),
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
                        id: prove_id.clone(),                                        
                        owner: prepared_prove_job.owner,
                        status: job::Status::Running,
                        proof_file_path: None,
                        job_type: job::JobType::Prove(prepared_prove_job.segment_id),
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
                let prove_id = res.job_id;             
                println!("[info] Prove finished for `{}`, let's upload the proof.", prove_id);
                proof_upload_futures.push(
                    upload_proof(
                        &ds_client,
                        &ds_key,
                        format!(
                            "{}/.wholesum/prover/jobs/{}",
                            get_home_dir()?,
                            prove_id
                        ),
                        prove_id.clone(),
                        res.blob, //@ copying 230kb is inefficient
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
                // keep track of running jobs
                jobs.insert(
                    prepared_join_job.join_id.clone(),
                    job::Job {
                        id: prepared_join_job.job_id,                                        
                        owner: prepared_join_job.owner,
                        status: job::Status::Running,
                        proof_file_path: None,
                        job_type: job::JobType::Join(
                            prepared_join_job.left_proof_cid,
                            prepared_join_job.right_proof_cid
                        ),
                    }
                );
                // run it
                join_execution_futures.push(
                    recursion::join(
                        prepared_join_job.join_id.clone(),
                        prepared_join_job.left_proof_file_path.into(),
                        prepared_join_job.right_proof_file_path.into()
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
                let join_id = res.job_id;             
                println!("[info] Join finished for `{}`, let's upload the proof.", join_id);
                proof_upload_futures.push(
                    upload_proof(
                        &ds_client,
                        &ds_key,
                        format!(
                            "{}/.wholesum/prover/jobs/{}",
                            get_home_dir()?,
                            join_id
                        ),
                        join_id.clone(),
                        res.blob,
                    )
                );                            
            },

            // groth16 job is finished
            // er = groth16_execution_futures.select_next_some() => {
            //     if let Err(failed) = er {
            //         eprintln!("[warn] Failed to run the groth16 job: `{:#?}`", failed);       
            //         //@ what to to with job id?
            //         // let _job_id = failed.who;
            //         // job.status = job::Status::ExecutionFailed;
            //         //     eprintln!("[warn] Job `{}`'s execution finished with error: `{}`",
            //         //         exec_res.job_id,
            //         //         exec_res.error_message.unwrap_or_else(|| String::from("")),
            //         //     );


            //         continue;
            //     }                                
            // },

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
                        job.status = job::Status::ExecutionFailed(failure.err_msg);

                    },

                    Ok(uploaded)=> {
                        let job = jobs.get_mut(&uploaded.job_id).unwrap();
                        job.proof_file_path = Some(uploaded.proof_file_path);
                        job.status = job::Status::ExecutionSucceeded(uploaded.proof_cid);
                    },

                };
            },
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

#[derive(Debug, Clone)]
struct PreparedProveJob {
    
    // client specified id
    job_id: String,

    segment_id: u32,
    
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
    let prove_dir = format!(
        "{}/.wholesum/prover/jobs/{}-{}",
        get_home_dir()?,
        job_id,
        segment_id
    );
    fs::create_dir_all(prove_dir.clone())?;
    let segment_file_path = format!("{prove_dir}/segment-{segment_id}");
    lighthouse::download_file(
        ds_client,
        &segment_cid,
        segment_file_path.clone()
    ).await?;
    
    Ok(PreparedProveJob {
        job_id: job_id.clone(),
        segment_id: segment_id,
        owner: owner,
        segment_file_path: segment_file_path
    })
}

#[derive(Debug, Clone)]
struct PreparedJoinJob {
    
    job_id: String,
    
    // "job_id-left_cid-right_cid"
    join_id: String,
    
    owner: PeerId,
    
    left_proof_cid: String,
    left_proof_file_path: String,

    right_proof_cid: String,
    right_proof_file_path: String,
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
    let join_dir = format!(
        "{}/.wholesum/prover/jobs/{}/join",
        get_home_dir()?,
        job_id
    );
    fs::create_dir_all(join_dir.clone())?;
    let left_proof_file_path = format!("{join_dir}/{left_proof_cid}-left");
    lighthouse::download_file(
        ds_client,
        &left_proof_cid,
        left_proof_file_path.clone()
    ).await?;
    let right_proof_file_path = format!("{join_dir}/{right_proof_cid}-right");
    lighthouse::download_file(
        ds_client,
        &right_proof_cid,
        right_proof_file_path.clone()
    ).await?;
    
    Ok(PreparedJoinJob {
        job_id: job_id.clone(),
        join_id: format!("{job_id}-{left_proof_cid}-{right_proof_cid}"),
        owner: owner,
        left_proof_cid: left_proof_cid,
        left_proof_file_path: left_proof_file_path,
        right_proof_cid: right_proof_cid,
        right_proof_file_path: right_proof_file_path
    })
}

#[derive(Debug, Clone)]
struct UploadProof {

    job_id: String,

    proof_file_path: String,

    proof_cid: String,
    
}

#[derive(Debug, Clone)]
pub struct UploadProofFailure {
    pub job_id: String,

    pub err_msg: String,
}

async fn upload_proof(
    ds_client: &reqwest::Client,
    ds_key: &str,
    residue_dir: String,
    job_id: String,
    proof_blob: Vec<u8>,
) -> anyhow::Result<UploadProof, UploadProofFailure> {
    // save to disk
    let proof_file_path = format!("{residue_dir}/proof");
    let _bytes_written = fs::write(
        &proof_file_path,
        proof_blob
    )
    .map_err(|e| UploadProofFailure {
        job_id: job_id.clone(),
        err_msg: e.to_string()
    })?; 
    // upload
    lighthouse::upload_file(
        &ds_client,
        &ds_key,
        proof_file_path.clone(),
        format!("{job_id}-proof")
    ).await
    .and_then(|upload_res|
        Ok(UploadProof {
            job_id: job_id.clone(),
            proof_file_path: proof_file_path,
            proof_cid: upload_res.cid
        })        
    )
    .map_err(|e| UploadProofFailure {
        job_id: job_id.clone(),
        err_msg: e.to_string()
    })    
}
