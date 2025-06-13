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
        BTreeMap
    },
    future::IntoFuture,
};

use tokio::time::interval;
use tokio_stream::wrappers::IntervalStream;

use env_logger::Env;
use log::{info, warn};
// use chrono::{DateTime, Utc};

use clap::Parser;
use xxhash_rust::xxh3::xxh3_128;

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

use peyk::{
    p2p::{
        MyBehaviourEvent
    },
    protocol,
    protocol::{
        NeedKind,
        ProofToken,
        InputBlob,
    },
    protocol::Request::{
        WouldProve,
        TransferBlob,
    },
};

mod job;
mod r0;
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

#[tokio::main]
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
    // once generated, aggregated proofs are held here
    let mut proof_blobs = HashMap::<u128, Vec<u8>>::new();
    
    // pull jobs to completion
    let mut keccak_futures = FuturesUnordered::new();
    let mut zkr_futures = FuturesUnordered::new();
    let mut aggregate_futures = FuturesUnordered::new();
    let mut groth16_futures = FuturesUnordered::new();

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
            warn!("No keys were supplied, so one is generated for you and saved to `./key.secret` file.");
            new_key
        }
    };    
    info!("my peer id: `{:?}`", PeerId::from_public_key(&local_key.public()));    

    // Libp2p swarm 
    let mut swarm = peyk::p2p::setup_swarm(&local_key).await?;
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

    let mut timer_peer_discovery = IntervalStream::new(
        interval(Duration::from_secs(5 * 60))
    )
    .fuse();
    // it takes ~5s for a rtx 3090 to prove 2m cycles
    let mut timer_satisfy_job_prerequisities = IntervalStream::new(
        interval(Duration::from_secs(5 * 60))
    )
    .fuse();
    loop {
        select! {
            // try to discover new peers
            _i = timer_peer_discovery.select_next_some() => {
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

            _i = timer_satisfy_job_prerequisities.select_next_some() => {
                jobs.values()
                    .filter(|j| j.status == job::Status::WaitingForBlobs)
                    .for_each(|j| {
                        j.prerequisites.values()
                            .for_each(|token| {
                                token.owners
                                    .iter()
                                    .for_each(|owner| {
                                        //@ move peer_id from string calculation to when job is being created
                                        let peer_id = match PeerId::from_bytes(owner.as_bytes()) {
                                            Ok(p) => p,

                                            Err(e) => {
                                                warn!("PeerId is invalid: {owner}: {e:?}");
                                                return
                                            }
                                        };
                                        let _req_id = swarm
                                            .behaviour_mut()
                                            .req_resp
                                            .send_request(
                                                &peer_id,
                                                TransferBlob(token.hash)
                                            );
                                        info!(
                                            "Requested transfer of blob `{}` from `{}`",
                                            token.hash,
                                            owner
                                        );
                                    });
                            })
                    });
            }

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
                    let need: NeedKind = match bincode::deserialize(&message.data) {
                        Ok(n) => n,

                        Err(e) => {
                            warn!("Gossip(need) message decode error: `{e:?}`");
                            continue;
                        },

                    };
                    info!("Received `{need:?}` need.");
                    match need {
                        NeedKind::Prove(_num_jobs) => {
                            let _req_id = swarm
                                .behaviour_mut().req_resp
                                .send_request(
                                    &peer_id,
                                    WouldProve,
                                );
                        },

                        NeedKind::Groth16(_nonce) => {
                            //@ handle mac os compatibility
                            let _req_id = swarm
                                .behaviour_mut().req_resp
                                .send_request(
                                    &peer_id,
                                    WouldProve,
                                );
                        }                        
                    };
                },

                // requests
                SwarmEvent::Behaviour(MyBehaviourEvent::ReqResp(request_response::Event::Message {
                    peer: _peer_id,
                    message: request_response::Message::Request {
                        request,
                        channel,
                        //request_id,
                        ..
                    }
                })) => {                
                    match request {
                        TransferBlob(hash) => {
                            //@ check db
                            if let Some(blob) = proof_blobs.get(&hash) {
                                let _req_id = swarm
                                    .behaviour_mut()
                                    .req_resp
                                    .send_response(
                                        channel,
                                        protocol::Response::BlobIsReady(
                                            blob.clone()
                                        )
                                    );
                                info!("Requested proof `{hash}` is found and sent back.");
                            }
                        },

                        _ => {
                            continue
                        }
                    };
                },

                // responses
                SwarmEvent::Behaviour(MyBehaviourEvent::ReqResp(request_response::Event::Message {
                    peer: client_peer_id,
                    message: request_response::Message::Response {
                        response,
                        //response_id,
                        ..
                    }
                })) => {                
                    match response {
                        protocol::Response::BlobIsReady(blob) => {
                            let hash = xxh3_128(&blob);
                            if let Some(job) = jobs.values_mut()
                                .filter(|j| j.status == job::Status::WaitingForBlobs)
                                .find(|j| j.pending_blobs.contains_key(&hash))
                            {
                                let index = job.pending_blobs.remove(&hash).unwrap();
                                let _ = job.prerequisites.remove(&index);
                                job.input_blobs.insert(index, blob.clone());
                                proof_blobs.insert(hash, blob);
                                info!(
                                    "Received blob `{}` from `{:?}`",
                                    hash,
                                    client_peer_id
                                );
                                if job.prerequisites.is_empty() {
                                    info!("Job {} is ready to run.", job.id);
                                    let input_blobs = job.input_blobs.values().cloned().collect();
                                    job.status = job::Status::Running;
                                    let blobs_are_segment = if let job::Kind::Segment(_) = job.kind {
                                        true
                                    } else if let job::Kind::Join(_) = job.kind {
                                        false
                                    } else {
                                        warn!("Invlid job kind for aggregate: `{:?}`", job.kind);
                                        continue
                                    };
                                    aggregate_futures.push(
                                        r0::aggregate(job.id.clone(), input_blobs, blobs_are_segment)
                                    );                                    
                                }
                            }
                        },

                        protocol::Response::Job(compute_job) => {                            
                            match compute_job.kind {
                                protocol::JobKind::Keccak(keccak_details) => {
                                    info!(
                                        "Received Keccak job from client: `{:?}`",
                                        client_peer_id
                                    );
                                    let prove_id = format!(
                                        "{}-{:?}",
                                        compute_job.id,
                                        keccak_details.claim_digest
                                    );
                                    // keep track of running jobs                
                                    jobs.insert(                    
                                        prove_id.clone(),
                                        job::Job {
                                            base_id: compute_job.id,
                                            id: prove_id.clone(),
                                            owner: client_peer_id,
                                            status: job::Status::Running,
                                            kind: job::Kind::Keccak(keccak_details.claim_digest.clone()),
                                            input_blobs: BTreeMap::from([
                                                (0, keccak_details.blob.clone())
                                            ]),
                                            prerequisites: BTreeMap::new(),
                                            pending_blobs: HashMap::new(),
                                            proof: None,
                                        },
                                    );
                                    // run it
                                    //@ use reference of blobs instead
                                    keccak_futures.push(
                                        r0::prove_keccak(
                                            prove_id,
                                            keccak_details.blob.clone() 
                                        )
                                    );
                                },

                                protocol::JobKind::Zkr(zkr_details) => {
                                    info!(
                                        "Received Zkr job from client: `{:?}`",
                                        client_peer_id
                                    );
                                    let prove_id = format!(
                                        "{}-{:?}",
                                        compute_job.id,
                                        zkr_details.claim_digest
                                    );
                                    // keep track of running jobs                
                                    jobs.insert(                    
                                        prove_id.clone(),
                                        job::Job {
                                            base_id: compute_job.id,
                                            id: prove_id.clone(),
                                            owner: client_peer_id,
                                            status: job::Status::Running,
                                            kind: job::Kind::Zkr(zkr_details.claim_digest.clone()),
                                            input_blobs: BTreeMap::from([
                                                (0, zkr_details.blob.clone())
                                            ]),
                                            prerequisites: BTreeMap::new(),
                                            pending_blobs: HashMap::new(),
                                            proof: None,
                                        },
                                    );
                                    // run it
                                    //@ use reference of blobs instead
                                    zkr_futures.push(
                                        r0::prove_zkr(
                                            prove_id,
                                            zkr_details.blob.clone() 
                                        )
                                    );
                                }

                                protocol::JobKind::Aggregate(agg_details) => {
                                    info!(
                                        "Received Aggregate job `{:?}` from client: `{:?}`",
                                        agg_details.id,
                                        client_peer_id
                                    );
                                    let prove_id = format!(
                                        "{}-{}",
                                        compute_job.id,
                                        agg_details.id
                                    );
                                    let mut input_blobs = BTreeMap::new();
                                    let mut prerequisites = BTreeMap::new();
                                    let mut pending_blobs = HashMap::new();
                                    for (i, input_blob) in agg_details.batch.into_iter().enumerate() {
                                        match input_blob {
                                            InputBlob::Blob(b) => {
                                                input_blobs.insert(i, b);
                                            },

                                            InputBlob::Token(hash, owners) => {
                                                if let Some(blob) = proof_blobs.get(&hash) {
                                                    input_blobs.insert(i, blob.clone());
                                                } else {                                                        
                                                    prerequisites.insert(
                                                        i, 
                                                        job::Token {
                                                            hash: hash,
                                                            owners: owners
                                                        }
                                                    );
                                                    pending_blobs.insert(hash, agg_details.id as usize);
                                                }
                                            }
                                        };
                                    }
                                    let ready_for_proving = prerequisites.is_empty();
                                    if ready_for_proving {                                        
                                        let cloned_blobs = input_blobs.values().cloned().collect();                                        
                                        aggregate_futures.push(
                                            r0::aggregate(
                                                prove_id.clone(),
                                                cloned_blobs,
                                                agg_details.blobs_are_segment
                                            )
                                        );
                                    } else {
                                        info!(
                                            "The following blobs must be received for the job to start: `{:?}`",
                                            prerequisites.values().map(|token| token.hash).collect::<Vec<_>>()
                                        );
                                    }
                                    jobs.insert(
                                        prove_id.clone(),
                                        job::Job {
                                            base_id: compute_job.id,
                                            id: prove_id,
                                            owner: client_peer_id,
                                            status: if ready_for_proving {
                                                job::Status::Running
                                            } else {
                                                job::Status::WaitingForBlobs
                                            },
                                            kind: if agg_details.blobs_are_segment { 
                                                job::Kind::Segment(agg_details.id)
                                            } else {
                                                job::Kind::Join(agg_details.id)
                                            },
                                            input_blobs: input_blobs,
                                            prerequisites: prerequisites,
                                            pending_blobs: pending_blobs,
                                            proof: None,
                                        }
                                    );
                                },
                                
                                protocol::JobKind::Groth16(groth16_details) => {
                                    info!(
                                        "Received Groth16 job from client: `{:?}`",
                                        client_peer_id
                                    );
                                    let groth16_id = format!("{}-g16",compute_job.id);
                                    // keep track of running jobs                
                                    jobs.insert(                    
                                        groth16_id.clone(),
                                        job::Job {
                                            base_id: compute_job.id.clone(),
                                            id: groth16_id.clone(),
                                            owner: client_peer_id,
                                            status: job::Status::Running,
                                            kind: job::Kind::Groth16,
                                            input_blobs: BTreeMap::from([
                                                (0, groth16_details.blob.clone())
                                            ]),
                                            prerequisites: BTreeMap::new(),
                                            pending_blobs: HashMap::new(),
                                            proof: None,
                                        },
                                    );
                                    // run it
                                    //@ use reference of blobs instead
                                    groth16_futures.push(
                                        r0::to_groth16(
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

            // keccak is proved
            er = keccak_futures.select_next_some() => {
                if let Err(failed) = er {
                    warn!("Failed to run prove keccak job: `{:#?}`", failed);       
                    //@ wtd with job?
                    let job = jobs.get_mut(&failed.job_id).unwrap();
                    job.status = job::Status::ExecutionFailed(failed.err_msg);
                    continue;
                }
                let res = er.unwrap();
                let job = jobs.get_mut(&res.job_id).unwrap();
                job.status = job::Status::ExecutionSucceded;
                info!("Prove keccak is finished for `{}`", res.job_id);
                let claim_digest = if let job::Kind::Keccak(claim_digest) = job.kind { 
                    claim_digest
                } else {
                    warn!("No claim digest is available for `{:?}`.", job.kind);
                    [0u8; 32]
                };
                let blob_hash = xxh3_128(&res.blob);
                // record to db                
                db_insert_futures.push(
                    col_proofs.insert_one(
                        db::Proof {
                            client_job_id: job.base_id.clone(),
                            job_id: job.id.clone(),
                            kind: db::JobKind::Keccak(claim_digest),
                            owner: job.owner.to_string(),
                            input_blobs: job.input_blobs.values().cloned().collect(),
                            blob: res.blob.clone(),    
                            hash: blob_hash,                
                        }
                    )
                    .into_future()
                );
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
                        protocol::Request::ProofIsReady(
                            protocol::ProofToken {
                                job_id: job.base_id.clone(),
                                kind: protocol::ProofKind::Assumption(
                                    claim_digest,
                                    res.blob
                                ),
                                hash: blob_hash
                            }
                        )
                    ); 
            },

            // zkr is proved
            er = zkr_futures.select_next_some() => {
                if let Err(failed) = er {
                    warn!("Failed to run prove zkr job: `{:#?}`", failed);       
                    //@ wtd with job?
                    let job = jobs.get_mut(&failed.job_id).unwrap();
                    job.status = job::Status::ExecutionFailed(failed.err_msg);
                    continue;
                }
                let res = er.unwrap();
                let job = jobs.get_mut(&res.job_id).unwrap();
                job.status = job::Status::ExecutionSucceded;
                info!("Prove zkr is finished for `{}`", res.job_id);
                let claim_digest = if let job::Kind::Zkr(claim_digest) = job.kind { 
                    claim_digest
                } else {
                    warn!("No claim digest is available for `{:?}`.", job.kind);
                    [0u8; 32]
                };
                let blob_hash = xxh3_128(&res.blob);
                // record to db                
                db_insert_futures.push(
                    col_proofs.insert_one(
                        db::Proof {
                            client_job_id: job.base_id,
                            job_id: job.id.clone(),
                            kind: db::JobKind::Zkr(claim_digest),
                            owner: job.owner.to_string(),
                            input_blobs: job.input_blobs.values().cloned().collect(),
                            blob: res.blob.clone(),                    
                            hash: blob_hash
                        }
                    )
                    .into_future()
                );

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
                        protocol::Request::ProofIsReady(
                            ProofToken {
                                job_id: job.base_id.clone(),
                                kind: protocol::ProofKind::Assumption(
                                    claim_digest,
                                    res.blob
                                ),
                                hash: blob_hash
                            }
                        )
                    ); 
            },

            // batch is proved
            er = aggregate_futures.select_next_some() => {
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
                info!("Aggregate job `{}` is proved.", res.job_id);
                let batch_id = match job.kind { 
                    job::Kind::Segment(id) | job::Kind::Join(id) => id,
                    
                    _ => {
                        warn!("No batch id is available for `{:?}`.", job.kind);
                        u128::MAX
                    }
                };
                let blob_hash = xxh3_128(&res.blob);
                // record to db                
                db_insert_futures.push(
                    col_proofs.insert_one(
                        db::Proof {
                            client_job_id: job.base_id,
                            job_id: job.id.clone(),
                            kind: db::JobKind::Segment(batch_id),
                            owner: job.owner.to_string(),
                            input_blobs: job.input_blobs.values().cloned().collect(),
                            blob: res.blob.clone(),
                            hash: blob_hash,
                        }
                    )
                    .into_future()
                );

                job.proof = Some(
                    job::Proof {
                        hash: blob_hash,
                        blob: res.blob.clone()
                    }
                );
                proof_blobs.insert(blob_hash, res.blob);

                let _ = swarm
                    .behaviour_mut()
                    .req_resp
                    .send_request(
                        &job.owner,
                        protocol::Request::ProofIsReady(
                            protocol::ProofToken {
                                job_id: job.base_id.clone(),
                                kind: protocol::ProofKind::Aggregate(batch_id),
                                hash: blob_hash
                            }
                        )
                    ); 
            },            

            // groth16 job is finished
            er = groth16_futures.select_next_some() => {                
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
                let blob_hash = xxh3_128(&res.blob);
                // record to db                
                db_insert_futures.push(
                    col_proofs.insert_one(
                        db::Proof {
                            client_job_id: job.base_id.clone(),
                            job_id: job.id.clone(),
                            kind: db::JobKind::Groth16,
                            owner: job.owner.to_string(),
                            input_blobs: job.input_blobs.values().cloned().collect(),
                            blob: res.blob.clone(),       
                            hash: blob_hash,             
                        }
                    )
                    .into_future()
                );

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
                        protocol::Request::ProofIsReady(
                            protocol::ProofToken {
                                job_id: job.base_id.clone(),
                                kind: protocol::ProofKind::Groth16(res.blob),
                                hash: blob_hash,
                            }
                        )
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
//     Ok(home_dir.to_string
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