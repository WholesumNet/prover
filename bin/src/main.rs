
use futures::{
    select,
    stream::{
        FuturesUnordered,
        StreamExt,
        TryStreamExt
    },
};

use std::{
    error::Error,
    time::Duration,
    collections::{
        HashMap,
        BTreeMap,
        VecDeque
    },
    future::IntoFuture,
    thread,
    sync::mpsc
};

use tokio::time::{
    self, interval
};
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
        InputBlob,
        ProofToken,
        ProofKind
    },
    protocol::Request::{
        Would,
        TransferBlob,
    },
};

use zkvm;

mod job;
use job::Job;
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

    // maintain jobs
    let mut active_job = None;
    let mut ready_jobs = VecDeque::new();
    let mut pending_jobs = Vec::new();

    // pull jobs to completion
    let mut run_futures = FuturesUnordered::new();

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
    let mut swarm = peyk::p2p::setup_swarm(&local_key)?;
    let topic = gossipsub::IdentTopic::new("<-- Wholesum p2p prover bazaar -->");
    let _ = swarm
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
        interval(Duration::from_secs(5))
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
                pending_jobs
                    .iter()                    
                    .for_each(|j: &Job| {
                        j.prerequisites.values()
                            .for_each(|token| {
                                //@ move peer_id from string calculation to when job is being created
                                let peer_id = match PeerId::from_bytes(&token.owner) {
                                    Ok(p) => p,

                                    Err(e) => {
                                        warn!("PeerId is invalid: {e:?}");
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
                                    peer_id
                                );
                            });
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
                    let _need: NeedKind = match bincode::deserialize(&message.data) {
                        Ok(n) => n,

                        Err(e) => {
                            warn!("Gossip(need) message decode error: `{e:?}`");
                            continue;
                        },

                    };
                    if !ready_jobs.is_empty() {
                        continue;
                    }
                    // info!("Gossip: need `{need:?}`");
                    let _req_id = swarm
                        .behaviour_mut().req_resp
                        .send_request(
                            &peer_id,
                            Would,
                        );                     
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
                            if let Ok(blob) = lookup_proof(&col_proofs, hash).await {
                                let _req_id = swarm
                                    .behaviour_mut()
                                    .req_resp
                                    .send_response(
                                        channel,
                                        protocol::Response::BlobIsReady(blob)
                                    );
                                info!("Requested blob `{hash}` is found and sent back.");                                    
                            } else {
                                warn!("Requested blob `{hash}` is not available.");
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
                            pending_jobs.retain_mut(|job| { 
                                if !job.pending_blobs.contains_key(&hash) {
                                    return true
                                }
                                let index = job.pending_blobs.remove(&hash).unwrap();
                                let _ = job.prerequisites.remove(&index);
                                job.input_blobs.insert(index, blob.clone());
                                //@ should be saved to db?
                                info!("Received requested blob `{hash}` from `{client_peer_id}`");
                                if job.prerequisites.is_empty() {
                                    info!("Job {} is ready to run.", job.id);
                                    ready_jobs.push_back(job.clone());
                                }
                                !job.prerequisites.is_empty()
                            });
                            if active_job.is_none() {
                                // prove it
                                if let Some(j) = ready_jobs.pop_front() {
                                    run_futures.push(
                                        spawn_run(
                                            j.input_blobs.values().cloned().collect(),
                                            j.kind.clone()
                                        )
                                    );
                                    active_job = Some(j);
                                }
                            }
                        },

                        protocol::Response::Job(compute_job) => {                            
                            match compute_job.kind {
                                protocol::JobKind::R0(r0_op) => match r0_op {
                                    protocol::R0Op::Assumption(ass_details) => {
                                        info!(
                                            "Received assumption job from peer `{}`",
                                            client_peer_id
                                        );
                                        let mut input_blobs = BTreeMap::new();
                                        let mut prerequisites = BTreeMap::new();
                                        let mut pending_blobs = HashMap::new();
                                        for (i, input_blob) in ass_details.batch.into_iter().enumerate() {
                                            match input_blob {
                                                InputBlob::Blob(b) => {
                                                    input_blobs.insert(i, b);
                                                },

                                                InputBlob::Token(hash, owner) => {
                                                    if let Ok(blob) = lookup_proof(&col_proofs, hash).await {
                                                        input_blobs.insert(i, blob.clone());
                                                    } else {                                                        
                                                        prerequisites.insert(
                                                            i, 
                                                            job::Token {
                                                                hash: hash,
                                                                owner: owner
                                                            }
                                                        );
                                                        pending_blobs.insert(hash, ass_details.id as usize);
                                                    }
                                                }
                                            };
                                        }
                                        let ready_for_proving = prerequisites.is_empty();
                                        let job = Job {
                                            id: compute_job.id,
                                            owner: client_peer_id,
                                            kind: if ass_details.blobs_are_keccak { 
                                                zkvm::JobKind::R0(zkvm::R0Op::Keccak, ass_details.id)
                                            } else {
                                                zkvm::JobKind::R0(zkvm::R0Op::Union, ass_details.id)
                                            },
                                            input_blobs: input_blobs,
                                            prerequisites: prerequisites,
                                            pending_blobs: pending_blobs,
                                        };
                                        if ready_for_proving {
                                            ready_jobs.push_back(job);                                        
                                        } else {                                        
                                            pending_jobs.push(job);
                                        }                   
                                    },                                

                                    protocol::R0Op::Aggregate(agg_details) => {
                                        info!(
                                            "Received aggregate job `{}` from peer `{}`",
                                            agg_details.id,
                                            client_peer_id
                                        );
                                        let mut input_blobs = BTreeMap::new();
                                        let mut prerequisites = BTreeMap::new();
                                        let mut pending_blobs = HashMap::new();
                                        for (i, input_blob) in agg_details.batch.into_iter().enumerate() {
                                            match input_blob {
                                                InputBlob::Blob(b) => {
                                                    input_blobs.insert(i, b);
                                                },

                                                InputBlob::Token(hash, owner) => {
                                                    if let Ok(blob) = lookup_proof(&col_proofs, hash).await {
                                                        input_blobs.insert(i, blob.clone());
                                                    } else {                                                        
                                                        prerequisites.insert(
                                                            i, 
                                                            job::Token {
                                                                hash: hash,
                                                                owner: owner
                                                            }
                                                        );
                                                        pending_blobs.insert(hash, agg_details.id as usize);
                                                    }
                                                }
                                            };
                                        }
                                        let ready_for_proving = prerequisites.is_empty();
                                        let job = Job {
                                            id: compute_job.id,
                                            owner: client_peer_id,
                                            kind: if agg_details.blobs_are_segment { 
                                                zkvm::JobKind::R0(zkvm::R0Op::Segment, agg_details.id)
                                            } else {
                                                zkvm::JobKind::R0(zkvm::R0Op::Join, agg_details.id)
                                            },
                                            input_blobs: input_blobs,
                                            prerequisites: prerequisites,
                                            pending_blobs: pending_blobs,
                                        };
                                        if ready_for_proving {
                                            ready_jobs.push_back(job);                                        
                                        } else {                                        
                                            pending_jobs.push(job);
                                        }                                    
                                    },
                                    
                                    protocol::R0Op::Groth16(groth16_details) => {
                                        info!(
                                            "Received Groth16 job from peer `{}`",
                                            client_peer_id
                                        );                                    
                                        let (batch_id, blob) = {
                                            let ib = &groth16_details.batch[0];
                                            if let protocol::InputBlob::Blob(b) = &ib.1 {
                                                (ib.0, b.clone())
                                            } else {
                                                warn!(
                                                    "Input blob should be of blob kind but is: `{:?}`",
                                                    ib.1                                                
                                                );
                                                continue
                                            }
                                        };
                                        // keep track of running jobs                
                                        ready_jobs.push_back(Job {
                                            id: compute_job.id,
                                            owner: client_peer_id,
                                            kind: zkvm::JobKind::R0(zkvm::R0Op::Groth16, batch_id),
                                            input_blobs: BTreeMap::from([
                                                (0, blob)
                                            ]),
                                            prerequisites: BTreeMap::new(),
                                            pending_blobs: HashMap::new()
                                        });
                                    }
                                },

                                protocol::JobKind::SP1(sp1_op) => match sp1_op {
                                    protocol::SP1Op::Execute(execute_details) => {
                                        info!(
                                            "Received execute job from peer `{}`",
                                            client_peer_id
                                        );
                                        let blob = {
                                            let ib = &execute_details.batch[0];
                                            if let protocol::InputBlob::Blob(b) = &ib {
                                                b.clone()
                                            } else {
                                                warn!(
                                                    "Input blob should be of blob kind but is: `{:?}`",
                                                    ib                                                
                                                );
                                                continue
                                            }
                                        };
                                        let elf_kind = match execute_details.elf_kind {
                                            protocol::ELFKind::Subblock => zkvm::ELFKind::Subblock,

                                            protocol::ELFKind::Agg => zkvm::ELFKind::Agg,
                                        };
                                        ready_jobs.push_back(Job {
                                            id: compute_job.id,
                                            owner: client_peer_id,
                                            kind: zkvm::JobKind::SP1(
                                                zkvm::SP1Op::Execute(elf_kind),
                                                execute_details.id
                                            ),
                                            input_blobs: BTreeMap::from([
                                                (0, blob)
                                            ]),
                                            prerequisites: BTreeMap::new(),
                                            pending_blobs: HashMap::new(),
                                        });
                                    },
                                }
                            };
                            if active_job.is_none() {
                                // prove it
                                if let Some(j) = ready_jobs.pop_front() {
                                    run_futures.push(
                                        spawn_run(
                                            j.input_blobs.values().cloned().collect(),
                                            j.kind.clone()
                                        )
                                    );
                                    active_job = Some(j);
                                }
                            }
                        }
                    }
                },

                _ => {
                    // println!("{:#?}", event)
                },
            },

            res = run_futures.select_next_some() => {
                if let Err(e) = res {
                    let job = active_job.take().unwrap();                    
                    warn!("Failed to run job `{}`: `{:?}`", job.id, e);
                    continue
                }
                let proof_blob = res.unwrap();
                let job = active_job.take().unwrap();
                let (_batch_id, proof_kind, db_prove_kind) = match job.kind {
                    zkvm::JobKind::R0(r0_op, bid) => match r0_op {
                        zkvm::R0Op::Segment => {
                            info!("Segment aggregate `{}` is proved for `{}`", bid, job.id);
                            (
                                bid,
                                ProofKind::Aggregate(bid),
                                db::ProveKind::Segment(bid.to_string())
                            )
                        },

                        zkvm::R0Op::Join => {
                            info!("Join aggregate `{}` is proved for `{}`", bid, job.id);
                            (
                                bid,
                                ProofKind::Aggregate(bid),
                                db::ProveKind::Join(bid.to_string())
                            )
                        },

                        zkvm::R0Op::Keccak => {
                            info!("Keccak aggregate `{}` is proved for `{}`", bid, job.id);
                            (
                                bid,
                                ProofKind::Assumption(bid),
                                db::ProveKind::Assumption(bid.to_string())
                            )
                        },

                        zkvm::R0Op::Union => {
                            info!("Union aggregate `{}` is proved for `{}`", bid, job.id);
                            (
                                bid,
                                ProofKind::Assumption(bid),
                                db::ProveKind::Assumption(bid.to_string())
                            )
                        },

                        zkvm::R0Op::Groth16 => {
                            info!("Groth16 extraction `{}` is finished for `{}`", bid, job.id);
                            (
                                bid,
                                ProofKind::Groth16(bid, proof_blob.clone()),
                                db::ProveKind::Assumption(bid.to_string())
                            )
                        },
                    },

                    zkvm::JobKind::SP1(sp1_op, bid) => match sp1_op {
                        zkvm::SP1Op::Execute(_elf_kind) => {
                            info!("Execution suceeded for `{bid}`!");
                            (
                                bid,
                                ProofKind::Aggregate(bid),
                                db::ProveKind::Segment(bid.to_string())
                            )
                        }
                    },
                };
                let hash = xxh3_128(&proof_blob);
                // record to db
                let input_hashes = job
                    .input_blobs
                    .values()
                    .map(|b| xxh3_128(b).to_string())
                    .collect();
                db_insert_futures.push(
                    col_proofs.insert_one(
                        db::Proof {
                            job_id: job.id.to_string(),
                            kind: db_prove_kind,
                            input_hashes: input_hashes,
                            owner: job.owner.to_bytes(),
                            blob: proof_blob.clone(),    
                            hash: hash.to_string(),                
                        }
                    )
                    .into_future()
                );

                let _req_id = swarm
                    .behaviour_mut()
                    .req_resp
                    .send_request(
                        &job.owner,
                        protocol::Request::ProofIsReady(
                            ProofToken {
                                job_id: job.id,
                                kind: proof_kind,
                                hash: hash
                            }
                        )
                    );
                // start a new prove
                if let Some(j) = ready_jobs.pop_front() {
                    run_futures.push(
                        spawn_run(
                            j.input_blobs.values().cloned().collect(),
                            j.kind.clone()
                        )
                    );
                    active_job = Some(j);
                }

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
    let mut client_options = ClientOptions::parse(uri).await?;
    let server_api = ServerApi::builder().version(ServerApiVersion::V1).build();
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

async fn spawn_run(
    input_blobs: Vec<Vec<u8>>,
    kind: zkvm::JobKind
) -> anyhow::Result<Vec<u8>>{
    let (tx, rx) = mpsc::channel();   
    let handle = thread::spawn(move || 
        if let Err(e) = tx.send(zkvm::run(input_blobs, kind)) {
            warn!("Failed to share the prove result with the main thread: `{e:?}`");
        }
    );
    loop {
        if handle.is_finished() {
            break 
        }
        time::sleep(Duration::from_millis(100)).await;
    }
    let _ = handle.join();
    rx.recv()?
}

// look up proof in the db
async fn lookup_proof(
    col_proofs: &mongodb::Collection<db::Proof>,
    hash: u128
) -> anyhow::Result<Vec<u8>> {
    if let Some(proof) = col_proofs.find(
        doc! {
            "hash": hash.to_string()
        }
    )
    .await?
    .try_next()
    .await?
    {
        Ok(proof.blob)
    } else {
        Err(anyhow::Error::msg("Blob not found."))
    }    
}
