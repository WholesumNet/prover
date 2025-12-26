
use futures::{
    select,
    stream::{
        FuturesUnordered,
        StreamExt,
        // TryStreamExt
    },
};

use std::{
    time::Duration,
    env,
    collections::{
        HashMap,
        VecDeque
    },
    // future::IntoFuture,
    thread,
    sync::{
        mpsc,
        Arc,
    },
};
use tokio::{
    time::{
        self,
        interval
    },
};
use tokio_stream::wrappers::IntervalStream;
use env_logger::Env;
use log::{
    info,
    warn
};
use clap::Parser;
use anyhow::Context;
use xxhash_rust::xxh3::xxh3_128;
use libp2p::{
    identity,
    identify,  
    gossipsub,
    mdns,
    kad,
    request_response,
    swarm::{
        SwarmEvent
    },
    PeerId,
    multiaddr::Protocol,
};
// use mongodb::{
//     bson::{
//         doc,
//     },
//     options::{
//         ClientOptions,
//         ServerApi,
//         ServerApiVersion
//     },
// };

use peyk::{
    p2p::{
        MyBehaviourEvent
    },
    protocol,
    protocol::{
        NeedKind,
        // InputToken,
        ProofToken,
    },
    protocol::Request::{
        Would,
    },
    blob_transfer
};

use zkvm::{
    sp1::SP1Handle
};
// use anbar;

mod job;
use job::Job;

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
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .init();
    let cli = Cli::parse();
    info!("<-> `Prover` agent for Wholesum network <->");
    info!("Operating mode: `{}` network",
        if false == cli.dev { "global" } else { "local(development)" }
    ); 

    // setup mongodb
    // let _db_client = mongodb_setup("mongodb://localhost:27017").await?;

    // maintain jobs
    let mut active_job = None;
    let mut ready_jobs = VecDeque::new();
    let mut pending_jobs = HashMap::new();

    info!("Initializing SP1.");  
    let sp1_handle = tokio::task::spawn_blocking(|| 
        Arc::new(SP1Handle::new().unwrap())
    )
    .await
    .unwrap();

    // pull jobs to completion
    let mut run_futures = FuturesUnordered::new();

    // let col_proofs = db_client
    //     .database("wholesum_prover")
    //     .collection::<db::Proof>("proofs");
    
    // futures for mongodb progress saving 
    // let mut db_insert_futures = FuturesUnordered::new();

    // blob store
    let mut blob_store = HashMap::<u128, Vec<u8>>::new();
    
    // Libp2p swarm 
    // peer id
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
    let my_peer_id = PeerId::from_public_key(&local_key.public());    
    info!(
        "My peer id: `{}`",
        my_peer_id
    );    
    let mut swarm = peyk::p2p::setup_swarm(&local_key)?;
    // listen on all interfaces
    // ipv4
    swarm.listen_on(
        "/ip4/0.0.0.0/udp/20201/quic-v1".parse()?
    )?;
    swarm.listen_on(
        "/ip4/0.0.0.0/tcp/20201".parse()?
    )?;
    // ipv6
    // ipv6
    // swarm.listen_on(
    //     "/ip6/::/udp/20201/quic-v1".parse()?
    // )?;
    // swarm.listen_on(
    //     "/ip6/::/tcp/20201".parse()?
    // )?;
    // init gossip
    let topic = gossipsub::IdentTopic::new("<-- Wholesum p2p prover bazaar -->");
    let _ = swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&topic);
    
    // init kademlia
    if !cli.dev {
        // get to know bootnode(s)
        let bootnode_peer_id = env::var("BOOTNODE_PEER_ID")
            .context("`BOOTNODE_PEER_ID` environment variable does not exist.")?;
        let bootnode_ip_addr = env::var("BOOTNODE_IP_ADDR")
            .context("`BOOTNODE_IP_ADDR` environment variable does not exist.")?;
        swarm.behaviour_mut()
            .kademlia
            .add_address(
                &bootnode_peer_id.parse()?,
                format!(
                    "/ip4/{}/tcp/20201",
                    bootnode_ip_addr
                )
                .parse()?
            );
        // initiate bootstrapping
        match swarm.behaviour_mut().kademlia.bootstrap() {
            Ok(query_id) => {            
                info!(
                    "Bootstrap is initiated, query id: {:?}",
                    query_id
                );
            },
            Err(e) => {
                info!(
                    "Bootstrap failed: {:?}",
                    e
                );
            }
        };
        // specify the external address
        let external_ip_addr = env::var("EXTERNAL_IP_ADDR")
            .context("`EXTERNAL_IP_ADDR` environment variable does not exist.")?;
        let external_port = env::var("EXTERNAL_PORT")
            .context("`EXTERNAL_PORT` environment variable does not exist.")?;
        swarm.add_external_address(
            format!(
                "/ip4/{}/tcp/{}",
                external_ip_addr,
                external_port
            )
            .parse()?
        );
        swarm.add_external_address(
            format!(
                "/ip4/{}/udp/{}/quic-v1",
                external_ip_addr,
                external_port
            )
            .parse()?
        );
        info!(
            "Protocol names: {:?}",
            swarm
                .behaviour_mut()
                .kademlia
                .protocol_names()
        );
    }    
    let mut timer_peer_discovery = IntervalStream::new(
        interval(Duration::from_secs(60))
    )
    .fuse();
    // it takes ~5s for a rtx 3090 to prove 2m cycles
    let mut timer_satisfy_job_prerequisities = IntervalStream::new(
        interval(Duration::from_secs(5))
    )
    .fuse();
    // to pull for new peers 
    let mut timer_pull_rendezvous_providers = IntervalStream::new(
        interval(Duration::from_secs(10))
    )
    .fuse();

    loop {
        select! {
            // try to discover new peers
            _i = timer_peer_discovery.select_next_some() => {
                if !cli.dev {
                    continue;
                }
                let random_peer_id = PeerId::random();
                info!("Searching for the closest peers to `{random_peer_id}`");
                swarm
                    .behaviour_mut()
                    .kademlia
                    .get_closest_peers(random_peer_id);
            },

            _i = timer_satisfy_job_prerequisities.select_next_some() => {},

            // pull for new peers
            _i = timer_pull_rendezvous_providers.select_next_some() => {
                // if !cli.dev {
                //     swarm.behaviour_mut()
                //         .kademlia
                //         .get_providers(rendezvous_record.clone().unwrap());
                // }
            },

            // libp2p events
            event = swarm.select_next_some() => match event {
                // general events
                SwarmEvent::NewListenAddr { address, .. } => {
                    info!("Local node is listening on {address}");
                },

                SwarmEvent::ConnectionEstablished {
                    peer_id,
                    endpoint,
                    ..
                } => {
                    info!(
                        "A connection has been established to {} via {:?}",
                        peer_id,
                        endpoint
                    );                    
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
                            // peer_id,
                            // info,
                            ..
                        }
                    )
                ) => {
                    // if !cli.dev {
                    //     info!(
                    //         "Inbound identify event from {}: {:#?}`",
                    //         peer_id,
                    //         info
                    //     );                        
                    // }
                },

                SwarmEvent::NewExternalAddrOfPeer {
                    peer_id,
                    address
                } => {
                    let is_public = address.iter()
                        .filter_map(|c| 
                            if let Protocol::Ip4(ip4_addr) = c {
                                Some(ip4_addr)
                            } else {
                                None
                            }
                        )
                        .all(|a| !a.is_private() && !a.is_loopback());
                    if is_public {                        
                        info!(
                            "Added public address of the peer to the DHT: {}",
                            address
                        );
                        swarm.behaviour_mut()
                            .kademlia
                            .add_address(&peer_id, address);
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

                // kademlia events
                SwarmEvent::Behaviour(MyBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                    result: kad::QueryResult::GetClosestPeers(Ok(ok)),
                    ..
                })) => {
                    info!("Query finished with closest peers: {:#?}", ok.peers);
                },

                SwarmEvent::Behaviour(MyBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                    result:
                        kad::QueryResult::GetClosestPeers(Err(kad::GetClosestPeersError::Timeout {
                            ..
                        })),
                    ..
                })) => {
                    warn!("Query for closest peers timed out");
                },

                // SwarmEvent::Behaviour(MyBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                //     result: kad::QueryResult::GetProviders(Ok(found_providers)),
                //     ..
                // })) => {
                    
                // },

                // requests
                SwarmEvent::Behaviour(MyBehaviourEvent::ReqResp(request_response::Event::Message {
                    peer: _peer_id,
                    message: request_response::Message::Request {
                        request,
                        // channel,
                        // request_id,
                        ..
                    },
                    ..
                })) => {                
                    match request {
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
                    },
                    ..
                })) => {                
                    match response {
                        protocol::Response::Job(compute_job) => {                            
                            match compute_job.kind {                                
                                protocol::JobKind::SP1(sp1_op) => match sp1_op {                                    
                                    protocol::SP1Op::Prove(prove_details) => {
                                        info!("Received Prove job from `{client_peer_id}`.");
                                        let op = match prove_details.elf_kind {
                                            protocol::ELFKind::Subblock => zkvm::SP1Op::ProveSubblock,

                                            protocol::ELFKind::Agg => zkvm::SP1Op::ProveAgg,
                                        };
                                        let mut job = Job::new(
                                            compute_job.id,
                                            client_peer_id,
                                            zkvm::JobKind::SP1(op, prove_details.id)
                                        );
                                        //@ check if I have the proof already
                                        let mut get_blob_info_list = Vec::new();
                                        for (i, input_token) in prove_details.tokens.iter().enumerate() {
                                            job.add_prerequisite(i, input_token.hash);
                                            if blob_store.contains_key(&input_token.hash) {
                                                job.set_prerequisite_as_fulfilled(input_token.hash);
                                            } else {                                                
                                                if let Some(owner_peer_id) = peer_id_from_bytes(&input_token.owner) {
                                                    get_blob_info_list.push((input_token.hash, owner_peer_id));
                                                } else {
                                                    warn!("Job will never run due to missing owner PeerId.");
                                                    continue
                                                }
                                            }                                             
                                        }
                                        pending_jobs.insert(prove_details.id, job);
                                        // request blob info
                                        //@ retry mechanism?
                                        for (hash, owner_peer_id) in get_blob_info_list.into_iter() {
                                            let _req_id = swarm
                                                .behaviour_mut()
                                                .blob_transfer
                                                .send_request(
                                                    &owner_peer_id,
                                                    blob_transfer::Request(hash.to_string())
                                                );
                                            info!(
                                                "Requested blob(`{}`) from `{}`",
                                                hash,
                                                owner_peer_id
                                            );
                                        }
                                    },
                                }
                            };                            
                        }
                    }
                },

                // blob transfer requests
                SwarmEvent::Behaviour(MyBehaviourEvent::BlobTransfer(request_response::Event::Message {
                    peer: peer_id,
                    message: request_response::Message::Request {
                        request: blob_transfer::Request(blob_hash),
                        channel,
                        //request_id,
                        ..
                    },
                    ..
                })) => {
                    let blob_hash = blob_hash.parse::<u128>().unwrap();
                    if let Some(blob) = blob_store.get(&blob_hash) {
                        if let Err(e) = swarm
                            .behaviour_mut()
                            .blob_transfer
                                .send_response(
                                    channel,
                                    blob_transfer::Response(blob.clone())
                                )
                        {
                            warn!(
                                "Failed to initiate the requested blob(`{}`)'s transmission: `{:?}`.",
                                blob_hash,
                                e
                            );
                        } else {
                            info!(
                                "The requested blob(`{}`)'s transmission to `{}` is initiated: {:.2} KB",
                                blob_hash,
                                peer_id,
                                blob.len() as f64 / 1024.0f64
                            );
                        }                            
                    } else {
                        warn!(
                            "The requested blob(`{}`) does not exist.",
                            blob_hash,
                        );
                    }
                },

                // blob transfer responses
                SwarmEvent::Behaviour(MyBehaviourEvent::BlobTransfer(request_response::Event::Message {
                    peer: peer_id,
                    message: request_response::Message::Response {
                        response: blob_transfer::Response(blob),
                        //response_id,
                        ..
                    },
                    ..
                })) => {                
                    let mut ready_job = None;
                    let blob_hash = xxh3_128(&blob);
                    if let Some(job) = pending_jobs
                        .values_mut()
                        .find(|j| j.is_a_prerequisite(blob_hash))
                    {
                        blob_store.insert(blob_hash, blob);
                        job.set_prerequisite_as_fulfilled(blob_hash);
                        info!(
                            "Prerequisite(`{}`) of job(`{}`) is fullfilled.",
                            blob_hash,
                            job.get_batch_id()
                        );
                        if job.is_ready() {
                            let prove_id = match job.kind {
                                zkvm::JobKind::SP1(_, id) => id,                                        
                            };
                            ready_job = Some(prove_id);
                        }
                    } else {
                        warn!(
                            "Received unsolicited blob(`{}`) from `{}`.",
                            blob_hash,
                            peer_id
                        );
                        continue;
                    }
                    // run it
                    if let Some(id) = ready_job {
                        ready_jobs.push_back(pending_jobs.remove(&id).unwrap());
                        info!(
                            "Job(`{}`) is ready to run.",
                            id
                        );
                        if active_job.is_none() {
                            // run it
                            if let Some(job) = ready_jobs.pop_front() {
                                let mut inputs = Vec::new();
                                for blob_hash in job.prerequisites().into_iter() {
                                    if let Some(blob) = blob_store.get(&blob_hash) {
                                        inputs.push(blob.clone());
                                    } else {
                                        warn!(
                                            "Input(`{}`) is not available, job cannot start.",
                                            blob_hash
                                        );
                                    }
                                }
                                info!(
                                    "Prove started for job(`{}`).",
                                    job.get_batch_id()
                                );
                                run_futures.push(
                                    spawn_run(
                                        Arc::clone(&sp1_handle),
                                        job.get_batch_id(),
                                        inputs,
                                        job.kind.clone()
                                    )
                                );                                        
                                active_job = Some(job);
                            }
                        }
                    }
                },

                _ => {
                    // info!("{:#?}", event)
                },
            },

            result = run_futures.select_next_some() => {                
                if let Err(e) = result {
                    let job: Job = active_job.take().unwrap();
                    warn!(
                        "Failed to run job(`{}`): `{:?}`",
                        job.get_batch_id(),
                        e
                    );
                    continue;
                }
                let proof = result.unwrap();
                let job = active_job.take().unwrap();
                match job.kind {
                    zkvm::JobKind::SP1(ref op, sub_id) => {
                        match op {                            
                            zkvm::SP1Op::ProveSubblock => {
                                info!(
                                    "Subblock(`{}`) prove is finished for `{}`",
                                    sub_id,
                                    job.id
                                );
                                let hash = {
                                    let hash = xxh3_128(&proof);
                                    blob_store.insert(hash, proof);
                                    hash
                                };
                                let _req_id = swarm
                                    .behaviour_mut()
                                    .req_resp
                                    .send_request(
                                        &job.owner,
                                        protocol::Request::ProofIsReady(
                                            ProofToken {
                                                job_id: job.id,
                                                kind: protocol::ProofKind::Subblock(job.get_batch_id()),
                                                hash: hash
                                            }
                                        )
                                    );
                            },

                            zkvm::SP1Op::ProveAgg => {
                                info!(
                                    "Agg(`{}`) prove is finished for `{}`",
                                    sub_id,
                                    job.id
                                );
                                let hash = {
                                    let hash = xxh3_128(&proof);
                                    blob_store.insert(hash, proof);
                                    hash
                                };
                                let _req_id = swarm
                                    .behaviour_mut()
                                    .req_resp
                                    .send_request(
                                        &job.owner,
                                        protocol::Request::ProofIsReady(
                                            ProofToken {
                                                job_id: job.id,
                                                kind: protocol::ProofKind::Agg(job.get_batch_id()),
                                                hash: hash
                                            }
                                        )
                                    );
                            }
                        };
                    },
                }                 
                
                // start a new prove
                if let Some(job) = ready_jobs.pop_front() {
                    let mut inputs = Vec::new();
                    for blob_hash in job.prerequisites().into_iter() {
                        if let Some(blob) = blob_store.get(&blob_hash) {
                            inputs.push(blob.clone());
                        } else {
                            warn!("Input is not available, job cannot start.");
                        }
                    }     
                    info!(
                        "Prove started for job(`{}`).",
                        job.get_batch_id()
                    );
                    run_futures.push(
                        spawn_run(
                            Arc::clone(&sp1_handle),
                            job.get_batch_id(),
                            inputs,
                            job.kind.clone()
                        )
                    );                    
                    active_job = Some(job);
                }
            },            
        }
    }
}


// async fn mongodb_setup(
//     uri: &str,
// ) -> anyhow::Result<mongodb::Client> {
//     info!("Connecting to the MongoDB daemon...");
//     let mut client_options = ClientOptions::parse(uri).await?;
//     let server_api = ServerApi::builder().version(ServerApiVersion::V1).build();
//     client_options.server_api = Some(server_api);
//     let client = mongodb::Client::with_options(client_options)?;
//     // Send a ping to confirm a successful connection
//     client
//         .database("admin")
//         .run_command(doc! { "ping": 1 })
//         .await?;
//     info!("Successfully connected to the MongoDB instance.");
//     Ok(client)
// }

async fn spawn_run(
    sp1_handle: Arc<SP1Handle>,
    batch_id: u128,
    inputs: Vec<Vec<u8>>,
    kind: zkvm::JobKind
) -> anyhow::Result<Vec<u8>>{
    let (tx, rx) = mpsc::channel();   
    let handle = thread::spawn(move || { 
            let r = match kind {                
                zkvm::JobKind::SP1(ref op, _batch_id) => {
                    match op {
                        zkvm::SP1Op::ProveSubblock => {
                            let sp1_handle = Arc::clone(&sp1_handle);
                            let stdin = inputs.into_iter().next().unwrap();                            
                            sp1_handle.prove_subblock_on_cluster(batch_id, stdin)                            
                        },

                        zkvm::SP1Op::ProveAgg => {
                            let mut iter = inputs.into_iter();
                            let stdin = iter.next().unwrap();
                            let subblock_proofs = iter.collect();
                            sp1_handle.prove_aggregation_on_cluster(
                                batch_id,
                                stdin,
                                subblock_proofs,
                            )
                        },
                    }
                }
            };
            if let Err(e) = tx.send(r) {
                warn!("Failed to share the prove result with the main thread: `{e:?}`");
            }
        }
    );
    loop {
        if handle.is_finished() {
            break 
        }
        time::sleep(Duration::from_millis(50)).await;
    }
    let _ = handle.join();
    rx.recv()?
}

// look up proof in the db
// async fn lookup_proof(
//     col_proofs: &mongodb::Collection<db::Proof>,
//     hash: u128
// ) -> anyhow::Result<Vec<u8>> {
//     if let Some(proof) = col_proofs.find(
//         doc! {
//             "hash": hash.to_string()
//         }
//     )
//     .await?
//     .try_next()
//     .await?
//     {
//         Ok(proof.blob)
//     } else {
//         Err(anyhow::Error::msg("Blob not found."))
//     }    
// }

fn peer_id_from_bytes(b: &Vec<u8>) -> Option<PeerId> {
    match PeerId::from_bytes(b) {
        Ok(p) => Some(p),

        Err(e) => {
            warn!("PeerId is invalid: {e:?}");
            None
        }
    }
}