
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
    p2p::GlobalBehaviourEvent,
    protocol,
    protocol::{
        NeedKind,
        // InputToken,
        ProofToken,
        Request::Would
    },    
    blob_transfer
};

use zkvm::{
    sp1::SP1Handle
};

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
    #[arg(short, long)]
    key_file: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let cli = Cli::parse();
    info!("<-> `Prover` agent for Wholesum network <->");
   
    // setup mongodb
    // let _db_client = mongodb_setup("mongodb://localhost:27017").await?;

    // the current job
    let mut cur_job: Option<Job> = None;    

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
    let mut swarm = peyk::p2p::setup_global_swarm(&local_key)?;
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
    
    // init kademlia: get to know bootnode(s)
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
    let mut timer_peer_discovery = IntervalStream::new(
        interval(Duration::from_secs(60))
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
                let random_peer_id = PeerId::random();
                info!("Searching for the closest peers to `{random_peer_id}`");
                swarm
                    .behaviour_mut()
                    .kademlia
                    .get_closest_peers(random_peer_id);
            },

            _i = timer_satisfy_job_prerequisities.select_next_some() => {},

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

                // identify events
                SwarmEvent::Behaviour(
                    GlobalBehaviourEvent::Identify(
                        identify::Event::Received {
                            // peer_id,
                            // info,
                            ..
                        }
                    )
                ) => {
                    //     info!(
                    //         "Inbound identify event from {}: {:#?}`",
                    //         peer_id,
                    //         info
                    //     );                        
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
            
                SwarmEvent::Behaviour(GlobalBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source: peer_id,
                    message,
                    ..
                })) => {
                    if cur_job.is_some() {
                        continue;
                    }
                    let _need: NeedKind = match bincode::deserialize(&message.data) {
                        Ok(n) => n,

                        Err(e) => {
                            warn!("Gossip(need) message decode error: `{e:?}`");
                            continue;
                        },

                    };
                    let _req_id = swarm
                        .behaviour_mut()
                        .req_resp
                        .send_request(
                            &peer_id,
                            Would,
                        );
                },

                // kademlia events
                SwarmEvent::Behaviour(GlobalBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                    result: kad::QueryResult::GetClosestPeers(Ok(ok)),
                    ..
                })) => {
                    info!("Query finished with closest peers: {:#?}", ok.peers);
                },

                SwarmEvent::Behaviour(GlobalBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                    result:
                        kad::QueryResult::GetClosestPeers(Err(kad::GetClosestPeersError::Timeout {
                            ..
                        })),
                    ..
                })) => {
                    warn!("Query for closest peers timed out");
                },

                // SwarmEvent::Behaviour(GlobalBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                //     result: kad::QueryResult::GetProviders(Ok(found_providers)),
                //     ..
                // })) => {
                    
                // },

                // requests
                SwarmEvent::Behaviour(GlobalBehaviourEvent::ReqResp(request_response::Event::Message {
                    peer: _peer_id,
                    message: request_response::Message::Request {
                        // request,
                        // channel,
                        // request_id,
                        ..
                    },
                    ..
                })) => {},

                // responses
                SwarmEvent::Behaviour(GlobalBehaviourEvent::ReqResp(request_response::Event::Message {
                    peer: client_peer_id,
                    message: request_response::Message::Response {
                        response,
                        //request_id,
                        ..
                    },
                    ..
                })) => {                
                    match response {
                        protocol::Response::Job(compute_job) => {
                            if cur_job.is_some() {
                                continue;
                            }
                            match compute_job.kind {                                
                                protocol::JobKind::SP1(sp1_op) => match sp1_op {                                    
                                    protocol::SP1Op::Prove(prove_details) => {
                                        let (op, kind_str) = match prove_details.elf_kind {
                                            protocol::ELFKind::Subblock => (
                                                zkvm::SP1Op::ProveSubblock,
                                                "Subblock"
                                            ),

                                            protocol::ELFKind::Agg => (
                                                zkvm::SP1Op::ProveAgg,
                                                "Aggregation"
                                            )
                                        };
                                        let mut job = Job::new(
                                            compute_job.id,
                                            client_peer_id.clone(),
                                            zkvm::JobKind::SP1(op, prove_details.id)
                                        );
                                        info!(
                                            "Received Prove job('{}') from `{}`: `{}`.",
                                            job.get_batch_id(),
                                            client_peer_id,
                                            kind_str
                                        );
                                        //@ check if I have the proof already
                                        for input_token in prove_details.tokens.into_iter() {
                                            let fulfilled = blob_store.contains_key(&input_token.hash);
                                            job.add_prerequisite(input_token.hash, fulfilled);
                                            if fulfilled {
                                                info!(
                                                    "Prerequisite blob(`{}`) is already fulfilled.",
                                                    input_token.hash
                                                );
                                            } else {
                                                if let Some(owner_peer_id) = peer_id_from_bytes(&input_token.owner) {                                                    
                                                    //@ retry mechanism?
                                                    let req_id = swarm
                                                        .behaviour_mut()
                                                        .blob_transfer
                                                        .send_request(
                                                            &owner_peer_id,
                                                            blob_transfer::Request(input_token.hash.to_string())
                                                        );
                                                    info!(
                                                        "Requested prerequisite blob(`{}`) from `{}`. req_id: `{}`.",
                                                        input_token.hash,
                                                        owner_peer_id,
                                                        req_id
                                                    );                                                                                                        
                                                } else {
                                                    warn!("Failed to decode owner PeerId, aborting job.");
                                                    continue;
                                                }
                                            }                                             
                                        }
                                        cur_job = Some(job);                                    
                                    },
                                }
                            };                            
                        }
                    }
                },

                // <blob transfer>
                // requests
                SwarmEvent::Behaviour(GlobalBehaviourEvent::BlobTransfer(request_response::Event::Message {
                    peer: peer_id,
                    message: request_response::Message::Request {
                        request: blob_transfer::Request(blob_hash),
                        channel,
                        request_id,
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
                                "Failed to initiate the requested blob(`{}`)'s transmission to `{}`. req_id `{:?}`: `{:?}`.",
                                blob_hash,
                                peer_id,
                                request_id,
                                e,
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

                // responses
                SwarmEvent::Behaviour(GlobalBehaviourEvent::BlobTransfer(request_response::Event::Message {
                    peer: peer_id,
                    message: request_response::Message::Response {
                        response: blob_transfer::Response(blob),
                        // request_id,
                        ..
                    },
                    ..
                })) => {
                    if cur_job.is_none() {
                        warn!(
                            "Received unsolicited blob from `{}`.",
                            peer_id
                        );
                        continue;                        
                    }
                    let blob_hash = xxh3_128(&blob);
                    let job = cur_job.as_mut().unwrap();
                    if !job.is_a_prerequisite(blob_hash) {
                        warn!(
                            "Blob(`{}`) `{}` is not a prerequisite.",
                            blob_hash,
                            peer_id
                        );
                        continue;
                    }

                    blob_store.insert(blob_hash, blob);
                    job.set_prerequisite_as_fulfilled(blob_hash);
                    info!(
                        "Prerequisite blob(`{}`) of job(`{}`) is fullfilled.",
                        blob_hash,
                        job.get_batch_id()
                    );
                    if job.is_ready() {                        
                        info!(
                            "Job(`{}`) is ready to run.",
                            job.get_batch_id()
                        );
                        let (num_prerequisites, prerequisites) = {
                            let p = job.prerequisites();
                            (p.len(), p)
                        };
                        let inputs: Vec<_> = prerequisites
                            .into_iter()
                            .filter_map(|blob_hash| blob_store.remove(&blob_hash))
                            .collect();
                        if inputs.len() != num_prerequisites {
                            warn!(
                                "`{}` prerequisites are missing. Job won't start.",
                                num_prerequisites - inputs.len()
                            );
                            //@ remove blobs from blob_store?
                            cur_job = None;
                            continue;
                        }                        
                        info!(
                            "Prove is started for job(`{}`).",
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
                    }
                },

                // inbound
                SwarmEvent::Behaviour(GlobalBehaviourEvent::BlobTransfer(request_response::Event::InboundFailure {
                    peer: peer_id,
                    connection_id,
                    request_id,
                    error,
                })) => {
                    warn!(
                        "Blob transfer `inbound failure`: peer `{}`, con_id: {:?}, req_id: {:?}, e: {:?} ",
                        peer_id,
                        connection_id,
                        request_id,
                        error
                    );
                },

                // outbound
                SwarmEvent::Behaviour(GlobalBehaviourEvent::BlobTransfer(request_response::Event::OutboundFailure {
                    peer: peer_id,
                    connection_id,
                    request_id,
                    error,
                })) => {
                    warn!(
                        "Blob transfer `outbound failure`: peer `{}`, con_id: {:?}, req_id: {:?}, e: {:?} ",
                        peer_id,
                        connection_id,
                        request_id,
                        error
                    );
                },

                _ => {
                    // info!("{:#?}", event)
                },
            },

            result = run_futures.select_next_some() => {                
                let job = cur_job.take().unwrap();
                if let Err(e) = result {
                    warn!(
                        "Failed to run job(`{}`): `{:?}`",
                        job.get_batch_id(),
                        e
                    );
                    continue;
                }
                let proof = result.unwrap();
                let (is_subblock, batch_id) = match job.kind {
                    zkvm::JobKind::SP1(ref op, batch_id) => {
                        match op {                            
                            zkvm::SP1Op::ProveSubblock => {
                                (true, batch_id)
                            },

                            zkvm::SP1Op::ProveAgg => {
                                (false, batch_id)
                            }
                        }
                    },
                };
                info!(
                    "{}(`{}`) prove is finished for `{}`.",
                    if is_subblock { "Subblock" } else { "Agg" },
                    batch_id,
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
                                kind: if is_subblock {
                                    protocol::ProofKind::Subblock(batch_id)
                                } else {
                                    protocol::ProofKind::Agg(batch_id)
                                },
                                hash: hash
                            }
                        )
                    );
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