
use futures::{
    select,
    stream::{
        FuturesUnordered,
        StreamExt,
        // TryStreamExt
    },
};

use std::{
    error::Error,
    time::Duration,
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
    self, interval
    },
};
use tokio_stream::wrappers::IntervalStream;

use env_logger::Env;
use log::{info, warn};

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
        // InputToken,
        ProofToken,
    },
    protocol::Request::{
        Would,
    },
    blob_transfer
};

use zkvm::{
    sp1::SP1CudaHandle
};
use anbar;

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
async fn main() -> Result<(), Box<dyn Error + 'static>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .init();
    let cli = Cli::parse();
    info!("<-> `Prover` agent for Wholesum network <->");
    info!("Operating mode: `{}` network",
        if false == cli.dev { "global" } else { "local(development)" }
    ); 

    // setup mongodb
    let _db_client = mongodb_setup("mongodb://localhost:27017").await?;

    // maintain jobs
    let mut active_job = None;
    let mut ready_jobs = VecDeque::new();
    let mut pending_jobs = HashMap::new();

    info!("Initializing SP1 CUDA instance...");
    let sp1_cuda_handle = Arc::new(SP1CudaHandle::new()?);

    // pull jobs to completion
    let mut run_futures = FuturesUnordered::new();

    // let col_proofs = db_client
    //     .database("wholesum_prover")
    //     .collection::<db::Proof>("proofs");
    
    // futures for mongodb progress saving 
    // let mut db_insert_futures = FuturesUnordered::new();

    // blob store
    let mut blob_store = anbar::BlobStore::new();
    
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
                // pending_jobs
                //     .iter()                    
                //     .for_each(|j: &Job| {
                //         j.prerequisites.values()
                //             .for_each(|token| {
                //                 //@ move peer_id from string calculation to when job is being created
                //                 let peer_id = match PeerId::from_bytes(&token.owner) {
                //                     Ok(p) => p,

                //                     Err(e) => {
                //                         warn!("PeerId is invalid: {e:?}");
                //                         return
                //                     }
                //                 };
                //                 let _req_id = swarm
                //                     .behaviour_mut()
                //                     .req_resp
                //                     .send_request(
                //                         &peer_id,
                //                         TransferBlob(token.hash)
                //                     );
                //                 info!(
                //                     "Requested transfer of blob `{}` from `{}`",
                //                     token.hash,
                //                     peer_id
                //                 );
                //             });
                //     });
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
                        // channel,
                        // request_id,
                        ..
                    }
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
                    }
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
                                        for (i, input_token) in prove_details.batch.iter().enumerate() {
                                            job.add_prerequisite(i, input_token.hash);
                                            if blob_store.is_blob_complete(input_token.hash) {
                                                job.set_prerequisite_as_fulfilled(input_token.hash);
                                            } else {
                                                blob_store.add_incomplete_blob(input_token.hash);
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
                                        for (hash, owner_peer_id) in get_blob_info_list.into_iter() {
                                            let _req_id = swarm
                                                .behaviour_mut()
                                                .blob_transfer
                                                .send_request(
                                                    &owner_peer_id,
                                                    blob_transfer::Request::GetInfo(hash)
                                                );
                                            info!(
                                                "Requested info of blob(`{}`) from `{}`",
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
                    peer: _peer_id,
                    message: request_response::Message::Request {
                        request,
                        channel,
                        //request_id,
                        ..
                    }
                })) => {
                    match request {
                        blob_transfer::Request::GetInfo(hash) => {
                            if let Some(num_chunks) = blob_store.get_blob_info(hash) {
                                if let Err(e) = swarm
                                    .behaviour_mut()
                                    .blob_transfer
                                        .send_response(
                                            channel,
                                            blob_transfer::Response::Info(blob_transfer::BlobInfo {
                                                hash: hash,
                                                num_chunks: num_chunks,
                                            })
                                        )
                                {
                                    warn!("Failed to send back blob info: `{e:?}`");
                                }
                            }
                        },

                        blob_transfer::Request::GetChunk(blob_hash, req_chunk_index) => {
                            if let Some((data, chunk_hash)) = blob_store.get_chunk(
                                blob_hash,
                                req_chunk_index
                            ) {
                                if let Err(e) = swarm
                                    .behaviour_mut()
                                    .blob_transfer
                                        .send_response(
                                            channel,
                                            blob_transfer::Response::Chunk(
                                                blob_transfer::BlobChunk {
                                                    blob_hash: blob_hash,
                                                    index: req_chunk_index,
                                                    data: data,
                                                    chunk_hash: chunk_hash,
                                                }
                                            )
                                        )
                                {
                                    warn!("Failed to send back the blob chunk: `{e:?}`");
                                }
                            }
                        },
                    }
                },

                // blob transfer responses
                SwarmEvent::Behaviour(MyBehaviourEvent::BlobTransfer(request_response::Event::Message {
                    peer: peer_id,
                    message: request_response::Message::Response {
                        response,
                        //response_id,
                        ..
                    }
                })) => {                
                    match response {
                        blob_transfer::Response::Info(blob_info) => {
                            if pending_jobs
                                .values_mut()
                                .any(|j| j.is_a_prerequisite(blob_info.hash))
                            {    
                                blob_store.add_blob_info(blob_info.hash, blob_info.num_chunks);
                                //@ check if the blob is incomplete
                                // request first chunk
                                let _req_id = swarm
                                    .behaviour_mut()
                                    .blob_transfer
                                    .send_request(
                                        &peer_id,
                                        blob_transfer::Request::GetChunk(blob_info.hash, 0)
                                    );
                                info!(
                                    "Requested the first chunk of the blob(`{}`) from `{}`",
                                    blob_info.hash,
                                    peer_id
                                );                                
                            } else {
                                warn!("Received unsolicited blob info: `{blob_info:?}`");
                            }
                        },

                        blob_transfer::Response::Chunk(blob_chunk) => {
                            let mut ready_job = None;
                            if let Some(job) = pending_jobs
                                .values_mut()
                                .find(|j| j.is_a_prerequisite(blob_chunk.blob_hash))
                            {
                                //@ assumed owner === chunk sender
                                blob_store.add_blob_chunk(
                                    blob_chunk.blob_hash,
                                    blob_chunk.index,
                                    blob_chunk.data,
                                    blob_chunk.chunk_hash
                                );
                                if blob_store.is_blob_complete(blob_chunk.blob_hash) {
                                    job.set_prerequisite_as_fulfilled(blob_chunk.blob_hash);
                                }
                                if job.is_ready() {
                                    let prove_id = match job.kind {
                                        zkvm::JobKind::SP1(_, id) => id,                                        
                                    };
                                    ready_job = Some(prove_id);
                                } else {
                                    // request next chunk
                                    if let Some(next_chunk_index) = blob_store.get_next_blob_chunk_index(blob_chunk.blob_hash) {
                                        let _req_id = swarm
                                            .behaviour_mut()
                                            .blob_transfer
                                            .send_request(
                                                &peer_id,
                                                blob_transfer::Request::GetChunk(blob_chunk.blob_hash, next_chunk_index)
                                            );
                                    }
                                    // info!(
                                    //     "Requested next chunk({}) of the blob `{}` from `{}`",
                                    //     next_chunk_index,
                                    //     blob_chunk.blob_hash,
                                    //     peer_id
                                    // );
                                }
                            } else {
                                warn!("Ignored unsolicited blob chunk for `{}`", blob_chunk.blob_hash);
                                continue
                            }
                            // run it
                            if let Some(id) = ready_job {
                                ready_jobs.push_back(pending_jobs.remove(&id).unwrap());
                                info!("Job's prerequisites are fullfilled and is ready to run.");
                                if active_job.is_none() {
                                    // run it
                                    if let Some(job) = ready_jobs.pop_front() {
                                        let mut inputs = Vec::new();
                                        for blob_hash in job.prerequisites().iter() {
                                            if let Some(blob) = blob_store.get_blob(*blob_hash) {
                                                inputs.push(blob);
                                            } else {
                                                warn!("Input is not available, job cannot start.");
                                            }
                                        }
                                        info!("Starting job(`{}`)...", job.get_batch_id());
                                        run_futures.push(
                                            spawn_run(
                                                Arc::clone(&sp1_cuda_handle),
                                                inputs,
                                                job.kind.clone()
                                            )
                                        );                                        
                                        active_job = Some(job);
                                    }
                                }
                            }
                        }
                    }
                },

                _ => {
                    // println!("{:#?}", event)
                },
            },

            result = run_futures.select_next_some() => {                
                if let Err(e) = result {
                    let job: Job = active_job.take().unwrap();
                    warn!("Failed to run job(`{}`): `{:?}`", job.id, e);
                    continue
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
                                let hash = xxh3_128(&proof);
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
                                blob_store.store(proof);
                            },

                            zkvm::SP1Op::ProveAgg => {
                                info!(
                                    "Agg(`{}`) prove is finished for `{}`",
                                    sub_id,
                                    job.id
                                );
                                let hash = xxh3_128(&proof);
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
                                blob_store.store(proof);
                            }
                        };
                    },
                }                 
                
                // start a new prove
                if let Some(job) = ready_jobs.pop_front() {
                    let mut inputs = Vec::new();
                    for blob_hash in job.prerequisites().iter() {
                        if let Some(blob) = blob_store.get_blob(*blob_hash) {
                            inputs.push(blob);
                        } else {
                            warn!("Input is not available, job cannot start.");
                        }
                    }     
                    info!("Starting job(`{}`)...", job.get_batch_id());
                    run_futures.push(
                        spawn_run(
                            Arc::clone(&sp1_cuda_handle),
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
    info!("Successfully connected to the MongoDB instance.");
    Ok(client)
}

async fn spawn_run(
    sp1_cuda_handle: Arc<SP1CudaHandle>,
    inputs: Vec<Vec<u8>>,
    kind: zkvm::JobKind
) -> anyhow::Result<Vec<u8>>{
    let (tx, rx) = mpsc::channel();   
    let handle = thread::spawn(move || { 
            let r = match kind {                
                zkvm::JobKind::SP1(ref op, _batch_id) => {
                    match op {
                        zkvm::SP1Op::ProveSubblock => {
                            let sp1_cuda_handle = Arc::clone(&sp1_cuda_handle);
                            let stdin = inputs.into_iter().next().unwrap();                            
                            sp1_cuda_handle.prove_subblock(stdin)                            
                        },

                        zkvm::SP1Op::ProveAgg => {
                            let mut iter = inputs.into_iter();
                            let stdin = iter.next().unwrap();
                            let subblock_proofs = iter.collect();
                            sp1_cuda_handle.prove_agg(
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