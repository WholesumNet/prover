#![doc = include_str!("../README.md")]

use futures::{future::Either, prelude::*, select};
use libp2p::{
    core::{muxing::StreamMuxerBox, transport::OrTransport, upgrade},
    gossipsub, identity, mdns, noise, request_response,
    swarm::NetworkBehaviour,
    swarm::{StreamProtocol, SwarmBuilder, SwarmEvent},
    tcp, yamux, PeerId, Transport,
};
use libp2p_quic as quic;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::time::Duration;
use clap::Parser;
use uuid::Uuid;
use comms::{notice, compute};

mod job;

// CLI
#[derive(Parser, Debug)]
#[command(name = "Server CLI for Wholesum: p2p verifiable computing marketplace.")]
#[command(author = "Wholesum team")]
#[command(version = "1.0")]
#[command(about = "Yet another verifiable compute marketplace.", long_about = None)]
struct Cli {
}

// combine Gossipsub, mDNS, and RequestResponse
#[derive(NetworkBehaviour)]
struct MyBehaviour {
    req_resp: request_response::cbor::Behaviour<notice::Request, notice::Response>,
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::async_io::Behaviour,
}

#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
    
    let cli = Cli::parse();
    
    // running jobs(docker containers)
    let mut docker_job_stream = job::DockerJob::new();
    let mut running_jobs = HashMap::<String, job::JobInfo>::new();
    // jobs awaiting verification
    let mut to_be_verified_jobs = HashMap::<String, job::JobInfo>::new();
    // jobs waiting to be collected
    let mut to_be_collected_jobs = HashMap::<String, job::JobInfo>::new();
    // zombie jobs

    // get a random peer_id
    let id_keys = identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(id_keys.public());
    println!("PeerId: {local_peer_id}");
    // setup an encrypted dns-enabled transport over yamux
    let tcp_transport = tcp::async_io::Transport::new(tcp::Config::default().nodelay(true))
        .upgrade(upgrade::Version::V1Lazy)
        .authenticate(noise::Config::new(&id_keys).expect("signing libp2p static keypair"))
        .multiplex(yamux::Config::default())
        .timeout(std::time::Duration::from_secs(30))
        .boxed();
    let quic_transport = quic::async_std::Transport::new(quic::Config::new(&id_keys));
    let transport = OrTransport::new(quic_transport, tcp_transport)
        .map(|either_output, _| match either_output {
            Either::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
            Either::Right((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
        })
        .boxed();

    // to content-address message, take the hash of message and use it as an id
    let message_id_fn = |message: &gossipsub::Message| {
        let mut s = DefaultHasher::new();
        message.data.hash(&mut s);
        gossipsub::MessageId::from(s.finish().to_string())
    };

    // set a custom Gossipsub configuration
    let gossipsub_config = gossipsub::ConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(10)) // aid debugging by not cluttering log space
        .validation_mode(gossipsub::ValidationMode::Strict) // enforce message signing
        .message_id_fn(message_id_fn) // content-address messages
        .build()
        .expect("Invalid gossipsub config.");

    // build a Gossipsub network behaviour
    let mut gossipsub = gossipsub::Behaviour::new(
        gossipsub::MessageAuthenticity::Signed(id_keys),
        gossipsub_config,
    )
    .expect("Invalid behaviour configuration.");

    // subscribe to our topic
    const TOPIC_OF_INTEREST: &str = "<-- Compute Bazaar -->";
    println!("topic of interest: `{TOPIC_OF_INTEREST}`");
    // @ use topic_hash config for auto hash(topic)
    let topic = gossipsub::IdentTopic::new(TOPIC_OF_INTEREST);
    let _ = gossipsub.subscribe(&topic);

    // create a swarm to manage events and peers
    let mut swarm = {
        let mdns = mdns::async_io::Behaviour::new(mdns::Config::default(), local_peer_id)?;
        let req_resp = request_response::cbor::Behaviour::<notice::Request, notice::Response>::new(
            [(
                StreamProtocol::new("/p2pcompute"),
                request_response::ProtocolSupport::Full,
            )],
            request_response::Config::default(),
        );
        let behaviour = MyBehaviour {
            req_resp: req_resp,
            gossipsub: gossipsub,
            mdns: mdns,
        };
        SwarmBuilder::with_async_std_executor(transport, behaviour, local_peer_id).build()
    };

    // read full lines from stdin
    // let mut input = io::BufReader::new(io::stdin()).lines().fuse();

    // listen on all interfaces and whatever port the os assigns
    swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    
    // kick it off
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
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discovered a new peer: {peer_id}");
                        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                    }
                },

                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discovered peer has expired: {peer_id}");
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                    }
                },

                SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source: peer_id,
                    message,
                    ..
                })) => {
                    // let msg_str = String::from_utf8_lossy(&message.data);
                    // println!("Got message: '{}' with id: {id} from peer: {peer_id}",
                    //          msg_str);
                    println!("received gossip message: {:#?}", message);
                    // first byte is message identifier                
                    let notice_req = notice::Notice::try_from(message.data[0])?;
                    match notice_req {
                        notice::Notice::Compute => {
                            println!("`need compute` request from client: `{peer_id}`");
                            // engage with the client through a direct p2p channel
                            // and express interest in getting the compute done
                            let offer = compute::Offer {
                                price: 1,
                                hw_specs: compute::ServerSpecs {
                                    gflops: 100,
                                    ram_amount: 16_000,
                                    cpu_model: "core i7-5500u".to_string(),
                                },
                            };
                            let sw_req_id = swarm
                                .behaviour_mut().req_resp
                                .send_request(
                                    &peer_id,
                                    notice::Request::Compute(offer),
                                );
                            println!("compute offer was sent to client, id: {sw_req_id}");
                        },

                        notice::Notice::Verify => {                            
                            println!("`need verify` request from client: `{peer_id}`");
                            // engage with the client through a direct p2p channel
                            let sw_req_id = swarm
                                .behaviour_mut().req_resp
                                .send_request(
                                    &peer_id,
                                    notice::Request::Verify,
                                );
                            println!("verification offer was sent, id: {sw_req_id}");
                        },

                        notice::Notice::JobStatus => {
                            // job status inquiry 
                            // bytes [1-16] determine th job id 
                            let bytes_id = match message.data[1..=17].try_into() {
                                Ok(b) => b,
                                Err(e) => {
                                    println!("Invalid job id, {e:?}");
                                    continue;
                                },
                            };
                            let job_id = Uuid::from_bytes(bytes_id).to_string();
                            println!("`job-status` request from client: `{}`, job_id: `{}`",
                                peer_id, job_id);
                            let job_status;
                            // this is a stronger check that the `running_jobs` key existance
                            if docker_job_stream.is_job_running(&job_id) {
                                job_status = compute::JobStatus::Running;
                            } else {
                                if to_be_verified_jobs.contains_key(&job_id) {
                                    // awaits verification
                                    job_status = compute::JobStatus::ToBeVerified;

                                } else if to_be_collected_jobs.contains_key(&job_id) {
                                    // awaits collection
                                    job_status = compute::JobStatus::ToBeCollected;

                                } else {
                                    // unknown
                                    job_status = compute::JobStatus::Unknown;
                                }
                            }
                            let sw_req_id = swarm
                                .behaviour_mut().req_resp
                                .send_request(
                                    &peer_id,
                                    notice::Request::Verify,
                                );
                            println!("Job result was sent to the client. req_id: `{sw_req_id}`");
                        }
                    };
                },

                // incoming compute/verify request(interest actually)
                SwarmEvent::Behaviour(MyBehaviourEvent::ReqResp(request_response::Event::Message {
                    peer: sender_peer_id,
                    message: request_response::Message::Request {
                        //request,
                        channel,
                        request_id,
                        ..
                    }
                })) => {                
                    println!("request from: `{}`, req_id: {:#?}, chan: {:#?}",
                        sender_peer_id, request_id, channel);                    
                },

                // incoming response to an earlier compute/verify offer
                SwarmEvent::Behaviour(MyBehaviourEvent::ReqResp(request_response::Event::Message{
                    peer: peer_id,
                    message: request_response::Message::Response {
                        response,
                        ..
                    }
                })) => {                    
                    match response {
                        notice::Response::DeclineOffer => {
                            println!("Offer decliend by the client: `{peer_id}`");
                        },                        

                        notice::Response::Compute(compute_job) => {
                            let mut spawn_error = |e: String| {
                                println!("{e}");
                                // notify client
                                let sw_req_id = swarm
                                    .behaviour_mut().req_resp
                                    .send_request(
                                        &peer_id,
                                        notice::Request::JobResult(
                                            compute::JobResult {
                                                id: compute_job.id.clone(),
                                                status: compute::JobStatus::FinishedWithError(e),
                                            }
                                        ),
                                    );
                                println!("Client has been notified about this failure. req_id: {sw_req_id}");
                            };

                            println!("recived `compute job` request from client: {}, job: {:#?}",
                                peer_id, compute_job);                           
                            // no duplicate jobs are allowed
                            if running_jobs.contains_key(&compute_job.id) {
                                spawn_error("Duplicate job id.".to_string());
                                continue;
                            }
                            // schedule the job to run                            
                            if let Err(e) = docker_job_stream.add_job(
                                compute_job.id.clone(),
                                compute_job.details.docker_image.clone(),
                                compute_job.details.command.clone()) {

                                spawn_error(format!("Job spawn error: `{e:?}`"));
                                continue;
                            }
                            // keep track of running jobs
                            running_jobs.insert(
                                compute_job.id,
                                job::JobInfo { 
                                    owner: peer_id,
                                    job_details: compute_job.details,
                                }
                            );
                        },

                        notice::Response::Verify => (),
                    }

                },

                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Local node is listening on {address}");
                },

                _ => {}

            },
            // docker job is finished, get in touch with the client
            mut job_handle = docker_job_stream.select_next_some() => {
                println!("Docker job `{}` has been finished.", job_handle.id);
                //@ collect any relevant objects before terminating process
                let exit_code = match job_handle.child.status().await {
                    Ok(status) => status.code().unwrap_or_else(
                        || {
                            println!("Docker process terminated by signal.");
                            101 // @ retrieve signal
                        }
                    ),
                    Err(e) => {
                        println!("Failed to collect docker process's exit status: {e:?}");
                        102
                    }
                };

                //@ stdout/stderr can be quite large so how about moving them into streams? 
                // collect stdout and stderr
                let mut stdout_buffer = String::new();
                if let Some(mut stdout) = job_handle.child.stdout {
                    //@ can even ignore errors :)
                    if let Err(e) = stdout.read_to_string(&mut stdout_buffer).await {
                        stdout_buffer = format!("Failed to capture stdout: {e:?}");
                        // println!("stdout: `{stdout_buffer}`");
                    }
                }
                let mut stderr_buffer = String::new();
                if let Some(mut stderr) = job_handle.child.stderr {
                    if let Err(e) = stderr.read_to_string(&mut stderr_buffer).await {
                        stderr_buffer = format!("Failed to capture stderr: {e:?}");
                        // println!("stderr: `{stderr_buffer}`");
                    }
                }
                println!("docker container execution is terminated. \
                    exit_code: `{}`\n, stderr: `{}`\n, stdout: `{}`",
                    exit_code, stderr_buffer, stdout_buffer); 
                // job is ready to be verified now
                // notify client that the job has been executed
                if false == running_jobs.contains_key(&job_handle.id) {
                    println!("Critical error: container's job info is missing.");
                    //@ what to do with zombie jobs?
                    continue;
                }
                let job_info = running_jobs.remove(&job_handle.id).unwrap();

                let job_result;
                if exit_code == 0 {
                    job_result = compute::JobResult {
                        id: job_handle.id,
                        status: compute::JobStatus::ToBeVerified,
                    };
                } else {
                    job_result = compute::JobResult {
                        id: job_handle.id,
                        status: compute::JobStatus::FinishedWithError("".to_string()), //@ report stderr, ...
                    };                    
                }
                // running_jobs.retain(|&k, _| k != job_handle.id);
                let sw_req_id = swarm
                    .behaviour_mut().req_resp
                    .send_request(
                        &job_info.owner,
                        notice::Request::JobResult(job_result),
                    );
                println!("Notified the client: `{}` about the job. req-id: {}",
                    job_info.owner, sw_req_id);
            },
        }
    }
}
