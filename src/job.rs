use std::error::Error;
use futures_util::{
    TryStreamExt
};
// use std::fs;
use libp2p::PeerId;
use home;
use bollard::{
    Docker,
    models::{
        // CreateImageInfo,
        // ContainerCreateResponse,
        HostConfig,
        // ContainerWaitResponse,
        // ContainerWaitExitError,
    },
    image::{
        CreateImageOptions,
    },
    container::{
        CreateContainerOptions,
        Config, 
        StartContainerOptions,
        WaitContainerOptions,
    },
};

#[derive(Debug)]
pub struct Residue {
    pub fd12_cid: Option<String>,
    pub receipt_cid: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Status {
    DockerWarmingUp = 0,    // docker initializing
    Negotiating,            // making offers
    Running,
    ExecutionSucceeded,     // and ready to be verified or to be harvested
    ExecutionFailed,
}

// maintain lifecycle of a job
#[derive(Debug)]
pub struct Job {
    pub id: String,
    pub owner: PeerId,                      // the client
    pub status: Status,
    pub residue: Residue,                   // cids for stderr, output, receipt, ...
}

// get base residue path of the host
pub fn get_residue_path() -> Result<String, Box<dyn Error>> {
    let home_dir = home::home_dir()
        .ok_or_else(|| Box::<dyn Error>::from("Home dir is not available."))?
        .into_os_string().into_string()
        .or_else(|_| Err(Box::<dyn Error>::from("OS_String conversion failed.")))?;
    Ok(format!("{home_dir}/.wholesum/jobs"))
}

// import docker image
pub async fn import_docker_image<'a>(
    docker_con: &'a Docker,
    image: &'a String
) -> Result<(), Box<dyn Error>> {
    println!("Importing `{}`", image);
    let mut import_response_stream = docker_con
        .create_image(
            Some(CreateImageOptions{
                from_image: image.clone(),
                ..Default::default()
            }),
            None,
            None
        );
    let mut digest;
    let mut image_size = -1f64;
    let mut image_size_read = false;
    let (mut tp25, mut tp50, mut tp75) = (false, false, false);
    while let Some(create_image_info) = import_response_stream.try_next().await? {
        // look up digest 
        if let Some(status) = create_image_info.status {
            if true == status.starts_with("Digest:") {
                (_, digest) = status.split_at(8);
                println!("Image `{}`, digest: `{}`", image, digest);
            }
        }
        // track progress(pick only quarters: 0%, 25%, 50%, and 75%)
        if let Some(progress_detail) = create_image_info.progress_detail {
            let (current, total) = (
                progress_detail.current.unwrap_or_else(|| 0i64) as f64,
                progress_detail.total.unwrap_or_else(|| 0i64) as f64
            );
            if total > 0f64 {
                let progress = current / total;
                if false == image_size_read {
                    image_size = total / 1_048_576f64;
                    println!("Image `{image}`, download started, total size: `{image_size:.2} MB`");
                    image_size_read = true;
                }
                if progress > 0.25 && progress < 0.5 {
                    if tp25 == false {
                        println!("Image `{}`, downloaded `{:.2}/{:.2} MB` ~25%",
                            image,
                            current / 1_048_576f64,
                            image_size
                        );
                        tp25 = true;
                    }
                } else if progress > 0.5 && progress < 0.75 {
                    if tp50 == false {
                        println!("Image `{}`, downloaded `{:.2}/{:.2} MB` ~50%",
                            image,
                            current / 1_048_576f64,
                            image_size
                        );
                        tp50 = true;
                    }
                } else if progress > 0.75 {
                    if tp75 == false {
                        println!("Image `{}`, downloaded `{:.2}/{:.2} MB` ~75%",
                            image,
                            current / 1_048_576f64,
                            image_size
                        );
                        tp75 = true;
                    }
                } 
            }
        }        
    }

    println!("Image `{}` has been imported.", image);
    Ok(())
}

#[derive(Debug)]
pub struct JobExecutionResult {
    pub job_id: String,
    pub exit_status_code: i64,
    pub error_message: Option<String>,
}

pub async fn run_docker_job<'a>(
    docker_con: &'a Docker,
    job_id: String,
    image: String,
    command: Vec<String>,
    src_volume: String,
) -> Result<JobExecutionResult, Box<dyn Error + 'static>> {
    // 1- import the docker image
    import_docker_image(docker_con, &image).await?;
    // 2- create and start the container
    let volume = format!("{}:{}",
        src_volume,
        "/home/prince/residue");
    let create_container_resp = docker_con
        .create_container(
            Some(CreateContainerOptions{
                name: job_id.clone(),
                platform: Some(String::from("linux/amd64")),
            }),
            Config {
                image: Some(image.clone()),
                cmd: Some(command.clone()),
                // user: Some(String::from("prince")),   
                host_config: Some(HostConfig{
                    binds: Some(vec![volume]),
                    auto_remove: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            }
        )
        .await?;
    println!("Container created successfully, id: `{}`", create_container_resp.id);
    if create_container_resp.warnings.len() > 0 {
        println!("Though warnings were raised: {:#?}", create_container_resp.warnings)
    }
    docker_con
        .start_container(
            create_container_resp.id.as_str(),
            None::<StartContainerOptions<String>>,
        )
        .await?;
    println!("Container `{}` has been started.", job_id);
    // 3- wait for the container to finish
    let mut container_wait_response_stream = docker_con
        .wait_container(
            job_id.as_str(),
            None::<WaitContainerOptions<String>>,
        );
    let mut exec_result =  JobExecutionResult {
        job_id: job_id,
        exit_status_code: -1,
        error_message: None,
    };
    while let Some(container_wait_response) = container_wait_response_stream.try_next().await? {
        println!("{container_wait_response:#?}");
        exec_result.exit_status_code = container_wait_response.status_code;
        if let Some(wait_error) = container_wait_response.error {
            exec_result.error_message = wait_error.message;
        }
    }
    
    Ok(exec_result)
}