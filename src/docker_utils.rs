use std::process::{Stdio, Command};
use std::io::Read;
// use std::os::unix::process::CommandExt;

pub fn run_docker_image (image: String, command: String) -> (i32, String, String) {
    let mut exit_code = -1;
    let mut cmd_args = vec!["run", image.as_str()];
    cmd_args.extend(command.split(' '));
    println!("running docker with command `{:?}`", cmd_args);

    let mut child = Command::new("docker")
        // .current_dir(dir)
        // let cmd_args = vec!["run", "test-risc0", "sh", "-c", "/root/risc0-0.17.0/examples/target/release/factors"];
        .args(cmd_args)
        .stdin(Stdio::null())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn().expect("failed to start docker.");
    let exit_status = child.wait().expect("Failed to collect docker status.");                            
    if let Some(ecode) = exit_status.code() {
        exit_code = ecode;         
    } else {
        println!("docker process terminated by signal.");
    }
    let mut stderr_buffer = String::new();
    if let Some(mut stderr_pipe) = child.stderr {
        let _ = stderr_pipe.read_to_string(&mut stderr_buffer);
    }    
    let mut stdout_buffer = String::new();
    if let Some(mut stdout_pipe) = child.stdout {
        let _ = stdout_pipe.read_to_string(&mut stdout_buffer);
    } 

    (
        exit_code,
        stderr_buffer,
        stdout_buffer,
    )    
}