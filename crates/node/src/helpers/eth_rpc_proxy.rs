use std::{fs::File, path::Path, time::Duration};

use super::{Process, ProcessReadinessWaitBehavior};

/// Spawns an eth-rpc proxy process with the given configuration.
///
/// This encapsulates the common eth-rpc proxy spawning logic used by Substrate,
/// Polkadot Omni Node, and Zombienet node implementations.
#[allow(clippy::too_many_arguments)]
pub fn spawn_eth_rpc_proxy(
    log_file_prefix: &'static str,
    logs_directory: &Path,
    binary_path: &Path,
    rpc_port: u16,
    node_rpc_url: &str,
    logging_level: &str,
    cached_blocks: u32,
    extra_args: &[(&str, &str)],
    ready_marker: &str,
) -> anyhow::Result<Process> {
    let ready_marker = ready_marker.to_owned();

    Process::new(
        log_file_prefix,
        logs_directory,
        binary_path,
        |command, stdout_file: File, stderr_file: File| {
            command
                .arg("--dev")
                .arg("--node-rpc-url")
                .arg(node_rpc_url)
                .arg("--rpc-port")
                .arg(rpc_port.to_string())
                .arg("--rpc-max-connections")
                .arg(u32::MAX.to_string())
                .arg("--index-last-n-blocks")
                .arg(cached_blocks.to_string())
                .arg("--cache-size")
                .arg(cached_blocks.to_string())
                .env("RUST_LOG", logging_level);

            for (key, value) in extra_args {
                command.arg(*key).arg(*value);
            }

            command.stdout(stdout_file).stderr(stderr_file);
        },
        ProcessReadinessWaitBehavior::TimeBoundedWaitFunction {
            max_wait_duration: Duration::from_secs(30),
            check_function: Box::new(move |_, stderr_line| match stderr_line {
                Some(line) => Ok(line.contains(ready_marker.as_str())),
                None => Ok(false),
            }),
        },
    )
}
