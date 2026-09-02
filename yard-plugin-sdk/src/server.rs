//! Plugin protocol server -- entry point for provider plugin binaries.
//!
//! [`PluginServer::run`] implements the full protocol lifecycle:
//!
//! 1. Capture stdout via [`crate::stdout::ProtocolWriter`] (fd redirect)
//! 2. Initialize `tracing` subscriber on stderr
//! 3. Write [`HandshakeMessage`] to the protocol channel
//! 4. Read a [`PluginRequest`] from stdin
//! 5. Dispatch to the appropriate [`PluginHandler`] method
//! 6. Write the response to the protocol channel
//! 7. Exit

use std::io::BufRead;

use tracing_subscriber::EnvFilter;
use yard_structs::{HandshakeMessage, PluginOperation, PluginRequest, PROTOCOL_VERSION};

use crate::handler::PluginHandler;
use crate::stdout::ProtocolWriter;

/// Entry point for plugin binaries.
///
/// Handles the full protocol lifecycle: stdout capture, tracing
/// initialization, handshake, request parsing, dispatch, and response
/// serialization.
pub struct PluginServer;

impl PluginServer {
    /// Run the plugin protocol loop.
    ///
    /// This function never returns -- it calls [`std::process::exit`]
    /// after writing the response (exit 0) or on error (exit 1).
    ///
    /// # Protocol sequence
    ///
    /// 1. Capture stdout (redirect fd 1 to stderr via `dup2`)
    /// 2. Initialize tracing subscriber writing to stderr
    /// 3. Write handshake message to protocol channel
    /// 4. Read one request line from stdin
    /// 5. Dispatch to handler method based on operation
    /// 6. Write response to protocol channel and exit 0
    /// 7. On error: log to stderr and exit 1
    pub fn run(handler: impl PluginHandler) -> ! {
        // Step 1: Capture stdout before anything else writes to fd 1.
        let mut writer = match ProtocolWriter::capture() {
            Ok(w) => w,
            Err(e) => {
                eprintln!("fatal: failed to capture stdout: {e:#}");
                std::process::exit(1);
            }
        };

        // Step 2: Initialize tracing on stderr (D-09).
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .init();

        // Step 3: Write handshake (D-02, D-03).
        let handshake = HandshakeMessage {
            protocol_version: PROTOCOL_VERSION,
            name: handler.name().to_string(),
            version: handler.version().to_string(),
            capabilities: vec![
                PluginOperation::Validate,
                PluginOperation::Codegen,
                PluginOperation::Deploy,
                PluginOperation::Destroy,
                PluginOperation::Verify,
                PluginOperation::Schema,
            ],
        };
        let handshake_json =
            serde_json::to_string(&handshake).unwrap_or_else(|e| {
                eprintln!("fatal: failed to serialize handshake: {e:#}");
                std::process::exit(1);
            });
        if let Err(e) = writer.write_line(&handshake_json) {
            eprintln!("fatal: failed to write handshake: {e:#}");
            std::process::exit(1);
        }

        // Step 4: Read one request line from stdin.
        let stdin = std::io::stdin();
        let mut line = String::new();
        if let Err(e) = stdin.lock().read_line(&mut line) {
            eprintln!("fatal: failed to read request from stdin: {e:#}");
            std::process::exit(1);
        }

        // Step 5: Deserialize request.
        let request: PluginRequest = match serde_json::from_str(line.trim()) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("fatal: failed to parse request JSON: {e:#}");
                std::process::exit(1);
            }
        };

        // Step 6: Dispatch to handler.
        match dispatch(&handler, &request) {
            Ok(response_json) => {
                // Step 7a: Write response and exit 0.
                if let Err(e) = writer.write_line(&response_json) {
                    eprintln!("fatal: failed to write response: {e:#}");
                    std::process::exit(1);
                }
                std::process::exit(0);
            }
            Err(e) => {
                // Step 7b: Log error chain and exit 1 (D-08).
                eprintln!("error: {e:#}");
                std::process::exit(1);
            }
        }
    }
}

/// Dispatch a request to the appropriate handler method and serialize
/// the response to JSON.
fn dispatch(handler: &impl PluginHandler, request: &PluginRequest) -> anyhow::Result<String> {
    match request.operation {
        PluginOperation::Validate => {
            let job_name = request
                .job_name
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("validate requires job_name"))?;
            let job_config = request
                .job_config
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("validate requires job_config"))?;
            let resp = handler.validate(job_name, job_config)?;
            Ok(serde_json::to_string(&resp)?)
        }
        PluginOperation::Codegen => {
            let job_name = request
                .job_name
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("codegen requires job_name"))?;
            let job_config = request
                .job_config
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("codegen requires job_config"))?;
            let resp = handler.codegen(job_name, job_config)?;
            Ok(serde_json::to_string(&resp)?)
        }
        PluginOperation::Deploy => {
            let job_name = request
                .job_name
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("deploy requires job_name"))?;
            let job_config = request
                .job_config
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("deploy requires job_config"))?;
            let artifact = request
                .artifact
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("deploy requires artifact"))?;
            let resp = handler.deploy(job_name, job_config, artifact)?;
            Ok(serde_json::to_string(&resp)?)
        }
        PluginOperation::Destroy => {
            let job_name = request
                .job_name
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("destroy requires job_name"))?;
            let resources = request
                .resources
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("destroy requires resources"))?;
            let resp = handler.destroy(job_name, resources)?;
            Ok(serde_json::to_string(&resp)?)
        }
        PluginOperation::Verify => {
            let job_name = request
                .job_name
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("verify requires job_name"))?;
            let resources = request
                .resources
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("verify requires resources"))?;
            let resp = handler.verify(job_name, resources)?;
            Ok(serde_json::to_string(&resp)?)
        }
        PluginOperation::Schema => {
            let resp = handler.schema()?;
            Ok(serde_json::to_string(&resp)?)
        }
    }
}
