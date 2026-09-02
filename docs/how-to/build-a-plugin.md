<!-- generated-by: gsd-doc-writer -->
# Build a provider plugin

This guide is for developers building provider plugins for yard. A plugin is a standalone binary that yard spawns as a child process to perform provider-specific operations (validate, codegen, deploy, destroy, verify, schema) over a JSON-over-stdio protocol.

Two paths are covered:

1. **Rust SDK** (recommended) -- use the `yard-plugin-sdk` crate, which handles all protocol mechanics. You implement business logic only.
2. **JSON protocol** -- build a plugin in any language by implementing the raw stdio protocol.

## Part 1: Rust SDK tutorial

### Step 1: Create a new project

```bash
cargo init --name yard-plugin-example
cd yard-plugin-example
```

### Step 2: Add the SDK dependency

Add `yard-plugin-sdk` to your `Cargo.toml`:

```toml
[dependencies]
yard-plugin-sdk = { version = "0.1" }
```

The SDK re-exports everything you need -- `PluginHandler`, `PluginServer`, all response types, `serde_json::Value`, `anyhow`, and `tracing`. No direct `yard-structs` dependency is required.

### Step 3: Implement PluginHandler

The `PluginHandler` trait has 8 required methods. There are no default implementations -- every method must be provided, even if some are trivial pass-throughs.

Create `src/main.rs`:

```rust
use yard_plugin_sdk::{
    PluginHandler, PluginServer,
    CodegenResponse, DeployResponse, DestroyResponse,
    Resource, SchemaResponse, SchemaField,
    ValidateResponse, VerifyResponse,
};

struct ExampleProvider;

impl PluginHandler for ExampleProvider {
    fn name(&self) -> &str {
        "yard-plugin-example"
    }

    fn version(&self) -> &str {
        "0.1.0"
    }

    fn validate(
        &self,
        _job_name: &str,
        _job_config: &serde_json::Value,
    ) -> anyhow::Result<ValidateResponse> {
        // Return validation errors in the response, not as Err.
        // Err is for "validation could not run" (e.g. config parse failure).
        Ok(ValidateResponse { errors: vec![] })
    }

    fn codegen(
        &self,
        job_name: &str,
        _job_config: &serde_json::Value,
    ) -> anyhow::Result<CodegenResponse> {
        let script = format!("# Generated script for {job_name}\nprint('hello')");
        Ok(CodegenResponse { script: Some(script) })
    }

    fn deploy(
        &self,
        _job_name: &str,
        _job_config: &serde_json::Value,
        _artifact: &str,
    ) -> anyhow::Result<DeployResponse> {
        // Return the cloud resources created/updated.
        Ok(DeployResponse { resources: vec![] })
    }

    fn destroy(
        &self,
        _job_name: &str,
        _resources: &[Resource],
    ) -> anyhow::Result<DestroyResponse> {
        Ok(DestroyResponse {})
    }

    fn verify(
        &self,
        _job_name: &str,
        _resources: &[Resource],
    ) -> anyhow::Result<VerifyResponse> {
        Ok(VerifyResponse { statuses: vec![] })
    }

    fn schema(&self) -> anyhow::Result<SchemaResponse> {
        Ok(SchemaResponse {
            fields: vec![
                SchemaField {
                    name: "region".to_string(),
                    field_type: "string".to_string(),
                    required: false,
                    description: "AWS region for the provider".to_string(),
                },
            ],
            supported_source_types: None,
            supported_sink_types: None,
        })
    }
}

fn main() -> ! {
    PluginServer::run(ExampleProvider)
}
```

**What each method does:**

| Method | Purpose | Receives | Returns |
|--------|---------|----------|---------|
| `name()` | Plugin identity for handshake | -- | Plugin name string |
| `version()` | Plugin version for handshake | -- | Semver string |
| `validate()` | Check job config for errors | Job name, job config (JSON) | List of validation errors |
| `codegen()` | Generate deployment script | Job name, job config (JSON) | Script content (or `None`) |
| `deploy()` | Deploy artifact to cloud | Job name, job config, script content | List of created resources |
| `destroy()` | Tear down deployed resources | Job name, list of resources | Success/failure |
| `verify()` | Check if resources still exist | Job name, list of resources | Per-resource status |
| `schema()` | Describe accepted config fields | -- | Field descriptors, supported types |

### Step 4: Build

```bash
cargo build --release
```

The binary is at `target/release/yard-plugin-example`.

### Step 5: Test locally

Copy the binary into a yard project's plugin cache and configure a job to use it:

```bash
# Determine your platform key
# macOS ARM: aarch64-apple-darwin
# macOS Intel: x86_64-apple-darwin
# Linux x86: x86_64-unknown-linux-gnu
# Linux ARM: aarch64-unknown-linux-gnu

mkdir -p .yard/plugins
cp target/release/yard-plugin-example \
   .yard/plugins/yard-plugin-example-0.1.0-aarch64-apple-darwin
```

Create a test job file referencing the plugin:

```yaml
# test-job.yaml
type: example
plugin_version: "0.1.0"
plugin_source: "file:///path/to/yard-plugin-example-${version}-${os}-${arch}"
sources:
  - name: input
    source_type: s3
    location: s3://test-bucket/input/
    format: parquet
sink:
  sink_type: s3
  format: parquet
  path: s3://test-bucket/output/
  mode: overwrite
```

Run `yard plan` to verify the plugin is discovered and called correctly.

### Step 6: Logging

The SDK captures stdout at the file descriptor level -- any `println!()` in your code or dependencies is automatically redirected to stderr. The protocol channel (stdout) stays clean.

For structured logging, use `tracing` (re-exported by the SDK):

```rust
use yard_plugin_sdk::tracing;

fn deploy(&self, job_name: &str, ...) -> anyhow::Result<DeployResponse> {
    tracing::info!("deploying {job_name}");
    // ...
}
```

The SDK auto-initializes a stderr tracing subscriber with `RUST_LOG` env-filter support. No setup required.

### Step 7: Release to GitHub

Create a GitHub release with a tag matching your version (e.g. `v0.1.0`). Upload platform-specific binaries following the naming convention:

```
yard-plugin-example-0.1.0-aarch64-apple-darwin
yard-plugin-example-0.1.0-x86_64-apple-darwin
yard-plugin-example-0.1.0-x86_64-unknown-linux-gnu
yard-plugin-example-0.1.0-aarch64-unknown-linux-gnu
```

The general pattern is `{name}-{version}-{os}-{arch}` where:
- `{name}` is the plugin binary name (e.g. `yard-plugin-example`)
- `{version}` is the semver version (e.g. `0.1.0`)
- `{os}` is one of `apple-darwin` or `unknown-linux-gnu`
- `{arch}` is one of `aarch64` or `x86_64`

Users reference your release in their `job.yaml`:

```yaml
plugin_version: "0.1.0"
plugin_source: "https://github.com/your-org/yard-plugin-example/releases/download/v${version}/yard-plugin-example-${version}-${os}-${arch}"
```

### TOFU checksum model

yard uses a trust-on-first-use (TOFU) checksum model for plugin binaries:

1. **First download:** yard downloads the binary, computes its SHA-256 checksum, and records it in `yard.lock` at the project root.
2. **Subsequent runs:** yard verifies the cached binary's checksum against `yard.lock`. A mismatch aborts the operation.
3. **Version bumps:** changing `plugin_version` in `job.yaml` triggers a re-download. The new checksum replaces the old entry in `yard.lock`.

`yard.lock` should be committed to version control. Team members on different platforms accumulate their platform-specific checksums in the same file -- the lock file grows organically.

## Part 2: JSON protocol spec

For developers building plugins in Go, Python, or other languages, here is the raw protocol that the Rust SDK abstracts.

### Protocol flow

```
yard (host)                          plugin (child process)
    |                                       |
    |--- spawn plugin binary -------------->|
    |                                       |
    |<------- handshake line (stdout) ------|  (1)
    |                                       |
    |--- request line (stdin) + close ----->|  (2)
    |                                       |
    |<------- progress lines (stdout) ------|  (3, optional)
    |                                       |
    |<------- response line (stdout) -------|  (4)
    |                                       |
    |         plugin exits                  |  (5)
```

1. Plugin writes a handshake JSON line to stdout.
2. yard writes a request JSON line to stdin, then closes stdin (EOF).
3. Plugin may emit zero or more progress JSON lines to stdout.
4. Plugin writes the response JSON line to stdout.
5. Plugin exits. One process per operation -- no persistent connections.

### Handshake message

Written by the plugin immediately on startup, before reading stdin:

```json
{"protocol_version":1,"name":"yard-plugin-example","version":"0.1.0","capabilities":["validate","codegen","deploy","destroy","verify","schema"]}
```

| Field | Type | Description |
|-------|------|-------------|
| `protocol_version` | integer | Must be `1` (current protocol version) |
| `name` | string | Plugin name |
| `version` | string | Plugin semver version |
| `capabilities` | string[] | Operations the plugin supports. Must include all 6: `validate`, `codegen`, `deploy`, `destroy`, `verify`, `schema` |

### Request message

Written by yard to the plugin's stdin, followed by EOF:

```json
{"operation":"validate","job_name":"my-job","job_config":{...}}
```

| Field | Type | Present for |
|-------|------|-------------|
| `operation` | string | All operations |
| `job_name` | string | validate, codegen, deploy, destroy, verify |
| `job_config` | object | validate, codegen, deploy |
| `resources` | Resource[] | destroy, verify |
| `artifact` | string | deploy |

### Operations and responses

**validate** -- check job config for errors:
```json
{"errors":[{"field":"region","message":"unknown region","severity":"error"}]}
```

**codegen** -- generate deployment script:
```json
{"script":"# generated python script\nprint('hello')"}
```

**deploy** -- deploy artifact to cloud service:
```json
{"resources":[{"resource_type":"s3_object","id":"s3://bucket/key","provider":"example"}]}
```

**destroy** -- tear down resources (empty response on success):
```json
{}
```

**verify** -- check resource existence:
```json
{"statuses":[{"resource_type":"s3_object","id":"s3://bucket/key","exists":true}]}
```

**schema** -- describe accepted config fields:
```json
{"fields":[{"name":"region","field_type":"string","required":false,"description":"AWS region"}],"supported_source_types":null,"supported_sink_types":null}
```

### Progress messages

During long-running operations, the plugin may emit progress lines to stdout before the response:

```json
{"type":"progress","message":"Uploading script...","percent":50}
```

The host uses the `"type":"progress"` discriminator to distinguish progress lines from the operation response.

### Key constraints

- **stdout is the protocol channel.** All logging must go to stderr. Any stray print to stdout corrupts the protocol.
- **Line-delimited JSON.** Each message is one JSON object per newline. No multi-line JSON.
- **One process per operation.** The plugin handles a single request and exits. yard spawns a new process for each operation.
- **Protocol version must match.** The handshake `protocol_version` must equal `1`. A mismatch causes yard to abort with an error.
- **Exit code matters.** Exit 0 on success. Non-zero exit tells yard the operation failed -- yard reads the last lines of stderr for the error message.
