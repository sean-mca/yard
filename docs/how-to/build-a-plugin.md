<!-- generated-by: gsd-doc-writer -->
# Build a provider plugin

This guide is for developers building provider plugins for yard. A plugin is a standalone executable that yard spawns as a child process to perform provider-specific operations (validate, codegen, deploy, destroy, verify, schema) over a JSON-over-stdio protocol.

Part 1 walks through a minimal plugin in two languages side by side:

- **Rust**, using the `yard-plugin-sdk` crate, which handles the protocol mechanics so you implement business logic only.
- **Python**, implementing the protocol directly with the standard library, which shows that no SDK is required.

Part 2 is the raw protocol spec, for building a plugin in any other language.

## Part 1: Build a plugin

### Step 1: Create the project

<!-- tabs:start -->
<!-- tab: Rust -->
**Rust**

```bash
cargo init --name yard-plugin-example
cd yard-plugin-example
```

Add `yard-plugin-sdk` to `Cargo.toml`:

```toml
[dependencies]
yard-plugin-sdk = { version = "2.0" }
```

The SDK re-exports everything you need -- `PluginHandler`, `PluginServer`, all response types, `serde_json::Value`, `anyhow`, and `tracing`. No direct `yard-structs` dependency is required.
<!-- tab: Python -->
**Python**

```bash
mkdir yard-plugin-example
cd yard-plugin-example
touch yard-plugin-example.py
chmod +x yard-plugin-example.py
```

There is no Python SDK; the protocol is small enough to implement with `json` and `sys`. The single script is the plugin executable, so it must start with a `#!/usr/bin/env python3` line.

If your plugin needs third-party packages (a templating library, a cloud SDK), they must be installed for whichever `python3` is on the user's `PATH`. Keep dependencies minimal and document them, along with any credentials, roles, or other resources your plugin expects the user to provide.
<!-- tabs:end -->

### Step 2: Implement the handler

A plugin implements six operations. The Rust SDK expresses them as the `PluginHandler` trait; in Python they are plain functions plus a small dispatch loop that mirrors what `PluginServer::run()` does.

<!-- tabs:start -->
<!-- tab: Rust -->
**Rust**

The `PluginHandler` trait has 8 required methods. There are no default implementations -- every method must be provided, even if some are trivial pass-throughs.

Create `src/main.rs`:

```rust
use yard_plugin_sdk::{
    anyhow, PluginHandler, PluginServer, Value,
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
        _job_config: &Value,
    ) -> anyhow::Result<ValidateResponse> {
        // Return validation errors in the response, not as Err.
        // Err is for "validation could not run" (e.g. config parse failure).
        Ok(ValidateResponse { errors: vec![] })
    }

    fn codegen(
        &self,
        job_name: &str,
        _job_config: &Value,
    ) -> anyhow::Result<CodegenResponse> {
        let script = format!("# Generated script for {job_name}\nprint('hello')");
        Ok(CodegenResponse { script: Some(script) })
    }

    fn deploy(
        &self,
        _job_name: &str,
        _job_config: &Value,
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
<!-- tab: Python -->
**Python**

Each handler returns a plain `dict` that is written to stdout as one JSON line. Raise an exception only when the operation could not run at all; validation problems go in the `errors` list of the response.

Write `yard-plugin-example.py`:

```python
#!/usr/bin/env python3
"""yard-plugin-example: a minimal yard provider plugin."""

import json
import logging
import sys

# All logging goes to stderr. stdout is reserved for protocol JSON.
logging.basicConfig(stream=sys.stderr, level=logging.INFO,
                    format="%(levelname)s: %(message)s")
log = logging.getLogger("yard-plugin-example")

PLUGIN_NAME = "yard-plugin-example"
PLUGIN_VERSION = "0.1.0"
PROTOCOL_VERSION = 1
CAPABILITIES = ["validate", "codegen", "deploy", "destroy", "verify", "schema"]


def handle_validate(job_name, job_config):
    # Return validation errors in the response, not by raising.
    # Raise only when validation could not run (e.g. malformed config).
    return {"errors": []}


def handle_codegen(job_name, job_config):
    script = "# Generated script for {}\nprint('hello')".format(job_name)
    return {"script": script}


def handle_deploy(job_name, job_config, artifact):
    # Return the cloud resources created/updated.
    return {"resources": []}


def handle_destroy(job_name, resources):
    return {}


def handle_verify(job_name, resources):
    return {"statuses": []}


def handle_schema():
    return {
        "fields": [
            {
                "name": "region",
                "field_type": "string",
                "required": False,
                "description": "AWS region for the provider",
            }
        ],
        "supported_source_types": None,
        "supported_sink_types": None,
    }


def dispatch(request):
    op = request.get("operation")
    if op == "validate":
        return handle_validate(request["job_name"], request["job_config"])
    if op == "codegen":
        return handle_codegen(request["job_name"], request["job_config"])
    if op == "deploy":
        return handle_deploy(request["job_name"], request["job_config"],
                             request["artifact"])
    if op == "destroy":
        return handle_destroy(request["job_name"], request["resources"])
    if op == "verify":
        return handle_verify(request["job_name"], request["resources"])
    if op == "schema":
        return handle_schema()
    raise ValueError("unknown operation: {}".format(op))


def main():
    """handshake -> read one request -> dispatch -> write one response -> exit"""
    try:
        handshake = {
            "protocol_version": PROTOCOL_VERSION,
            "name": PLUGIN_NAME,
            "version": PLUGIN_VERSION,
            "capabilities": CAPABILITIES,
        }
        sys.stdout.write(json.dumps(handshake) + "\n")
        sys.stdout.flush()

        line = sys.stdin.readline()
        if not line.strip():
            log.error("no request received on stdin")
            sys.exit(1)

        response = dispatch(json.loads(line))
        sys.stdout.write(json.dumps(response) + "\n")
        sys.stdout.flush()
        sys.exit(0)
    except Exception as exc:
        log.error("%s", exc)
        sys.stdout.write(json.dumps({"error": str(exc)}) + "\n")
        sys.stdout.flush()
        sys.exit(1)


if __name__ == "__main__":
    main()
```
<!-- tabs:end -->

**What each operation does:**

| Operation | Purpose | Receives | Returns |
|-----------|---------|----------|---------|
| `name` / `version` | Plugin identity for the handshake | -- | Plugin name and semver string |
| `validate` | Check job config for errors | Job name, job config (JSON) | List of validation errors |
| `codegen` | Generate deployment script | Job name, job config (JSON) | Script content (or `None` / `null`) |
| `deploy` | Deploy artifact to cloud | Job name, job config, script content | List of created resources |
| `destroy` | Tear down deployed resources | Job name, list of resources | Success/failure |
| `verify` | Check if resources still exist | Job name, list of resources | Per-resource status |
| `schema` | Describe accepted config fields | -- | Field descriptors, supported types |

### Step 3: Build

<!-- tabs:start -->
<!-- tab: Rust -->
**Rust**

```bash
cargo build --release
```

The binary is at `target/release/yard-plugin-example`.
<!-- tab: Python -->
**Python**

There is no build step. The script itself is the plugin executable. Check that it runs and answers a `schema` request:

```bash
echo '{"operation":"schema"}' | ./yard-plugin-example.py
```

You should see two JSON lines on stdout: the handshake, then the schema response.
<!-- tabs:end -->

### Step 4: Test locally

Copy the plugin into a yard project's plugin cache and configure a job to use it.

yard looks for a cached binary at
`.yard/plugins/{name}-{version}-{arch}-{os}` and only downloads when that
file is missing. Pre-place your build there and yard will use it without
ever hitting the network.

The `{arch}-{os}` platform key comes from Rust's `std::env::consts` --
`aarch64-macos`, `x86_64-macos`, `x86_64-linux`, `aarch64-linux`. Print
your own with:

```bash
rustc --print cfg | grep -E 'target_arch|target_os'
# target_arch="aarch64"
# target_os="macos"      ->  platform key: aarch64-macos
```

Note the cache key uses Rust's *consts* spelling (`macos`, `linux`), not the
target-triple spelling (`apple-darwin`, `unknown-linux-gnu`). `rustc -vV`'s
`host:` line shows the triple, so do not copy the platform key from there.
Without a Rust toolchain, `uname -m` gives the architecture (`arm64` means
`aarch64`) and `uname -s` the OS (`Darwin` means `macos`).

<!-- tabs:start -->
<!-- tab: Rust -->
**Rust**

```bash
mkdir -p .yard/plugins
# On Apple Silicon:
cp target/release/yard-plugin-example \
   .yard/plugins/yard-plugin-example-0.1.0-aarch64-macos
```
<!-- tab: Python -->
**Python**

```bash
mkdir -p .yard/plugins
# On Apple Silicon:
cp yard-plugin-example.py \
   .yard/plugins/yard-plugin-example-0.1.0-aarch64-macos
chmod +x .yard/plugins/yard-plugin-example-0.1.0-aarch64-macos
```

The cached file keeps no `.py` extension; the shebang line is what makes it runnable.
<!-- tabs:end -->

Create a test job file referencing the plugin:

```yaml
# test-job.yaml
type: example
plugin_version: "0.1.0"
plugin_source: "https://example.invalid/yard-plugin-example-0.1.0-aarch64-macos"
sources:
  - name: input
    type: s3
    path: s3://test-bucket/input/
    format: parquet
sink:
  source: input
  type: s3
  path: s3://test-bucket/output/
  format: parquet
  mode: overwrite
```

`plugin_source` is required even for local testing, but it is never
fetched as long as the cached binary above is in place -- yard short-circuits
on the cache hit. It cannot point at a `file://` path: the downloader accepts
only `https://` (plus `http://` on loopback for local test servers), so a
`file://` URL fails the moment the cache misses.

Run `yard plan` to verify the plugin is discovered and called correctly. If you
see a download attempt, your cached filename does not match the expected
platform key.

### Step 5: Logging

stdout is the protocol channel. Anything else written to it corrupts the stream, so all logging goes to stderr.

<!-- tabs:start -->
<!-- tab: Rust -->
**Rust**

The SDK captures stdout at the file descriptor level -- any `println!()` in your code or dependencies is automatically redirected to stderr.

For structured logging, use `tracing` (re-exported by the SDK):

```rust
use yard_plugin_sdk::tracing;

fn deploy(&self, job_name: &str, ...) -> anyhow::Result<DeployResponse> {
    tracing::info!("deploying {job_name}");
    // ...
}
```

The SDK auto-initializes a stderr tracing subscriber with `RUST_LOG` env-filter support. No setup required.
<!-- tab: Python -->
**Python**

Nothing protects stdout for you. Never call `print()` in handler code, and watch for libraries that print. Use the `logging` module configured for stderr, as in the scaffold above:

```python
def handle_deploy(job_name, job_config, artifact):
    log.info("deploying %s", job_name)
    # ...
    return {"resources": []}
```

Non-zero exit tells yard the operation failed, and yard shows the last lines of stderr as the error message, so log the cause before exiting.
<!-- tabs:end -->

### Step 6: Release to GitHub

Create a GitHub release with a tag matching your version (e.g. `v0.1.0`). Upload one asset per platform following the naming convention:

```
yard-plugin-example-0.1.0-macos-aarch64
yard-plugin-example-0.1.0-macos-x86_64
yard-plugin-example-0.1.0-linux-x86_64
yard-plugin-example-0.1.0-linux-aarch64
```

<!-- tabs:start -->
<!-- tab: Rust -->
**Rust**

Cross-compile or build on each platform, and upload each binary under its platform name.
<!-- tab: Python -->
**Python**

The script is the same on every platform, but `plugin_source` resolves to one asset per platform, so upload the same file under each platform name.
<!-- tabs:end -->

Your asset names only have to match whatever `plugin_source` template your
users write. What is *not* free-form is how yard expands the placeholders:

| Placeholder | Expands to | Values |
|-------------|-----------|--------|
| `${name}` | `yard-plugin-<job type>` | Derived from the job's `type:` field |
| `${version}` | Value of `plugin_version` | e.g. `0.1.0` |
| `${os}` | `std::env::consts::OS` | `macos`, `linux` |
| `${arch}` | `std::env::consts::ARCH` | `aarch64`, `x86_64` |

`${os}` is **not** the target-triple fragment -- it is `macos`, not
`apple-darwin`, and `linux`, not `unknown-linux-gnu`. Name your release assets
to match, or your users' first `yard plan` will 404.
> **Known limitation (v2.0).** Placeholder templating is **not currently usable
> inside a `<job>.yaml`**. yard runs `${...}` context interpolation over the whole
> job file before parsing it, so a `plugin_source` containing `${version}`,
> `${os}` or `${arch}` fails to resolve with
> `Missing Variable: Could not find 'version' in the provided context`. This
> applies to YAML comments too. Until that is fixed, write the fully expanded
> URL for the platform you are on:
>
> ```yaml
> plugin_source: "https://<plugin-release-url>/yard-plugin-glue-0.1.0-aarch64-macos"
> ```
>
> The placeholder expansion itself works correctly once a template reaches the
> downloader -- it is only the job-file interpolation pass that rejects it.

Also note `${name}` is derived from the job's `type:` field as
`yard-plugin-<type>`. A job with `type: example` resolves to plugin name
`yard-plugin-example`, so your binary must be named accordingly.

Users reference your release in their `job.yaml`:

```yaml
plugin_version: "0.1.0"
plugin_source: "https://github.com/your-org/yard-plugin-example/releases/download/v0.1.0/yard-plugin-example-0.1.0-aarch64-macos"
```

### TOFU checksum model

yard uses a trust-on-first-use (TOFU) checksum model for plugin binaries:

1. **First download:** yard downloads the binary, computes its SHA-256 checksum, and records it in `yard.lock` at the project root.
2. **Subsequent runs:** yard verifies the cached binary's checksum against `yard.lock`. A mismatch aborts the operation.
3. **Version bumps:** changing `plugin_version` in `job.yaml` triggers a re-download. The new checksum replaces the old entry in `yard.lock`.

`yard.lock` should be committed to version control. Team members on different platforms accumulate their platform-specific checksums in the same file -- the lock file grows organically.

## Part 2: JSON protocol spec

For developers building plugins in Go, TypeScript, or any other language, here is the raw protocol that the Rust SDK abstracts and the Python scaffold above implements by hand.

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
| `resources` | Resource[] (`{"type","id","provider"}`) | destroy, verify |
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
{"resources":[{"type":"s3_object","id":"s3://bucket/key","provider":"example"}]}
```

Each resource is `{"type", "id", "provider"}`. Note the key is `type`, not
`resource_type` -- yard stores these verbatim in job state and passes them back
to `destroy` and `verify`.

**destroy** -- tear down resources (empty response on success):
```json
{}
```

**verify** -- check resource existence. Each status wraps the whole resource
object rather than flattening it:
```json
{"statuses":[{"resource":{"type":"s3_object","id":"s3://bucket/key","provider":"example"},"exists":true}]}
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
