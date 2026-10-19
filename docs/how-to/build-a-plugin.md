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

Point a job at your build with a `file://` source. Nothing is downloaded,
nothing is checksummed, and `plugin_version` is ignored, so you can rebuild
and re-run as often as you like:

<!-- tabs:start -->
<!-- tab: Rust -->
**Rust**

```yaml
# test-job.yaml
type: example
plugin_source: "file://../yard-plugin-example/target/release/yard-plugin-example"
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
<!-- tab: Python -->
**Python**

```yaml
# test-job.yaml
type: example
plugin_source: "file://../yard-plugin-example/yard-plugin-example.py"
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

The script needs its shebang line and the executable bit (`chmod +x`).
<!-- tabs:end -->

Relative paths resolve against the project root (the directory holding
`yard.yaml`); absolute paths (`file:///opt/...`) and `~/` work too. A bare
path starting with `./`, `../`, `/`, or `~/` is accepted as shorthand for
`file://`.

Run `yard validate` to see the plugin's `schema` and `validate` operations
called, and `yard show <job>` to print its `codegen` output.

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

Create a GitHub release with a tag matching your version (`v0.1.0`) and
upload one asset per platform named

```
<plugin name>-<version>-<target triple>
```

for example:

```
yard-plugin-example-0.1.0-aarch64-apple-darwin
yard-plugin-example-0.1.0-x86_64-apple-darwin
yard-plugin-example-0.1.0-x86_64-unknown-linux-gnu
yard-plugin-example-0.1.0-aarch64-unknown-linux-gnu
```

The suffix is the Rust target triple, exactly what a cargo cross-compile
matrix gives you as `${{ matrix.target }}`. yard works out the triple of the
machine it runs on and fetches the matching asset; users never spell out an
OS or architecture. On Linux yard assumes a `gnu` toolchain.

<!-- tabs:start -->
<!-- tab: Rust -->
**Rust**

Cross-compile or build on each platform, and upload each binary under its
target name.
<!-- tab: Python -->
**Python**

The script is the same on every platform, but yard asks for one asset per
target, so upload the same file under each target name.
<!-- tabs:end -->

Users then reference the repository and the version in their `job.yaml`:

```yaml
plugin_version: "0.1.0"
plugin_source: "https://github.com/your-org/yard-plugin-example"
```

yard turns that into
`https://github.com/your-org/yard-plugin-example/releases/download/v0.1.0/yard-plugin-example-0.1.0-<target>`
(falling back to a tag without the `v` prefix).

Other hosting works too. A `plugin_source` ending in `/` gets
`<name>-<version>-<target>` appended, and a URL template is expanded with
these placeholders:

| Placeholder | Expands to | Example |
|-------------|-----------|---------|
| `${name}` | `yard-plugin-<job type>` | `yard-plugin-example` |
| `${version}` | Value of `plugin_version` | `0.1.0` |
| `${target}` | Rust target triple of the running yard | `aarch64-apple-darwin` |
| `${os}` | `std::env::consts::OS` | `macos`, `linux` |
| `${arch}` | `std::env::consts::ARCH` | `aarch64`, `x86_64` |
| `${platform}` | `${arch}-${os}` | `aarch64-macos` |

A fully explicit URL with no placeholders is used verbatim, which ties the
job file to one platform; prefer the repository form.

Users without a release can build from source instead. A git source
(`git::https://...`, a URL ending in `.git`, or `git@...`) is cloned at the
tag, branch, or commit named by `plugin_version`; yard then runs
`cargo build --release` if it finds a `Cargo.toml`, or uses a script named
after the plugin. Append `//<subdir>` to select a directory inside a
monorepo. Git builds are cached by ref and are not recorded in `yard.lock`.

Note `${name}` is derived from the job's `type:` field as
`yard-plugin-<type>`. A job with `type: example` resolves to plugin name
`yard-plugin-example`, so your binary, package, and release assets must be
named accordingly.

### TOFU checksum model

yard uses a trust-on-first-use (TOFU) checksum model for plugin binaries:

1. **First download:** yard downloads the binary, computes its SHA-256 checksum, and records it in `yard.lock` at the project root.
2. **Subsequent runs:** yard verifies the cached binary's checksum against `yard.lock`. A mismatch aborts the operation.
3. **Version bumps:** changing `plugin_version` in `job.yaml` triggers a re-download. The new checksum replaces the old entry in `yard.lock`.

`yard.lock` should be committed to version control. Team members on different platforms accumulate their platform-specific checksums in the same file -- the lock file grows organically. Only downloaded binaries are locked; `file://` and git sources are never checksummed.

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
