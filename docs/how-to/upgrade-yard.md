# Upgrade yard

yard ships in three forms: a Cargo crate (when published to
crates.io), a source build from the GitHub repo, and a Linux x86_64
binary attached to GitHub releases. Pick the path that matches how
you originally installed yard. After upgrading, run a drift check
to confirm your existing yamls still validate against the new
schema.

For per-version schema changes, see
[Previous migrations](#previous-migrations) below.

## Upgrade procedure

### From `cargo install`

> **Note:** yard is not currently published to crates.io. This path
> is reserved for the future. Today, use the source build or
> binary download below.

Once yard is on crates.io, upgrade with:

```bash
cargo install yard --force
yard --version
```

`--force` overwrites the existing binary. Without it, cargo refuses
to reinstall an unchanged version.

### From source (`git pull`)

For developers who cloned the repo:

```bash
cd /path/to/yard
git pull origin main
cargo build --release -p yard
./target/release/yard --version
```

Optionally copy the resulting binary into your `$PATH`:

```bash
cp target/release/yard ~/.local/bin/yard
```

### From a GitHub release binary

For users on Linux x86_64 (the only platform yard publishes
binaries for today):

```bash
# Replace <tag> with the desired release tag, e.g. v1.3.4
curl -L -o yard-linux-x86_64 \
  https://github.com/sean-mca/yard/releases/download/<tag>/yard-linux-x86_64
curl -L -o yard-linux-x86_64.sha256 \
  https://github.com/sean-mca/yard/releases/download/<tag>/yard-linux-x86_64.sha256
sha256sum -c yard-linux-x86_64.sha256
install -m 0755 yard-linux-x86_64 yard
./yard --version
```

`sha256sum -c` MUST print `OK` before you trust the binary. macOS
and Windows users currently build from source — yard does not
publish binaries for those platforms.

### Verify the upgrade

From a directory containing your yard project, run a drift check:

```bash
yard plan
```

A clean upgrade prints `(no change)` for every target. Any `~ update`
or validation error after upgrade likely means a schema change in the
new version — check the relevant migration note in
[Previous migrations](#previous-migrations).

## Previous migrations

Per-version schema and behavior change notes. Each migration doc
explains what changed, how to migrate existing yamls, and any
deprecation timelines.

- **v2.0** — **Breaking.** Providers moved out of the yard binary into
  external plugin binaries. Every `<job>.yaml` must now declare
  `plugin_version:` and `plugin_source:`. Airflow DAG generation and the
  `yard show dag` command were removed from core.
  [docs/reference/migrations/v2.0.md](../reference/migrations/v2.0.md)

- **v1.11** — Version-aware DAG codegen (`airflow.version: "3"`) and the
  `"asset"` trigger alias.
  [docs/reference/migrations/v1.11.md](../reference/migrations/v1.11.md)

- **v1.6** — Event-driven DAGs and the `triggered_by:` /
  `produces:` rename to `trigger:` / `publishes:`. Hard rename, no
  back-compat aliases.
  [docs/reference/migrations/v1.6.md](../reference/migrations/v1.6.md)

Future migration docs land in the same folder
(`docs/reference/migrations/<version>.md`) and gain a bullet here
on each release.

## See also

- [docs/reference/cli.md](../reference/cli.md) — `yard --version`, `yard plan`, and full subcommand reference.
- [docs/reference/migrations/v2.0.md](../reference/migrations/v2.0.md) — current latest migration.
