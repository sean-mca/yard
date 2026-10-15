# Upgrade yard

yard ships in three forms: a Homebrew formula, prebuilt binaries
attached to each GitHub release (macOS Apple Silicon and Intel, Linux
x86_64 and ARM), and a source build from the GitHub repo. Pick the
path that matches how you originally installed yard. After upgrading,
run a drift check to confirm your existing yamls still validate
against the new schema.

If you are coming from v1.x, read the [v2.0 migration guide](../reference/migrations/v2.0.md)
after upgrading: every job file needs two new fields before `yard plan` works.

## Upgrade procedure

### From Homebrew

The tap is updated automatically on every release:

```bash
brew update
brew upgrade yard
yard --version
```

### From a GitHub release binary

Each release attaches one tarball per platform, named
`yard-<version>-<target>.tar.gz`, plus a `SHA256SUMS` file covering all
of them. The targets are `aarch64-apple-darwin`, `x86_64-apple-darwin`,
`x86_64-unknown-linux-gnu`, and `aarch64-unknown-linux-gnu`.

```bash
# Replace <version> with the release, e.g. 2.0.0, and <target> with your platform.
BASE=https://github.com/sean-mca/yard/releases/download/v<version>
curl -L -o yard-<version>-<target>.tar.gz "$BASE/yard-<version>-<target>.tar.gz"
curl -L -o SHA256SUMS "$BASE/SHA256SUMS"
shasum -a 256 -c SHA256SUMS --ignore-missing
tar -xzf yard-<version>-<target>.tar.gz
install -m 0755 yard ~/.local/bin/yard
yard --version
```

The checksum step MUST print `OK` before you trust the binary. On Linux
without `shasum`, use `sha256sum -c SHA256SUMS --ignore-missing`.

### From source (`git pull`)

For contributors who cloned the repo:

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

### Verify the upgrade

From a directory containing your yard project, run a drift check:

```bash
yard plan
```

A clean upgrade prints `(no change)` for every target. Any `~ update`
or validation error after upgrade likely means a schema change in the
new version — check the [migration guide](#migrating-from-v1x).

## Migrating from v1.x

v2.0 is a breaking release. Providers moved out of the yard binary into
plugin binaries, every `<job>.yaml` must declare `plugin_version:` and
`plugin_source:`, and Airflow DAG generation moved to its own plugin.
The [v2.0 migration guide](../reference/migrations/v2.0.md) has the steps.

Migration notes for v1.x point releases stay in
`docs/reference/migrations/` in the repository for anyone still on 1.x.

## See also

- [docs/reference/cli.md](../reference/cli.md) — `yard --version`, `yard plan`, and full subcommand reference.
- [docs/reference/migrations/v2.0.md](../reference/migrations/v2.0.md) — current latest migration.
