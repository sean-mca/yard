<!-- generated-by: gsd-doc-writer -->
# Contributing to YARD

Thanks for your interest in YARD. External contributions are welcome — this document describes how to file issues, propose changes, and what to expect from the review process.

## License and Contribution Terms

YARD is licensed under the **Business Source License 1.1 (BSL 1.1)**, not MIT or Apache 2.0. See [LICENSE](LICENSE) for the full text. Key points for contributors:

- The Licensed Work is (c) 2026 Sean McAuliffe.
- The **Change License** is Apache License 2.0, effective four years after the first publicly available distribution of each version.
- The **Additional Use Grant** permits use of YARD *except* as a "Data Pipeline Infrastructure Service" — defined in the license as "a commercial offering that allows third parties to access the functionality of the Licensed Work by creating, managing, or deploying data pipeline jobs." If you want to build such a service, you need a separate commercial license from the Licensor.
- By contributing, you agree your contribution is licensed under the same BSL 1.1 terms as the rest of the project.

There is **no CLA (Contributor License Agreement)** and **no DCO (Developer Certificate of Origin) sign-off** required by this repository at the time of writing. If that changes, this document and the PR template will be updated.

If anything about the license is unclear, open a discussion or issue before submitting a substantial contribution.

## Code of Conduct

This repository does not currently ship a `CODE_OF_CONDUCT.md`. Be respectful, constructive, and on-topic in issues and pull requests.

## Filing Issues and Proposing Features

Use GitHub Issues at `https://github.com/sean-mca/yard/issues` for bug reports, feature requests, and design discussions. There are no issue templates configured, so please include the following when relevant:

- **Bug reports**: YARD version or commit SHA, Rust toolchain version (`rustc --version`), OS, the exact command you ran, the `yard.yaml` or config snippet that reproduces the problem, and the observed vs. expected behavior.
- **Feature requests**: the problem you're trying to solve, the current workaround (if any), and — if you have one in mind — the shape of the API or config you'd like to see.
- **Larger proposals** (new provider, breaking change, architectural shift): open an issue *first* to discuss before opening a PR. This avoids wasted work if the direction isn't a fit.

## Setting Up a Development Environment

See [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) for prerequisites, build commands, and local workflow. See [docs/GETTING-STARTED.md](docs/GETTING-STARTED.md) for first-run setup.

## Contribution Workflow

1. **Fork** the repository on GitHub.
2. **Clone** your fork and add the upstream remote:
   ```bash
   git clone https://github.com/<your-username>/yard.git
   cd yard
   git remote add upstream https://github.com/sean-mca/yard.git
   ```
3. **Pull** the latest `main` before branching:
   ```bash
   git checkout main
   git pull upstream main
   ```
4. **Create a feature branch**. Use a descriptive slug:
   ```bash
   git checkout -b feat/my-feature
   # or fix/..., docs/..., refactor/..., test/..., chore/...
   ```
5. **Make your changes**, commit, and push to your fork.
6. **Open a pull request** against `sean-mca/yard`'s `main` branch.

## Coding Standards

The full rules live in [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) and [CLAUDE.md](CLAUDE.md). The non-negotiables for every PR:

- **`cargo clippy --all-targets -- -D warnings` must pass with zero issues.** CI enforces this.
- **`cargo fmt --all -- --check` must pass.** CI enforces this.
- **`cargo test` must pass.** CI enforces this.
- **No `unsafe {}` anywhere.** Ever.
- **No `.unwrap()` or `.expect()` in production code.** It is fine in tests.
- **Prefer stdlib over adding crates** for simple tasks. Justify new dependencies.
- **Never modify `Cargo.toml`** (including adding dependencies or bumping versions) without discussing it first in the issue or PR.
- **Never hardcode personal GitHub handles, email addresses, or repo names** as defaults.
- **Respect the crate boundaries**: all business logic lives in `yard-core`; `yard-cli` is a thin wrapper that parses args and displays output.

## Commit and PR Conventions

The project uses a conventional-commits-style prefix. Look at `git log` for examples. The observed pattern:

```
<type>(<scope>): <short description>
```

Common types in this repo: `feat`, `fix`, `docs`, `test`, `refactor`, `chore`, `merge`.

Scopes are optional. For multi-step phased work the repo uses numeric scopes like `feat(08-05): ...`; for one-off changes a module or area name is fine (e.g. `fix(cli): ...`, `docs(readme): ...`). Merge commits from PRs use GitHub's default `Merge pull request #N from <branch>` format.

PR guidelines:

- Keep PRs focused — one logical change per PR. Split large changes into a series if you can.
- Write a PR description that explains **what** changed and **why**. Reference the issue it closes (`Closes #123`) when applicable.
- Include tests for new behavior and bug fixes. See [docs/TESTING.md](docs/TESTING.md) for the testing strategy and how to run the suite.
- Make sure CI is green before asking for review. The CI job runs `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test`; the result is posted as a comment on the PR.
- Rebase on `main` (rather than merging `main` into your branch) to keep history clean when possible.

## Testing Expectations

See [docs/TESTING.md](docs/TESTING.md) for the full testing strategy. In short:

- New features need tests covering the happy path and at least the obvious error cases.
- Bug fixes should include a regression test that fails without the fix.
- `.unwrap()` is acceptable in tests; production code must handle errors explicitly.
- `cargo test` must pass before a PR is merged — CI will block otherwise.

## Code Review Expectations

- Reviews focus on correctness, clarity, adherence to the rules above, and fit with the existing architecture (see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)).
- Expect comments — most PRs go through at least one round of revisions.
- Reviewers may ask you to split a PR, extract a refactor into its own change, or discuss an approach in an issue before continuing.
- The project is early-stage and actively developed, so response times vary. If a PR has been idle for a while, a polite nudge on the PR thread is welcome.

## Scope Notes

This document covers contribution mechanics. For deeper context:

- Architecture and crate layout: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- Configuration schema and environment variables: [docs/CONFIGURATION.md](docs/CONFIGURATION.md)
- Development workflow and build commands: [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)
- Testing strategy: [docs/TESTING.md](docs/TESTING.md)
