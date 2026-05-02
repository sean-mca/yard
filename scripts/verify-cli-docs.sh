#!/usr/bin/env bash
set -euo pipefail

DOC="docs/reference/cli.md"

# Build once so cargo run is fast
cargo build -p yard --quiet

# Ordered list -- keep stable for clear failure messages.
# "list targets" is the one nested case.
SUBCOMMANDS=(
  "init"
  "plan"
  "apply"
  "show"
  "validate"
  "destroy"
  "force-unlock"
  "list"
  "list targets"
)

missing=0

for cmd in "${SUBCOMMANDS[@]}"; do
  # Capture --help text. Use cargo run --quiet -- so build noise stays out.
  help_text=$(cargo run -p yard --quiet -- $cmd --help 2>&1)

  # Extract long-form flags (--foo or --foo-bar). Strip values like <TARGET>.
  # Filter out the global --help/--version which are clap-injected, not yard-defined.
  flags=$(printf '%s\n' "$help_text" \
    | grep -oE -- '--[a-z][a-z0-9-]*' \
    | sort -u \
    | grep -vE '^(--help|--version)$' || true)

  for flag in $flags; do
    if ! grep -q -- "$flag" "$DOC"; then
      echo "MISSING: subcommand '$cmd' has flag '$flag' in --help, but not in $DOC"
      missing=$((missing + 1))
    fi
  done
done

if [[ $missing -gt 0 ]]; then
  echo "FAIL: $missing missing flag mention(s) in $DOC"
  exit 1
fi
echo "OK: $DOC mentions every flag from yard <cmd> --help"
