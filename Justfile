
set positional-arguments
# List all available recipes
@help:
  just --list

# Install dependencies and pre-commit hooks
install:
    {{just_executable()}} needs uv
    uv sync --all-groups
    uv run pre-commit install --install-hooks

# Update dependencies and pre-commit hooks
update:
    uv sync --all-groups --update-packages
    uv run pre-commit autoupdate

# Run pre-commit hooks
lint:
    uv run pre-commit run

# Run all tests
test:
    uv run pytest tests

# Launch docker compose
up:
  {{just_executable()}} needs docker
  docker compose up --build --detach

# Stop docker compose
down:
  {{just_executable()}} needs docker
  docker compose down


# Commit with conventional commits
@commit:
    uv run cz commit

alias c := commit


# Live preview the documentation
preview-docs:
  uv run mike serve --config-file=docs/mkdocs.yml

# Assert a command is available
[private]
needs +commands:
  #!/usr/bin/env zsh
  set -euo pipefail
  for cmd in "$@"; do
    if ! command -v $cmd &> /dev/null; then
      echo "$cmd binary not found. Did you forget to install it?"
      exit 1
    fi
  done