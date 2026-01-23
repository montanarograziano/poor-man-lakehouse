
set positional-arguments
# List all available recipes
@help:
  just --list

# Install dependencies and pre-commit hooks
install:
    {{just_executable()}} needs uv
    uv sync --all-groups
    uv run prek install

# Update dependencies and pre-commit hooks
update:
    uv sync --all-groups --update-packages
    uv run prek auto-update

# Run linters
lint:
    {{just_executable()}} needs uv
    uv run ruff format src tests
    uv run ruff check src tests --fix --unsafe-fixes
    uv run mypy src tests

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

# Launch docker compose in clean environment
up_clean:
  {{just_executable()}} needs docker
  docker compose down --remove-orphans --volumes
  find ./configs -type d -name "data" -exec rm -rf {} +
  docker compose up --build --detach

# Read logs from Compose
logs:
  {{just_executable()}} needs docker
  docker compose logs --follow


# Commit with conventional commits
commit:
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