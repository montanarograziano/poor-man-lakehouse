
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

# Launch docker compose with optional profile
# Usage: just up              (core only: minio + postgres)
#        just up nessie       (core + Nessie catalog)
#        just up lakekeeper   (core + Lakekeeper catalog)
#        just up dremio       (core + Nessie + Dremio)
#        just up spark        (core + Spark cluster)
#        just up full         (all services)
up profile="":
  {{just_executable()}} needs docker
  @if [ -z "{{profile}}" ]; then \
    docker compose up --build --detach; \
  else \
    docker compose --profile {{profile}} up --build --detach; \
  fi

# Stop docker compose (stops all profiles)
down:
  {{just_executable()}} needs docker
  docker compose --profile full down

# Launch docker compose in clean environment
up-clean profile="":
  {{just_executable()}} needs docker
  docker compose --profile full down --remove-orphans --volumes
  find ./configs -type d -name "data" -exec rm -rf {} +
  @if [ -z "{{profile}}" ]; then \
    docker compose up --build --detach; \
  else \
    docker compose --profile {{profile}} up --build --detach; \
  fi

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