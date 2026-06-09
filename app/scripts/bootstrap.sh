#!/usr/bin/env bash
# Install Docker Engine + the Compose plugin on a clean Ubuntu host. Idempotent:
# safe to re-run; skips anything already present. Installs NOTHING else — Postgres,
# RabbitMQ and Redis all run as compose services, never on the host.
#
# Usage:  ./app/scripts/bootstrap.sh   (run from the repo root)
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  SUDO="sudo"
else
  SUDO=""
fi

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  echo "Docker Engine and the Compose plugin are already installed:"
  docker --version
  docker compose version
  exit 0
fi

echo "Installing Docker Engine + Compose plugin..."

export DEBIAN_FRONTEND=noninteractive
$SUDO apt-get update -y
$SUDO apt-get install -y ca-certificates curl gnupg

# Docker's official APT repository.
$SUDO install -m 0755 -d /etc/apt/keyrings
if [[ ! -f /etc/apt/keyrings/docker.gpg ]]; then
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
    | $SUDO gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  $SUDO chmod a+r /etc/apt/keyrings/docker.gpg
fi

. /etc/os-release
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" \
  | $SUDO tee /etc/apt/sources.list.d/docker.list >/dev/null

$SUDO apt-get update -y
$SUDO apt-get install -y \
  docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

$SUDO systemctl enable --now docker || true

# Let the invoking user run docker without sudo (effective on next login).
if [[ -n "${SUDO_USER:-}" ]]; then
  $SUDO usermod -aG docker "$SUDO_USER" || true
elif [[ $EUID -ne 0 ]]; then
  $SUDO usermod -aG docker "$USER" || true
fi

echo "Done:"
docker --version
docker compose version
echo "Note: re-login (or 'newgrp docker') for group membership to take effect."
