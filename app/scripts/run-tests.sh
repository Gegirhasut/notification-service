#!/usr/bin/env bash
# Run the integration + unit test suite in a container, keeping the host clean.
#
# testcontainers spawns ephemeral Postgres/RabbitMQ/Redis on the host Docker via
# the mounted socket. They publish to random host ports; the test container
# reaches them through the host gateway (host.docker.internal), so the host's own
# Redis on 6379 never clashes and we avoid the --network host port-binding clash.
set -euo pipefail

cd "$(dirname "$0")/.."

docker build --target test -t notification-service-test .

exec docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$(pwd):/app" \
  --add-host=host.docker.internal:host-gateway \
  -e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal \
  -e TESTCONTAINERS_RYUK_DISABLED=true \
  notification-service-test "$@"
