# syntax=docker/dockerfile:1

# --- Build stage: install dependencies into a virtualenv ---------------------
FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

# Create an isolated venv we can copy wholesale into the runtime image.
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install dependencies first (better layer caching), then the package itself.
COPY pyproject.toml ./
COPY notification_service ./notification_service
RUN pip install .

# --- Runtime stage -----------------------------------------------------------
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH"

# Non-root user.
RUN useradd --create-home --uid 10001 appuser

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
COPY notification_service ./notification_service
COPY migrations ./migrations
COPY alembic.ini ./alembic.ini

USER appuser

# Default command runs the API; compose overrides it for dispatcher/receipts/migrate.
CMD ["uvicorn", "notification_service.main:app", "--host", "0.0.0.0", "--port", "8000"]

# --- Test stage --------------------------------------------------------------
# Runs the integration suite. Launched (see scripts/run-tests.sh) with the host
# Docker socket mounted and host.docker.internal mapped to the host gateway, so
# testcontainers can spawn ephemeral sibling containers (Postgres/RabbitMQ/Redis)
# on the host and reach them on random ports. Code is installed editable so the
# source can be bind-mounted for fast iteration.
FROM python:3.12-slim AS test

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH" \
    TESTCONTAINERS_RYUK_DISABLED=true

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
COPY pyproject.toml alembic.ini ./
COPY notification_service ./notification_service
COPY migrations ./migrations
COPY tests ./tests
RUN pip install -e ".[dev]"

CMD ["pytest", "-q"]
