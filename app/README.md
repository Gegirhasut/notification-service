# Notification Service — app/

This directory is the project root: all source, the Dockerfile, `docker-compose.yml`,
migrations, tests, scripts, and committed docs (`docs/openapi.json`,
`docs/postman_collection.json`) live here. Run all build/run/test commands from inside
`app/`.

```bash
cd app
docker compose up --build        # bring up the whole stack
./scripts/run-tests.sh           # run the test suite (testcontainers; Docker only)
```

**The full project documentation — quick start, API, architecture, broker topology,
delivery semantics, status model, and testing — is the [root README](../README.md).**
It is the single source of truth; this file is only a pointer to avoid duplication.
