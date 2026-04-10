#!/usr/bin/env bash
# Bootstrap the `grammys_dw` database and grant the application user
# access to it. This runs as a SOURCED init script inside the mysql:8.0
# container, so it has access to the env vars set on the container and
# to the `docker_process_sql` helper provided by the official entrypoint.
#
# We need this as a separate step because the `MYSQL_DATABASE` env var
# only creates and grants access to ONE database (the raw source), and
# the warehouse lives in its own schema to keep the landing layer and
# the analytical layer cleanly separated.
#
# IMPORTANT: do NOT enable `set -e`, `set -u` or `set -o pipefail` here.
# Because the entrypoint sources this file, any shell options we set
# persist in the entrypoint's own shell after the source returns, and
# the entrypoint has trailing logic that relies on permissive defaults
# (e.g. `"$1"` references that fail under `set -u`).

docker_process_sql --database=mysql <<SQL
CREATE DATABASE IF NOT EXISTS \`${MYSQL_DW_DB}\`
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci;

GRANT ALL PRIVILEGES ON \`${MYSQL_DW_DB}\`.* TO '${MYSQL_USER}'@'%';
FLUSH PRIVILEGES;
SQL
