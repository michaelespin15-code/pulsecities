#!/usr/bin/env bash
#
# Source this to get libpq credentials without putting the password in argv.
#
#   . "$(dirname "$0")/lib/pgenv.sh"
#   pg_dump "$PGDSN" | gzip > out.gz
#
# Why. `pg_dump "$DATABASE_URL"` puts the whole connection URI, password and
# all, into the process command line, where every user on the box can read it
# out of `ps` for as long as the dump runs. On 2026-08-29 a routine `pgrep`
# printed the live password straight into a terminal for exactly this reason,
# and the password had to be rotated again.
#
# The box is single-tenant, so the practical exposure is small. The habit is
# not: argv is the one place a secret is readable without any privilege at all,
# and a nightly 20-minute pg_dump is a 20-minute window every night.
#
# PGPASSWORD is read by libpq from the environment, which `ps` does not show to
# other users on Linux. PGDSN carries everything else.
set -euo pipefail

_pg_env_file="${APP_DIR:-/root/pulsecities}/.env"
_pg_url=$(grep -E '^DATABASE_URL=' "$_pg_env_file" | cut -d= -f2-)
if [ -z "${_pg_url:-}" ]; then
    echo "ERROR: DATABASE_URL not found in $_pg_env_file" >&2
    exit 1
fi

# postgresql://user:password@host:port/dbname  ->  password, and the rest
PGPASSWORD=$(printf '%s' "$_pg_url" | sed -E 's|^[^:]+://[^:]+:([^@]*)@.*$|\1|')
PGDSN=$(printf '%s' "$_pg_url" | sed -E 's|^([^:]+://[^:]+):[^@]*@|\1@|')
export PGPASSWORD PGDSN

if [ "$PGDSN" = "$_pg_url" ]; then
    echo "ERROR: could not strip the password from DATABASE_URL" >&2
    exit 1
fi
unset _pg_url _pg_env_file
