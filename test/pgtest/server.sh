# shellcheck shell=bash
# Runs PostgreSQL under a loop so that it can be stopped and started without stopping the container.
set -euo pipefail

PGDATA=/tmp/pgdata
HOLD=/tmp/pg-hold
pid=

initdb -D "$PGDATA" -U postgres --auth-local=trust --auth-host=scram-sha-256 --pwfile=<(echo postgres) >/dev/null
echo "host all all all scram-sha-256" >>"$PGDATA/pg_hba.conf"

shutdown() {
  if [ -n "$pid" ]; then
    kill -INT "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  exit 0
}
trap shutdown TERM

while true; do
  while [ -e "$HOLD" ]; do
    sleep 0.2
  done
  postgres -D "$PGDATA" -c listen_addresses='*' &
  pid=$!
  wait "$pid" || true
  pid=
done
