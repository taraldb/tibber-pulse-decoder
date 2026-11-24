#!/usr/bin/env bash
#
# Redeploy the Tibber Pulse HAN decoder to the Unraid host.
#
# Run from the repo root on your machine:  ./scripts/deploy.sh
#
# It rsyncs the build inputs + .env to the host, rebuilds the image there (the host has
# no `docker compose`), and recreates the container with the canonical run flags
# below. Safe to run repeatedly; the container is replaced in place.
#
# Broker host/credentials + meter id live in the git-ignored .env (see .env.example).
# rsync copies it byte-for-byte, so non-ASCII passwords survive (they don't via ssh -e).
#
set -euo pipefail

HOST="${HOST:-root@10.1.1.200}"
APPDIR="${APPDIR:-/mnt/user/appdata/tibber-pulse-decoder}"
IMAGE="tibber-pulse-decoder:latest"
NAME="tibber-pulse-decoder"

# --- canonical non-secret config (keep in sync with docker-compose.yml) -----------
RUN_ENV=(
  -e PULSE_TOPIC=puls/publish
  -e PULSE_CMD_TOPIC=puls/subscribe
  -e BATCHING_AUTO_DISABLE=true
  -e OUT_PREFIX=tibber/pulse
  -e HASS_DISCOVERY_ENABLED=true
  -e HASS_DISCOVERY_PREFIX=homeassistant
  -e ENERGY_INTEGRATION_ENABLED=true
  -e ENERGY_TZ=Europe/Oslo
  -e ENERGY_GAP_OK_SEC=15
  -e ENERGY_GAP_ESTIMATE_SEC=45
  -e ENERGY_RESUME_FROM_MQTT=true
  -e LOG_LEVEL=INFO
)

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

[[ -f .env ]] || { echo "missing .env (copy .env.example and fill in)" >&2; exit 1; }

echo ">> syncing build inputs to $HOST:$APPDIR"
ssh "$HOST" "mkdir -p '$APPDIR/tibber_pulse_decoder'"
rsync -a -e ssh Dockerfile docker-compose.yml PULSE_DECODER.md "$HOST:$APPDIR/"
rsync -a -e ssh .env "$HOST:$APPDIR/.env"
ssh "$HOST" "chmod 600 '$APPDIR/.env'"
rsync -a -e ssh tibber_pulse_decoder/pulse_decoder.py tibber_pulse_decoder/requirements.txt \
  "$HOST:$APPDIR/tibber_pulse_decoder/"

echo ">> building image on $HOST"
ssh "$HOST" "cd '$APPDIR' && docker build -t '$IMAGE' ."

echo ">> recreating container '$NAME'"
ssh "$HOST" "docker rm -f '$NAME' >/dev/null 2>&1 || true; \
  docker run -d \
    --name '$NAME' \
    --restart unless-stopped \
    --network host \
    --label com.centurylinklabs.watchtower.enable=false \
    --env-file '$APPDIR/.env' \
    ${RUN_ENV[*]} \
    '$IMAGE'"

echo ">> status"
ssh "$HOST" "docker ps --filter name='$NAME' --format '{{.Names}}\t{{.Status}}'; \
  sleep 6; docker logs --tail 8 '$NAME'"

echo ">> done. Verify:  mosquitto_sub -h <LOCAL_HOST> -u <LOCAL_USERNAME> -P <LOCAL_PASSWORD> -t 'tibber/pulse/#' -v   (values in .env)"
