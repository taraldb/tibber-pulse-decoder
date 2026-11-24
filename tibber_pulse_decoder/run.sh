#!/usr/bin/with-contenv bashio
# shellcheck shell=bash
# Map app options + the Supervisor's MQTT service to the env vars pulse_decoder.py reads.

if bashio::config.has_value 'mqtt_host'; then
  export LOCAL_HOST="$(bashio::config 'mqtt_host')"
  export LOCAL_PORT="$(bashio::config 'mqtt_port' '1883')"
  export LOCAL_USERNAME="$(bashio::config 'mqtt_username' '')"
  export LOCAL_PASSWORD="$(bashio::config 'mqtt_password' '')"
  bashio::log.info "Using MQTT broker from app options"
elif bashio::services.available 'mqtt'; then
  export LOCAL_HOST="$(bashio::services mqtt 'host')"
  export LOCAL_PORT="$(bashio::services mqtt 'port')"
  export LOCAL_USERNAME="$(bashio::services mqtt 'username')"
  export LOCAL_PASSWORD="$(bashio::services mqtt 'password')"
  if bashio::var.true "$(bashio::services mqtt 'ssl')"; then
    bashio::log.warning "Broker reports SSL; the decoder speaks plain MQTT. Set mqtt_host/mqtt_port to a non-TLS listener if it fails to connect."
  fi
  bashio::log.info "Using MQTT broker from the Mosquitto broker app"
else
  bashio::exit.nok "No MQTT broker found. Install the Mosquitto broker app, or set mqtt_host in this app's configuration."
fi

export PULSE_TOPIC="$(bashio::config 'pulse_topic')"
export PULSE_CMD_TOPIC="$(bashio::config 'pulse_cmd_topic')"
export BATCHING_AUTO_DISABLE="$(bashio::config 'batching_auto_disable')"
export OUT_PREFIX="$(bashio::config 'out_prefix')"
export HASS_DISCOVERY_ENABLED="$(bashio::config 'hass_discovery_enabled')"
export ENERGY_INTEGRATION_ENABLED="$(bashio::config 'energy_integration_enabled')"
export ENERGY_TZ="$(bashio::config 'energy_tz')"
export LOG_LEVEL="$(bashio::config 'log_level')"

bashio::log.info "Broker ${LOCAL_HOST}:${LOCAL_PORT}, source topic ${PULSE_TOPIC}, output ${OUT_PREFIX}"
exec python3 /app/pulse_decoder.py
