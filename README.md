# Tibber Pulse HAN decoder

Decodes the Tibber Pulse's **raw HAN feed** (MQTT topic `puls/publish`) locally into clean JSON on
`tibber/pulse/<meter_id>/state`, plus Home Assistant MQTT discovery: power, current/voltage per
phase, cumulative energy, and an hourly energy integrator. No Tibber cloud is involved.
It handles Aidon, Kaifa and Kamstrup meters through [`amshan`](https://pypi.org/project/amshan/).

## Home Assistant OS: install as an app

[![Add repository to my Home Assistant](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](https://my.home-assistant.io/redirect/supervisor_add_addon_repository/?repository_url=https%3A%2F%2Fgithub.com%2Ftaraldb%2Ftibber-pulse-decoder)

Or add the repository manually: **Settings → Apps → App store → ⋮ → Repositories** →
`https://github.com/taraldb/tibber-pulse-decoder`. Then install **Tibber Pulse HAN decoder**.

Prerequisites (the Mosquitto broker app, and a Pulse Bridge publishing to it) are covered in
[`tibber_pulse_decoder/DOCS.md`](tibber_pulse_decoder/DOCS.md).

## Plain Docker

Copy `.env.example` to `.env` and fill in the broker settings. Then run `docker compose up -d --build`,
or adapt `scripts/deploy.sh` (rsync + `docker run` on a remote host).

**→ [`PULSE_DECODER.md`](PULSE_DECODER.md) covers architecture, config, MQTT output and troubleshooting.**
