# Tibber Pulse HAN decoder

Decodes the **raw HAN feed** the Tibber Pulse publishes on `puls/publish` and republishes
it as clean JSON on `tibber/pulse/<meter_id>/state`, plus Home Assistant MQTT discovery so
the meter appears as a device automatically. Runs two ways from the same `pulse_decoder.py`:

- **Unraid** (my setup): plain Docker container next to Mosquitto, deployed by `scripts/deploy.sh`.
- **Home Assistant OS**: as an HA **app** (formerly "add-on"), installed from this GitHub repo.
  See [HA app](#ha-app-home-assistant-os) below and `tibber_pulse_decoder/DOCS.md`.

This is the single source of truth for the service. It replaced the retired OSS.no cloud
integration (now under `archive/oss-integration/`).

---

## Data flow

```
Tibber Pulse ──MQTT──> mosquitto (:1883)  topic: puls/publish
                                   │
                                   ▼
                        tibber-pulse-decoder container
                        ├─ ignores {"status":{...}} health JSON
                        ├─ parses the protobuf-ish envelope: per-telegram
                        │    (uptime-ms timestamp, HAN tag, 0x7E-delimited HDLC frame)
                        │    — falls back to a bare 0x7E scan if the shape changes
                        ├─ decodes each frame with han.autodecoder.AutoDecoder (amshan==2.1.1)
                        │    (per-frame fresh HdlcFrameReader)
                        ├─ integrates each telegram's active_power_import into per-hour
                        │    energy, timestamped from the envelope
                        ├─ nudges the Pulse out of buffered mode (see below)
                        └─ merges frames into a running snapshot, publishes:
                                   │
                                   ▼
   puls/subscribe                          batching_disable true  (command to the Pulse)
   tibber/pulse/status                     online | offline   (retained, LWT)
   tibber/pulse/<meter_id>/state           full decoded JSON  (retained)
   homeassistant/sensor/tibber_pulse_<id>/<key>/config   18 sensors (retained)
```

Each binary `puls/publish` message = ~3 short frames (active power only) + 1 long Aidon
list frame, each behind a short `HAN` preamble. The long frame carries meter id/type,
active/reactive power, current L1–L3, voltage L1–L3, and — **only in the top-of-hour
frame** — the meter clock and cumulative energy registers. The running snapshot keeps the
hourly-only fields between top-of-hour frames.

**Batching auto-disable.** When a `puls/publish` message contains more than one HDLC frame
(the Pulse's buffered mode), the decoder publishes `batching_disable true` to
`puls/subscribe`: once on every (re)connect, then unconditionally every 30 min, and every
5 min for as long as batched messages keep arriving (the Pulse never acks). Disable with
`BATCHING_AUTO_DISABLE=false`.

**Hourly energy integrator.** Each telegram's `active_power_import` is trapezoidally
integrated into `energy_hour_kwh` (running total for the current `ENERGY_TZ` clock hour,
resets at `:00`) and `energy_last_hour_kwh` (the last completed hour). Samples are
timestamped from the envelope's per-frame uptime counter (newest frame anchored to
message-arrival wall-clock), so resolution stays ~2.5 s even while messages are batched
~10 s apart. Fills the gap between the meter's own once-per-hour cumulative registers.
Disable with `ENERGY_INTEGRATION_ENABLED=false`.

**Restart / gap handling & data quality.** On boot the integrator resumes from its own
retained `…/state` (`ENERGY_RESUME_FROM_MQTT`), so a mid-hour restart keeps counting from
where it left off. Every interval with no data is classified by length:

| gap | behaviour | `energy_hour_quality` |
|---|---|---|
| ≤ `ENERGY_GAP_OK_SEC` (15 s) | integrated normally | `ok` |
| ≤ `ENERGY_GAP_ESTIMATE_SEC` (45 s) | filled with mean power | `estimated` |
| > 45 s | **not** filled — energy is lost | `degraded` (rest of the hour) |

`energy_hour_gap_seconds` accumulates the unaccounted time. Quality only worsens within an
hour and resets to `ok` at `:00`. A cold start with no retained state partway into an hour
marks that hour `degraded` straight away.

**`:00` register fix-up.** When the meter's own `active_power_import_total` lands at the
top of the hour, the just-closed hour is reconciled: `energy_last_hour_kwh` is replaced
with the exact register delta (`energy_last_hour_source: meter`), a `degraded` hour becomes
`corrected`, and `energy_last_hour_error_kwh` = integrated − meter is published (+ a log
line). Needs a register reading from the previous `:00` too; absent that (first run / down
over the boundary) the integrated value stands with its flag.

**Consumer pattern:** while `binary_sensor energy_hour_degraded` is ON (or
`energy_hour_quality != ok`), treat `energy_hour_kwh` as a lower bound only — don't bill or
automate on it. It self-clears at `:00`; use `energy_last_hour_kwh` (now meter-sourced)
for that hour instead.

Meter (mine): 3-phase Aidon, type `6534`. Id is in `.env` (`METER_ID_DEFAULT`).

---

## Repo layout

| path | purpose |
|---|---|
| `tibber_pulse_decoder/pulse_decoder.py` | **the service** (single source for both deploy targets) |
| `tibber_pulse_decoder/requirements.txt` | runtime deps |
| `tibber_pulse_decoder/{config.yaml,Dockerfile,run.sh,DOCS.md,translations/}` | HA app packaging |
| `repository.yaml` | makes the repo an HA app repository |
| `Dockerfile` | Unraid image (python:3.12-slim, copies from `tibber_pulse_decoder/`) |
| `scripts/deploy.sh` | one-command redeploy to the Unraid host |
| `.env` (git-ignored) / `.env.example` | Unraid broker host/credentials + meter id |
| `docker-compose.yml` | reference config **only**. The host has no `docker compose`, so `deploy.sh` uses plain `docker run`. Keep the two in sync. |
| `mqtt.log` (git-ignored) | original raw capture used to reverse-engineer the format |
| `archive/oss-integration/` (git-ignored) | retired OSS.no cloud bridge + its docs/secrets. Not used. Safe to delete. |

---

## Deployed state

| | |
|---|---|
| Host | `root@10.1.1.200` (Unraid), SSH as root works |
| App dir | `/mnt/user/appdata/tibber-pulse-decoder/` |
| Image | `tibber-pulse-decoder:latest` (built on the host) |
| Container | `tibber-pulse-decoder` |
| Network | `--network host` |
| Restart | `--restart unless-stopped` (survives crashes, daemon restarts, host reboots; stays down only after a manual `docker stop`) |
| Watchtower | disabled via `com.centurylinklabs.watchtower.enable=false` label (image is local, no registry) |
| Not in the Unraid Docker UI | created outside the template system; use `docker`/Dozzle to manage |

Broker host/credentials + `METER_ID_DEFAULT` come from `.env`. `deploy.sh` rsyncs it to
`$APPDIR/.env` (mode 600) and passes it with `--env-file`. rsync keeps the bytes intact, so
the non-ASCII password survives; inline over `ssh -e` it gets mangled. All other settings
are passed explicitly (see `deploy.sh` / the table below).

---

## Redeploy

After changing `pulse_decoder.py` (or the Dockerfile / requirements), from the repo root:

```bash
./scripts/deploy.sh
```

It rsyncs the build inputs + `.env` to the host, rebuilds the image there, recreates the container
with the canonical flags, and prints status + recent logs. Idempotent.

Override target if needed: `HOST=root@1.2.3.4 APPDIR=/some/path ./scripts/deploy.sh`

### Manual redeploy (if `deploy.sh` can't run)

```bash
rsync -a Dockerfile .env root@10.1.1.200:/mnt/user/appdata/tibber-pulse-decoder/
rsync -a tibber_pulse_decoder/pulse_decoder.py tibber_pulse_decoder/requirements.txt \
  root@10.1.1.200:/mnt/user/appdata/tibber-pulse-decoder/tibber_pulse_decoder/

ssh root@10.1.1.200
cd /mnt/user/appdata/tibber-pulse-decoder
docker build -t tibber-pulse-decoder:latest .
docker rm -f tibber-pulse-decoder
docker run -d --name tibber-pulse-decoder \
  --restart unless-stopped --network host \
  --label com.centurylinklabs.watchtower.enable=false \
  --env-file /mnt/user/appdata/tibber-pulse-decoder/.env \
  -e PULSE_TOPIC=puls/publish -e PULSE_CMD_TOPIC=puls/subscribe -e BATCHING_AUTO_DISABLE=true \
  -e OUT_PREFIX=tibber/pulse \
  -e HASS_DISCOVERY_ENABLED=true -e HASS_DISCOVERY_PREFIX=homeassistant \
  -e ENERGY_INTEGRATION_ENABLED=true -e ENERGY_TZ=Europe/Oslo \
  -e ENERGY_GAP_OK_SEC=15 -e ENERGY_GAP_ESTIMATE_SEC=45 -e ENERGY_RESUME_FROM_MQTT=true \
  -e LOG_LEVEL=INFO \
  tibber-pulse-decoder:latest
```

---

## Configuration reference (env vars)

| var | default | note |
|---|---|---|
| `LOCAL_HOST` | `localhost` | Mosquitto host (Unraid: `.env`; HA app: from the Mosquitto app) |
| `LOCAL_PORT` | `1883` | |
| `LOCAL_USERNAME` / `LOCAL_PASSWORD` | empty | empty username = anonymous |
| `MQTT_CLIENT_ID` | `tibber-pulse-decoder` | must be unique per broker. Change it for a second instance (e.g. local dev), or it kicks the other one off |
| `PULSE_TOPIC` | `puls/publish` | source topic |
| `PULSE_CMD_TOPIC` | `puls/subscribe` | command topic the `batching_disable true` nudge is published to |
| `BATCHING_AUTO_DISABLE` | `true` | set `false` to never send the `batching_disable` command |
| `OUT_PREFIX` | `tibber/pulse` | output topic prefix |
| `HASS_DISCOVERY_ENABLED` | `true` | |
| `HASS_DISCOVERY_PREFIX` | `homeassistant` | |
| `METER_ID_DEFAULT` | empty | used only until a frame reports `meter_id`. Empty = no state/discovery publish until then |
| `ENERGY_INTEGRATION_ENABLED` | `true` | set `false` to drop the `energy_hour_*` fields |
| `ENERGY_TZ` | `Europe/Oslo` | clock-hour boundary for the energy integrator |
| `ENERGY_GAP_OK_SEC` | `15` | gap ≤ this: integrate silently |
| `ENERGY_GAP_ESTIMATE_SEC` | `45` | gap ≤ this: fill + flag `estimated`; above → `degraded` |
| `ENERGY_RESUME_FROM_MQTT` | `true` | resume the integrator from the retained `…/state` on boot |
| `LOG_LEVEL` | `INFO` | `DEBUG` = per-frame detail |

---

## MQTT output

| topic | payload | retained |
|---|---|---|
| `puls/subscribe` | `batching_disable true` (command to the Pulse) | no |
| `tibber/pulse/status` | `online` / `offline` (LWT) | yes |
| `tibber/pulse/<meter_id>/state` | full decoded JSON snapshot | yes |
| `homeassistant/{sensor,binary_sensor}/tibber_pulse_<meter_id>/<key>/config` | HA discovery, 21 entities | yes |

The decoder also subscribes briefly to `tibber/pulse/+/state` on boot to read back its own
retained snapshot (integrator resume), then unsubscribes.

Decoded JSON fields (`han.obis_map` names): `meter_id`, `meter_type`, `meter_manufacturer`,
`list_ver_id`, `active_power_import` / `active_power_export` (W), `reactive_power_import` /
`reactive_power_export` (var), `current_l1..l3` (A), `voltage_l1..l3` (V),
`active_power_import_total` / `active_power_export_total` (Wh, hourly),
`reactive_power_import_total` / `reactive_power_export_total` (varh, hourly),
`meter_datetime` (hourly; the meter's own RTC, a **naive** local-standard-time string —
amshan omits the offset when the DLMS `deviation` field is unset, and this meter runs
~1 h behind CEST in summer — so it's a plain string sensor, not a HA `timestamp`),
`decoded_at` (added by the service, UTC, tz-aware). Plus, when
`ENERGY_INTEGRATION_ENABLED`:

- `energy_hour_kwh` — kWh so far this `ENERGY_TZ` clock hour
- `energy_hour_start` — ISO start of that hour (HA `last_reset`)
- `energy_hour_quality` — `ok` / `estimated` / `degraded`
- `energy_hour_gap_seconds` — unaccounted seconds this hour
- `energy_last_hour_kwh` — last completed hour (meter-sourced after the `:00` fix-up)
- `energy_last_hour_source` — `integrated` / `meter`
- `energy_last_hour_quality` — `ok` / `estimated` / `degraded` / `corrected`
- `energy_last_hour_error_kwh` — integrated − meter for the last hour (when known)
- `energy_hour_start_total_wh` / `energy_prev_hour_start_total_wh` — internal, for resume

---

## Home Assistant

Discovery is automatic. Device: **`Tibber Pulse <meter_id>`**, 21 entities (power,
reactive power, current L1–L3, voltage L1–L3, cumulative energy import/export, reactive
energy import/export, energy this hour / last hour, energy-this-hour quality + a `problem`
binary_sensor + last-hour integration error, meter clock, last-decoded). The cumulative
energy sensors use `state_class: total_increasing` (Wh) and only advance hourly — fine for
the Energy dashboard. `energy_hour` uses `state_class: total` with a `last_reset` of
`energy_hour_start` (and carries `quality` / `gap_seconds` / `source` as JSON attributes).

Discovery configs are retained. To remove the device, clear them (note the two
`binary_sensor` topics):
```bash
set -a; . ./.env; set +a; ID=$METER_ID_DEFAULT
for k in active_power_import active_power_export reactive_power_import reactive_power_export \
  current_l1 current_l2 current_l3 voltage_l1 voltage_l2 voltage_l3 \
  energy_import energy_export reactive_energy_import reactive_energy_export \
  energy_hour energy_last_hour energy_hour_quality energy_last_hour_error \
  meter_datetime last_decoded; do
  mosquitto_pub -h "$LOCAL_HOST" -u "$LOCAL_USERNAME" -P "$LOCAL_PASSWORD" -r -n \
    -t "homeassistant/sensor/tibber_pulse_$ID/$k/config"
done
mosquitto_pub -h "$LOCAL_HOST" -u "$LOCAL_USERNAME" -P "$LOCAL_PASSWORD" -r -n \
  -t "homeassistant/binary_sensor/tibber_pulse_$ID/energy_hour_degraded/config"
```

---

## Troubleshooting

```bash
set -a; . ./.env; set +a     # broker creds for the commands below
ssh root@10.1.1.200 'docker logs -f --tail 50 tibber-pulse-decoder'
mosquitto_sub -h "$LOCAL_HOST" -u "$LOCAL_USERNAME" -P "$LOCAL_PASSWORD" -t 'tibber/pulse/#' -v
mosquitto_sub -h "$LOCAL_HOST" -u "$LOCAL_USERNAME" -P "$LOCAL_PASSWORD" -t 'puls/publish' -C 3 | xxd | head   # raw feed
```

- **`status` stuck at `offline`** — container down or can't reach the broker. Check logs;
  the LWT fires on ungraceful disconnect.
- **No `state` updates but raw `puls/publish` is flowing** — run with `LOG_LEVEL=DEBUG`;
  look for "no decodable frames". If the Pulse firmware changed its framing, revisit
  `extract_hdlc_frames` (ported from asbjornhansen/amshan-homeassistant `metercon.py`).
- **Energy fields never appear** — the *cumulative registers* (`active_power_import_total`
  etc.) only come in the top-of-hour frame; wait past `:00`. `energy_hour_kwh` is derived
  locally and should appear on the first decode that carries `active_power_import`.
- **`batching_disable` keeps being re-sent** — the Pulse hasn't applied it yet; the decoder
  re-sends every 5 min while it still sees multi-frame messages. Check `puls/publish` with
  `xxd`: a single frame has two `7e` bytes, buffered mode has 8–9. `BATCHING_AUTO_DISABLE=false`
  stops the nudge.
- **`energy_hour_quality` stuck at `degraded`** — a gap > `ENERGY_GAP_ESTIMATE_SEC` (45 s)
  happened this hour, so `energy_hour_kwh` is an under-count. It clears at the next `:00`;
  that hour's `energy_last_hour_kwh` is then corrected from the meter register
  (`energy_last_hour_source: meter`) and `energy_last_hour_error_kwh` shows the drift.
- **`energy_hour_kwh` reset to ~0 after a restart** — the retained-state resume failed
  (broker lost the retained `…/state`, or `ENERGY_RESUME_FROM_MQTT=false`). The hour is
  marked `degraded` and self-corrects at `:00`.
- **`current_l3` ~0.7 A at idle** — real meter reading, not a bug.

---

## Local development

```bash
.venv/bin/pip install -r tibber_pulse_decoder/requirements.txt
set -a; . ./.env; set +a
MQTT_CLIENT_ID=pulse-dev OUT_PREFIX=test/pulse HASS_DISCOVERY_ENABLED=false BATCHING_AUTO_DISABLE=false \
  LOG_LEVEL=DEBUG .venv/bin/python tibber_pulse_decoder/pulse_decoder.py
```

Connects to the same broker; publishes to the same retained topics as production (there is
one meter, so this is generally safe — it just overwrites the same retained values).

---

## Uninstall

```bash
set -a; . ./.env; set +a
ssh root@10.1.1.200 '
  docker rm -f tibber-pulse-decoder
  docker rmi tibber-pulse-decoder:latest
  rm -rf /mnt/user/appdata/tibber-pulse-decoder
'
mosquitto_pub -h "$LOCAL_HOST" -u "$LOCAL_USERNAME" -P "$LOCAL_PASSWORD" -r -n -t tibber/pulse/status
# + clear the HA discovery configs (see Home Assistant section)
```

---

## HA app (Home Assistant OS)

HA OS has no user-facing Docker, so the decoder ships as an **app** (formerly "add-on").
The repo itself is an app repository (`repository.yaml` + the `tibber_pulse_decoder/`
folder). The Supervisor builds the image **on the user's box** from
`tibber_pulse_decoder/Dockerfile` (`ghcr.io/home-assistant/base-python`, multi-arch
aarch64/amd64). No registry or CI is involved.

- **Broker**: `services: mqtt:want`. `run.sh` reads host/port/user/password from the
  Mosquitto broker app through `bashio::services mqtt`. The `mqtt_*` options override it for
  other brokers.
- **Options → env**: `run.sh` maps `config.yaml` options to the env vars in the table above.
  Gap thresholds, discovery prefix and resume keep the script defaults.
- **User setup** (Mosquitto, a broker login for the Pulse, pointing the Pulse Bridge at it,
  installing): see `tibber_pulse_decoder/DOCS.md`, shown in the app's Documentation tab.
- **Releasing an update**: change code → bump `version` in `tibber_pulse_decoder/config.yaml` +
  `CHANGELOG.md` → push. Users get an Update button (Supervisor polls repos; they can
  force it with ⋮ → Check for updates).
- Built per the 2026 app conventions: explicit `FROM` in the Dockerfile, no `build.yaml`
  (removed in Supervisor 2026.04), and `init: false` for the s6-overlay v3 base image.

---

## Key dependencies / references

- `amshan==2.1.1` (PyPI) — DLMS/COSEM + Aidon/Kaifa/Kamstrup decoding. **Imports as `han`.**
  Only dep is `construct`.
- HDLC framing approach ported from `asbjornhansen/amshan-homeassistant`
  (`fix/batched-hdlc-frames` branch), `custom_components/amshan/metercon.py`.
- `toreamun/amshan` repo root `main_mqtt.py` — the serial-input template this service mirrors.
