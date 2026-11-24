# Tibber Pulse HAN decoder

Reads the **raw HAN feed** a Tibber Pulse publishes to your local MQTT broker. Decodes it
locally (Aidon, Kaifa and Kamstrup meters) and creates a **Tibber Pulse &lt;meter id&gt;** device
in Home Assistant through MQTT discovery. It covers power, current, voltage, cumulative
energy and an hourly energy integrator. No Tibber cloud is involved.

## Before you start

1. **Mosquitto broker app** installed and running. The **MQTT** integration must be set
   up in Home Assistant.
2. **A broker login for the Pulse.** Either create a Home Assistant user (e.g. `pulse`), or add
   one under `logins` in the Mosquitto broker app's configuration.
3. **The Pulse Bridge must publish to your broker**, not to Tibber's cloud. Open the bridge's
   web UI (the admin password is on the bridge's QR sticker) and set:
   - MQTT host: your Home Assistant IP, port `1883`
   - the login from step 2
   - publish topic: `puls/publish`, subscribe topic: `puls/subscribe`

   Note: while the bridge publishes locally, live data in the Tibber app stops.

   Check that it works: under **Settings → Devices & services → MQTT → Configure**, listen to
   `puls/publish`. Binary messages should arrive every few seconds.

## Install

1. **Settings → Apps → App store → ⋮ → Repositories** (older HA: *Add-ons → Add-on store*), then add
   `https://github.com/taraldb/tibber-pulse-decoder`.
2. Install **Tibber Pulse HAN decoder**. The first build takes a minute or two.
3. **Start**, then open the **Log** tab. You should see `connected ... rc=0 (Connection Accepted.)`,
   followed by `meter <id>: P=...W` lines.
4. The device **Tibber Pulse &lt;meter id&gt;** appears under the MQTT integration.

## Configuration

The defaults match a standard Pulse setup. Normally you change nothing.

| option | default | |
|---|---|---|
| `pulse_topic` | `puls/publish` | where the Pulse publishes |
| `pulse_cmd_topic` | `puls/subscribe` | where the decoder sends `batching_disable true` |
| `batching_auto_disable` | `true` | keeps the Pulse out of buffered mode (~2.5 s updates) |
| `out_prefix` | `tibber/pulse` | decoded JSON goes to `<prefix>/<meter_id>/state` |
| `hass_discovery_enabled` | `true` | auto-create the device and entities |
| `energy_integration_enabled` | `true` | "energy this hour / last hour" sensors |
| `energy_tz` | `Europe/Oslo` | hour boundary for the integrator |
| `log_level` | `INFO` | `DEBUG` logs per-frame detail |
| `mqtt_host` / `mqtt_port` / `mqtt_username` / `mqtt_password` | empty | only needed for a broker other than the Mosquitto app |

## Energy dashboard

Use **Energy import** (and **Energy export**, if you have solar) as the grid consumption and
return sensors. They come from the meter's own register, which updates once per hour.
**Energy this hour** is a live estimate. While **Energy this hour degraded** is on, data was
missing this hour; the value self-corrects at the next `:00`.

## Troubleshooting

- **Nothing after `connected`**: the Pulse isn't publishing to this broker or topic. Listen
  on `puls/publish` (see above).
- **`Connection Refused: not authorised`**: if you use the `mqtt_*` overrides, check the
  credentials. Otherwise restart the Mosquitto broker app.
- **No `meter ... P=` lines, but data arrives on `puls/publish`**: set `log_level: DEBUG`,
  restart, and look for `no decodable frames`.
