#!/usr/bin/env python3
"""Decode the Tibber Pulse raw HAN feed and republish it as clean JSON + HA discovery.

The Tibber Pulse publishes to ``puls/publish`` (topic configurable). Two payload kinds
arrive there:

* ``{"status": {...}}`` device-health JSON -- ignored here.
* Binary "buffered mode" payloads: several concatenated HDLC/DLMS frames, each preceded
  by a short ``HAN`` preamble and delimited by ``0x7E``. Typically 3 short frames
  (active power only) plus one long Aidon list frame (meter id/type, power, current
  L1-L3, voltage L1-L3, and -- once per hour -- clock + cumulative energy registers).

The binary payload is a protobuf-ish envelope: an outer stream counter followed by one
record per telegram, each carrying a device-uptime timestamp (ms), a ``HAN`` tag and the
raw ``0x7E``-delimited HDLC frame. We parse that envelope (falling back to a bare
``0x7E`` scan if the shape ever changes), decode each frame with
:class:`han.autodecoder.AutoDecoder` (``pip install amshan==2.1.1`` -- imported as
``han``), feed each telegram's active power into the hourly energy integrator at its own
timestamp, merge all results into a running snapshot (so the hourly-only energy fields
persist between top-of-hour frames) and publish that snapshot as one retained JSON message
on ``<OUT_PREFIX>/<meter_id>/state``. On the first successful decode we also publish
retained Home Assistant MQTT discovery configs for the meter.

The energy integrator resumes from that retained snapshot on boot, classifies data gaps by
length (``ok`` / ``estimated`` / ``degraded`` in ``energy_hour_quality``), and at each
``:00`` reconciles the just-closed hour against the meter's own cumulative register.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import time
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import paho.mqtt.client as mqtt
from han.autodecoder import AutoDecoder
from han.hdlc import HdlcFrameReader

# --- configuration (env; broker settings come from .env on Unraid, run.sh in the HA app) --
LOCAL_HOST = os.environ.get("LOCAL_HOST", "localhost")
LOCAL_PORT = int(os.environ.get("LOCAL_PORT", "1883"))
LOCAL_USERNAME = os.environ.get("LOCAL_USERNAME", "")
LOCAL_PASSWORD = os.environ.get("LOCAL_PASSWORD", "")
MQTT_CLIENT_ID = os.environ.get("MQTT_CLIENT_ID", "tibber-pulse-decoder")

PULSE_TOPIC = os.environ.get("PULSE_TOPIC", "puls/publish")
OUT_PREFIX = os.environ.get("OUT_PREFIX", "tibber/pulse").rstrip("/")
STATUS_TOPIC = f"{OUT_PREFIX}/status"

HASS_DISCOVERY_ENABLED = os.environ.get("HASS_DISCOVERY_ENABLED", "true").lower() == "true"
HASS_DISCOVERY_PREFIX = os.environ.get("HASS_DISCOVERY_PREFIX", "homeassistant")
# used only until a frame reports meter_id; empty = publish nothing until one does
METER_ID_DEFAULT = os.environ.get("METER_ID_DEFAULT", "")

# --- batching auto-disable: nudge the Pulse out of buffered mode -------------------
PULSE_CMD_TOPIC = os.environ.get("PULSE_CMD_TOPIC", "puls/subscribe")
BATCHING_AUTO_DISABLE = os.environ.get("BATCHING_AUTO_DISABLE", "true").lower() == "true"
BATCHING_DISABLE_CMD = "batching_disable true"
BATCHING_RESEND_SEC = 300      # re-send while batched messages keep arriving
BATCHING_HEARTBEAT_SEC = 1800  # re-send unconditionally at this cadence

# --- hourly energy integrator ----------------------------------------------------
ENERGY_INTEGRATION_ENABLED = os.environ.get("ENERGY_INTEGRATION_ENABLED", "true").lower() == "true"
ENERGY_TZ = os.environ.get("ENERGY_TZ", "Europe/Oslo")
# gap = seconds with no data. <= OK: fill silently. <= ESTIMATE: fill with mean power,
# mark the hour "estimated". > ESTIMATE: don't fill, mark the hour "degraded".
ENERGY_GAP_OK_SEC = float(os.environ.get("ENERGY_GAP_OK_SEC", "15"))
ENERGY_GAP_ESTIMATE_SEC = float(os.environ.get("ENERGY_GAP_ESTIMATE_SEC", "45"))
# on startup, resume the integrator from the retained <OUT_PREFIX>/<id>/state message
ENERGY_RESUME_FROM_MQTT = os.environ.get("ENERGY_RESUME_FROM_MQTT", "true").lower() == "true"
_tz = ZoneInfo(ENERGY_TZ)

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("pulse_decoder")
# han.hdlc logs a warning for every short/garbage frame; the Pulse preambles trip it
# constantly, so keep that decoder quiet.
logging.getLogger("han").setLevel(logging.ERROR)

# --- Home Assistant discovery: HA sensor key -> decoded field name ------------------
# Fields come from han.obis_map. Energy totals / meter_datetime only appear in the
# top-of-hour frame; value_template uses `| default('')` so HA tolerates their absence.
SENSORS: dict[str, dict[str, Any]] = {
    "active_power_import": {"name": "Active Power Import", "field": "active_power_import", "unit": "W", "device_class": "power", "state_class": "measurement"},
    "active_power_export": {"name": "Active Power Export", "field": "active_power_export", "unit": "W", "device_class": "power", "state_class": "measurement"},
    "reactive_power_import": {"name": "Reactive Power Import", "field": "reactive_power_import", "unit": "var", "device_class": "reactive_power", "state_class": "measurement"},
    "reactive_power_export": {"name": "Reactive Power Export", "field": "reactive_power_export", "unit": "var", "device_class": "reactive_power", "state_class": "measurement"},
    "current_l1": {"name": "Current L1", "field": "current_l1", "unit": "A", "device_class": "current", "state_class": "measurement"},
    "current_l2": {"name": "Current L2", "field": "current_l2", "unit": "A", "device_class": "current", "state_class": "measurement"},
    "current_l3": {"name": "Current L3", "field": "current_l3", "unit": "A", "device_class": "current", "state_class": "measurement"},
    "voltage_l1": {"name": "Voltage L1", "field": "voltage_l1", "unit": "V", "device_class": "voltage", "state_class": "measurement"},
    "voltage_l2": {"name": "Voltage L2", "field": "voltage_l2", "unit": "V", "device_class": "voltage", "state_class": "measurement"},
    "voltage_l3": {"name": "Voltage L3", "field": "voltage_l3", "unit": "V", "device_class": "voltage", "state_class": "measurement"},
    "energy_import": {"name": "Energy Import (Cumulative)", "field": "active_power_import_total", "unit": "Wh", "device_class": "energy", "state_class": "total_increasing"},
    "energy_export": {"name": "Energy Export (Cumulative)", "field": "active_power_export_total", "unit": "Wh", "device_class": "energy", "state_class": "total_increasing"},
    "reactive_energy_import": {"name": "Reactive Energy Import (Cumulative)", "field": "reactive_power_import_total", "unit": "varh", "device_class": None, "state_class": "total_increasing"},
    "reactive_energy_export": {"name": "Reactive Energy Export (Cumulative)", "field": "reactive_power_export_total", "unit": "varh", "device_class": None, "state_class": "total_increasing"},
    "energy_hour": {"name": "Energy This Hour", "field": "energy_hour_kwh", "unit": "kWh", "device_class": "energy", "state_class": "total", "last_reset_field": "energy_hour_start", "attributes_fields": ["energy_hour_quality", "energy_hour_gap_seconds", "energy_last_hour_source"]},
    "energy_last_hour": {"name": "Energy Last Hour", "field": "energy_last_hour_kwh", "unit": "kWh", "device_class": "energy", "state_class": "total"},
    "energy_hour_quality": {"name": "Energy This Hour Quality", "field": "energy_hour_quality", "unit": None, "device_class": None, "state_class": None, "icon": "mdi:shield-check"},
    "energy_hour_degraded": {"name": "Energy This Hour Degraded", "field": "energy_hour_quality", "unit": None, "device_class": "problem", "state_class": None, "component": "binary_sensor", "value_template": "{{ 'ON' if value_json.energy_hour_quality == 'degraded' else 'OFF' }}", "payload_on": "ON", "payload_off": "OFF"},
    "energy_last_hour_error": {"name": "Energy Last Hour Error", "field": "energy_last_hour_error_kwh", "unit": "kWh", "device_class": None, "state_class": "measurement", "icon": "mdi:delta"},
    # meter_datetime is the meter's own RTC. amshan only makes it tz-aware when the DLMS
    # "deviation" field is set; this Aidon leaves it unspecified, so we get a naive
    # local-standard-time value (~1 h behind CEST in summer). Publish it as a plain
    # string -- HA's `timestamp` device_class rejects a value without an offset.
    "meter_datetime": {"name": "Meter Clock", "field": "meter_datetime", "unit": None, "device_class": None, "state_class": None, "icon": "mdi:clock-outline"},
    "last_decoded": {"name": "Last Decoded", "field": "decoded_at", "unit": None, "device_class": "timestamp", "state_class": None},
}

# --- runtime state ----------------------------------------------------------------
_decoder = AutoDecoder()
_last_state: dict[str, Any] = {}
_discovered: set[str] = set()
_batching_cmd_sent_at: float = 0.0  # time.monotonic() of the last batching_disable send
_energy: dict[str, Any] = {
    "hour_start": None,               # datetime (tz-aware, start of the current local hour)
    "wh": 0.0,                        # energy accumulated so far this hour
    "last_ts": None,                  # datetime (UTC) of the last integrated sample
    "last_power": None,               # W
    "gap_seconds": 0.0,               # unaccounted seconds so far this hour
    "quality": "ok",                  # "ok" | "estimated" | "degraded"
    "hour_start_total_wh": None,      # meter active_power_import_total latched at this hour's start
    "prev_hour_start_total_wh": None, # ... and at the previous hour's start (for the :00 fix-up)
    "last_hour_wh": None,             # last completed hour, Wh (integrated, or meter after fix-up)
    "last_hour_quality": None,        # "ok" | "estimated" | "degraded" | "corrected"
    "last_hour_source": None,         # "integrated" | "meter"
    "last_hour_meter_wh": None,       # last completed hour from the register delta, when known
    "last_hour_error_wh": None,       # integrated - meter, last completed hour
}
_resumed: bool = False               # have we consumed the retained state snapshot yet?
_resume_deadline: float = 0.0        # time.monotonic() until which we defer integrating


def extract_hdlc_frames(payload: bytes) -> list[bytes]:
    """Return every 0x7E-delimited slice from a (possibly batched) payload.

    Ported from asbjornhansen/amshan-homeassistant metercon.py: take each pair of
    adjacent flag-sequence offsets and keep the inclusive slice if it is long enough
    to be a plausible HDLC frame. Identical slices (the repeated short frames) are
    de-duplicated.
    """
    offsets = [i for i, b in enumerate(payload) if b == HdlcFrameReader.FLAG_SEQUENCE]
    frames: list[bytes] = []
    seen: set[bytes] = set()
    for start, end in zip(offsets, offsets[1:]):
        chunk = payload[start : end + 1]
        if len(chunk) >= 10 and chunk not in seen:
            seen.add(chunk)
            frames.append(chunk)
    return frames


def _read_varint(buf: bytes, i: int) -> tuple[int, int]:
    """Read a protobuf base-128 varint from ``buf`` at ``i``; return (value, next_i)."""
    val = shift = 0
    while i < len(buf):
        b = buf[i]
        i += 1
        val |= (b & 0x7F) << shift
        if not b & 0x80:
            return val, i
        shift += 7
    raise ValueError("truncated varint")


def _parse_frame_record(rec: bytes) -> tuple[int | None, bytes] | None:
    """Pull (uptime_ms, hdlc_bytes) out of one protobuf frame record; None if no frame.

    Fields: 1 (I64) = device-uptime timestamp ms, 2 (LEN) = ``HAN`` tag (skipped),
    3 (LEN) = the raw HDLC frame.
    """
    i, n = 0, len(rec)
    ts_ms: int | None = None
    hdlc: bytes | None = None
    try:
        while i < n:
            tag = rec[i]
            i += 1
            field, wire = tag >> 3, tag & 7
            if wire == 0:
                _, i = _read_varint(rec, i)
            elif wire == 1:
                if field == 1:
                    ts_ms = int.from_bytes(rec[i:i + 8], "little")
                i += 8
            elif wire == 2:
                length, i = _read_varint(rec, i)
                if field == 3:
                    hdlc = rec[i:i + length]
                i += length
            elif wire == 5:
                i += 4
            else:
                break
    except (IndexError, ValueError):
        pass
    return (ts_ms, hdlc) if hdlc else None


def _iter_frame_records(payload: bytes) -> Iterator[tuple[int | None, bytes]]:
    """Yield (uptime_ms, hdlc_bytes) per telegram from the Pulse's protobuf envelope.

    Outer shape: field 1 (VARINT) stream counter, field 2 (LEN) repeated frame record.
    Any malformed byte stops iteration (the caller then falls back to a bare 0x7E scan).
    Repeated identical short frames are *not* de-duplicated -- they are real samples at
    distinct timestamps.
    """
    i, n = 0, len(payload)
    try:
        while i < n:
            tag = payload[i]
            i += 1
            field, wire = tag >> 3, tag & 7
            if wire == 0:
                _, i = _read_varint(payload, i)
            elif wire == 1:
                i += 8
            elif wire == 2:
                length, i = _read_varint(payload, i)
                chunk = payload[i:i + length]
                i += length
                if len(chunk) != length:
                    return
                if field == 2:
                    rec = _parse_frame_record(chunk)
                    if rec is not None:
                        yield rec
            elif wire == 5:
                i += 4
            else:
                return
    except (IndexError, ValueError):
        return


def decode_frames(payload: bytes) -> list[tuple[int | None, dict[str, Any]]]:
    """Decode a raw MQTT payload into an ordered list of (uptime_ms, {field: value}).

    ``uptime_ms`` is the Pulse's per-telegram device-uptime timestamp, or ``None`` when
    the envelope could not be parsed (bare 0x7E-scan fallback).
    """
    records: list[tuple[int | None, bytes]] = list(_iter_frame_records(payload))
    if not records:
        records = [(None, chunk) for chunk in extract_hdlc_frames(payload)]

    out: list[tuple[int | None, dict[str, Any]]] = []
    for ts_ms, chunk in records:
        reader = HdlcFrameReader(use_octet_stuffing=False, use_abort_sequence=False)
        for frame in reader.read(chunk):
            if not (frame.payload and frame.is_good_ffc and frame.is_expected_length):
                continue
            try:
                decoded = _decoder.decode_message_payload(frame.payload)
            except Exception:  # noqa: BLE001 - never let a bad frame kill the loop
                log.exception("decoder crashed on frame %s", frame.as_bytes.hex())
                continue
            if decoded:
                out.append((ts_ms, decoded))
    return out


def decode_message(payload: bytes) -> dict[str, Any]:
    """Decode one raw MQTT payload into a merged {field: value} dict (may be empty)."""
    merged: dict[str, Any] = {}
    for _ts_ms, decoded in decode_frames(payload):
        merged.update(decoded)
    return merged


def _jsonable(value: Any) -> Any:
    return value.isoformat() if isinstance(value, datetime) else value


def _looks_batched(payload: bytes) -> bool:
    """True if the payload holds more than one HDLC frame (Pulse buffered mode).

    A lone HDLC frame is delimited by exactly two 0x7E flags; a buffered-mode
    message concatenates ~4 frames and carries 8-9.
    """
    return payload.count(0x7E) > 2


def _maybe_send_batching_disable(client: mqtt.Client, *, batched: bool, force: bool = False) -> None:
    """Publish ``batching_disable true`` to the Pulse, rate-limited.

    Sent on every reconnect (``force``), then unconditionally every
    ``BATCHING_HEARTBEAT_SEC`` and every ``BATCHING_RESEND_SEC`` while batched
    messages keep arriving. The Pulse never acks, hence the repeats.
    """
    if not BATCHING_AUTO_DISABLE:
        return
    global _batching_cmd_sent_at
    now = time.monotonic()
    interval = BATCHING_RESEND_SEC if batched else BATCHING_HEARTBEAT_SEC
    if not force and now - _batching_cmd_sent_at < interval:
        return
    client.publish(PULSE_CMD_TOPIC, BATCHING_DISABLE_CMD, qos=1)
    _batching_cmd_sent_at = now
    log.info(
        "sent '%s' to %s (batched=%s force=%s)",
        BATCHING_DISABLE_CMD, PULSE_CMD_TOPIC, batched, force,
    )


def _classify_gap(dt: float) -> str:
    if dt <= ENERGY_GAP_OK_SEC:
        return "ok"
    if dt <= ENERGY_GAP_ESTIMATE_SEC:
        return "estimated"
    return "degraded"


def _apply_gap(power_w: float, dt: float, *, fill: bool = True) -> None:
    """Advance ``_energy['wh']`` across ``dt`` seconds and update the quality flag.

    ``dt`` <= OK: fill silently. <= ESTIMATE: fill with mean power, mark "estimated".
    > ESTIMATE: do not fill, mark "degraded". Quality only escalates within an hour;
    ``_finalize_hour`` resets it.
    """
    if dt <= 0:
        return
    e = _energy
    band = _classify_gap(dt)
    if fill and band != "degraded":
        p = power_w if e["last_power"] is None else (e["last_power"] + power_w) / 2
        e["wh"] += p * dt / 3600
    if band != "ok":
        e["gap_seconds"] += dt
        if band == "degraded" or e["quality"] == "ok":
            e["quality"] = band


def _finalize_hour(new_hour: datetime) -> None:
    """Close the running hour: snapshot totals, reset the accumulator for ``new_hour``."""
    e = _energy
    e["last_hour_wh"] = e["wh"]
    e["last_hour_quality"] = e["quality"]
    e["last_hour_source"] = "integrated"
    e["last_hour_meter_wh"] = None
    e["last_hour_error_wh"] = None
    e["prev_hour_start_total_wh"] = e["hour_start_total_wh"]
    e["hour_start_total_wh"] = None
    e["wh"] = 0.0
    e["gap_seconds"] = 0.0
    e["quality"] = "ok"
    e["hour_start"] = new_hour


def _integrate_energy(power_w: float, now: datetime) -> None:
    """Trapezoidal-integrate active power (W) into per-local-hour energy (Wh).

    Every interval is routed through :func:`_apply_gap`, which fills or flags it by
    length. ``_energy['wh']`` is the running total for the current local-clock hour.
    """
    e = _energy
    hour = now.astimezone(_tz).replace(minute=0, second=0, microsecond=0)

    if e["hour_start"] is None:
        # cold start: begin this hour at ``now``. If we booted well into the hour with
        # nothing carried over, the hour is already incomplete -> flag it.
        into_hour = (now - hour.astimezone(timezone.utc)).total_seconds()
        e["hour_start"] = hour
        e["last_ts"], e["last_power"] = now, power_w
        _apply_gap(power_w, into_hour, fill=False)
        return

    if hour != e["hour_start"]:
        boundary = hour.astimezone(timezone.utc)  # start of the new local hour == old hour's end
        _apply_gap(power_w, (boundary - e["last_ts"]).total_seconds())
        _finalize_hour(hour)
        e["last_ts"], e["last_power"] = boundary, power_w
        _apply_gap(power_w, (now - boundary).total_seconds())
        e["last_ts"], e["last_power"] = now, power_w
        return

    _apply_gap(power_w, (now - e["last_ts"]).total_seconds())
    e["last_ts"], e["last_power"] = now, power_w


def _register_fixup(total_wh: float) -> None:
    """At the first top-of-hour ``active_power_import_total`` of a new hour, reconcile the
    just-closed hour against the meter's own register delta (which is exact)."""
    e = _energy
    if e["hour_start_total_wh"] is not None:
        return  # already recorded this hour's starting register
    e["hour_start_total_wh"] = total_wh
    prev = e["prev_hour_start_total_wh"]
    if prev is None or e["last_hour_wh"] is None:
        return  # no delta available (first run, or down over the previous :00)
    meter_wh = total_wh - prev
    if meter_wh < 0:
        return  # register went backwards -> ignore
    integrated_wh = e["last_hour_wh"]
    e["last_hour_meter_wh"] = meter_wh
    e["last_hour_error_wh"] = integrated_wh - meter_wh
    e["last_hour_wh"] = meter_wh
    e["last_hour_source"] = "meter"
    if e["last_hour_quality"] == "degraded":
        e["last_hour_quality"] = "corrected"
    log.info(
        "hour closed: integrated %.3f kWh vs meter %.3f kWh (error %+.3f kWh), quality=%s",
        integrated_wh / 1000, meter_wh / 1000, e["last_hour_error_wh"] / 1000,
        e["last_hour_quality"],
    )


def _write_energy_fields() -> None:
    """Copy the integrator's state into ``_last_state`` for publishing."""
    e = _energy
    if e["hour_start"] is None:
        return
    _last_state["energy_hour_kwh"] = round(e["wh"] / 1000, 4)
    _last_state["energy_hour_start"] = e["hour_start"].isoformat()
    _last_state["energy_hour_quality"] = e["quality"]
    _last_state["energy_hour_gap_seconds"] = round(e["gap_seconds"], 1)
    if e["hour_start_total_wh"] is not None:
        _last_state["energy_hour_start_total_wh"] = e["hour_start_total_wh"]
    if e["prev_hour_start_total_wh"] is not None:
        _last_state["energy_prev_hour_start_total_wh"] = e["prev_hour_start_total_wh"]
    if e["last_hour_wh"] is not None:
        _last_state["energy_last_hour_kwh"] = round(e["last_hour_wh"] / 1000, 4)
        _last_state["energy_last_hour_quality"] = e["last_hour_quality"]
        _last_state["energy_last_hour_source"] = e["last_hour_source"]
    if e["last_hour_error_wh"] is not None:
        _last_state["energy_last_hour_error_kwh"] = round(e["last_hour_error_wh"] / 1000, 4)


def _resume_from_state(js: dict[str, Any], now: datetime) -> None:
    """Restore ``_energy`` from a retained state snapshot, if it is for the current hour."""
    e = _energy
    raw = js.get("energy_hour_start")
    try:
        hour_start = datetime.fromisoformat(raw) if raw else None
    except (TypeError, ValueError):
        hour_start = None
    cur_hour = now.astimezone(_tz).replace(minute=0, second=0, microsecond=0)
    if hour_start is None or hour_start != cur_hour:
        log.info("no usable retained energy state (hour_start=%s); cold start", raw)
        return
    try:
        last_ts = datetime.fromisoformat(js["decoded_at"])
    except (KeyError, TypeError, ValueError):
        last_ts = None
    e["hour_start"] = cur_hour
    e["wh"] = float(js.get("energy_hour_kwh", 0.0)) * 1000
    e["gap_seconds"] = float(js.get("energy_hour_gap_seconds", 0.0))
    e["quality"] = js.get("energy_hour_quality", "ok")
    e["last_ts"], e["last_power"] = last_ts, None
    e["hour_start_total_wh"] = js.get("energy_hour_start_total_wh")
    e["prev_hour_start_total_wh"] = js.get("energy_prev_hour_start_total_wh")
    for k_json, k_state in (("energy_last_hour_kwh", "last_hour_wh"),
                            ("energy_last_hour_error_kwh", "last_hour_error_wh")):
        v = js.get(k_json)
        if isinstance(v, (int, float)):
            e[k_state] = v * 1000
    e["last_hour_quality"] = js.get("energy_last_hour_quality")
    e["last_hour_source"] = js.get("energy_last_hour_source")
    log.info(
        "resumed energy: %.4f kWh into hour %s, quality=%s, last data %s",
        e["wh"] / 1000, cur_hour.isoformat(), e["quality"], last_ts,
    )


def _feed_energy(frames: list[tuple[int | None, dict[str, Any]]], now: datetime) -> None:
    """Feed each telegram's active power into :func:`_integrate_energy` at its own time.

    The newest frame is anchored to message-arrival wall-clock ``now``; older frames sit
    behind it by their intra-message uptime deltas (the only trustworthy part of a
    counter that resets on device reboot). Falls back to a single sample at ``now`` when
    the envelope carried no usable per-frame timestamps.
    """
    samples = [
        (ts_ms, float(d["active_power_import"]))
        for ts_ms, d in frames
        if ts_ms is not None and isinstance(d.get("active_power_import"), (int, float))
    ]
    if samples:
        ts_last = samples[-1][0]
        for ts_ms, power in samples:
            _integrate_energy(power, now - timedelta(milliseconds=ts_last - ts_ms))
        return
    for _ts_ms, d in reversed(frames):
        power = d.get("active_power_import")
        if isinstance(power, (int, float)):
            _integrate_energy(float(power), now)
            return


def publish_discovery(client: mqtt.Client, meter_id: str) -> None:
    device = {
        "identifiers": [f"tibber_pulse_{meter_id}"],
        "name": f"Tibber Pulse {meter_id}",
        "manufacturer": _last_state.get("meter_manufacturer", "Aidon"),
        "model": "Aidon_V2 (via Tibber Pulse)",
    }
    state_topic = f"{OUT_PREFIX}/{meter_id}/state"
    for key, sensor in SENSORS.items():
        component = sensor.get("component", "sensor")
        config: dict[str, Any] = {
            "name": sensor["name"],
            "unique_id": f"tibber_pulse_{meter_id}_{key}",
            "state_topic": state_topic,
            "value_template": sensor.get(
                "value_template", f"{{{{ value_json.{sensor['field']} | default('') }}}}"
            ),
            "availability_topic": STATUS_TOPIC,
            "payload_available": "online",
            "payload_not_available": "offline",
            "device": device,
        }
        if sensor["unit"]:
            config["unit_of_measurement"] = sensor["unit"]
        if sensor["device_class"]:
            config["device_class"] = sensor["device_class"]
        if sensor["state_class"]:
            config["state_class"] = sensor["state_class"]
        if sensor.get("icon"):
            config["icon"] = sensor["icon"]
        if sensor.get("payload_on"):
            config["payload_on"] = sensor["payload_on"]
        if sensor.get("payload_off"):
            config["payload_off"] = sensor["payload_off"]
        if sensor.get("last_reset_field"):
            config["last_reset_value_template"] = (
                f"{{{{ value_json.{sensor['last_reset_field']} | default('') }}}}"
            )
        if sensor.get("attributes_fields"):
            config["json_attributes_topic"] = state_topic
            pairs = ", ".join(
                f"'{f}': value_json.{f} | default(None)" for f in sensor["attributes_fields"]
            )
            config["json_attributes_template"] = f"{{{{ {{{pairs}}} | tojson }}}}"
        topic = f"{HASS_DISCOVERY_PREFIX}/{component}/tibber_pulse_{meter_id}/{key}/config"
        client.publish(topic, json.dumps(config), qos=1, retain=True)
    log.info("published %d HA discovery configs for meter %s", len(SENSORS), meter_id)


def on_connect(client: mqtt.Client, userdata, flags, rc, properties=None) -> None:
    global _resume_deadline
    log.info("connected to %s:%s rc=%s (%s)", LOCAL_HOST, LOCAL_PORT, rc, mqtt.connack_string(rc))
    if rc == 0:
        client.publish(STATUS_TOPIC, "online", qos=1, retain=True)
        client.subscribe(PULSE_TOPIC, qos=0)
        if ENERGY_INTEGRATION_ENABLED and ENERGY_RESUME_FROM_MQTT and not _resumed:
            _resume_deadline = time.monotonic() + 5.0
            client.subscribe(f"{OUT_PREFIX}/+/state", qos=0)
            log.info("subscribed to %s and %s/+/state (resume)", PULSE_TOPIC, OUT_PREFIX)
        else:
            log.info("subscribed to %s", PULSE_TOPIC)
        _maybe_send_batching_disable(client, batched=False, force=True)


def on_disconnect(client: mqtt.Client, userdata, flags, rc, properties=None) -> None:
    log.warning("disconnected rc=%s", rc)


def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage) -> None:
    global _resumed
    payload = msg.payload

    if msg.topic != PULSE_TOPIC:
        # our own retained <OUT_PREFIX>/<id>/state, delivered once on (re)subscribe
        if not _resumed:
            _resumed = True
            client.unsubscribe(f"{OUT_PREFIX}/+/state")
            if ENERGY_INTEGRATION_ENABLED and ENERGY_RESUME_FROM_MQTT:
                try:
                    js = json.loads(payload)
                    if isinstance(js, dict):
                        _resume_from_state(js, datetime.now(timezone.utc))
                except (ValueError, TypeError):
                    log.warning("could not parse retained state for resume")
        return

    if payload[:1] in (b"{", b"["):
        log.debug("status json (%d bytes), ignored", len(payload))
        return

    _maybe_send_batching_disable(client, batched=_looks_batched(payload))

    now = datetime.now(timezone.utc)
    frames = decode_frames(payload)
    if not frames:
        log.debug("no decodable frames in %d byte payload", len(payload))
        return

    merged: dict[str, Any] = {}
    for _ts_ms, decoded in frames:
        merged.update(decoded)
    _last_state.update({k: _jsonable(v) for k, v in merged.items()})
    _last_state["decoded_at"] = now.isoformat()

    if ENERGY_INTEGRATION_ENABLED:
        if not _resumed and ENERGY_RESUME_FROM_MQTT and time.monotonic() < _resume_deadline:
            log.debug("deferring energy integration until retained state arrives")
        else:
            _resumed = True  # deadline passed (or resume done) -> stop waiting
            _feed_energy(frames, now)
            total = merged.get("active_power_import_total")
            if isinstance(total, (int, float)):
                _register_fixup(float(total))
            _write_energy_fields()

    meter_id = str(_last_state.get("meter_id") or METER_ID_DEFAULT)
    if not meter_id:
        log.debug("meter_id not decoded yet; holding publish")
        return

    if HASS_DISCOVERY_ENABLED and meter_id not in _discovered:
        publish_discovery(client, meter_id)
        _discovered.add(meter_id)

    client.publish(f"{OUT_PREFIX}/{meter_id}/state", json.dumps(_last_state), qos=0, retain=True)
    log.info(
        "meter %s: P=%sW I=(%s,%s,%s)A U=(%s,%s,%s)V",
        meter_id,
        _last_state.get("active_power_import"),
        _last_state.get("current_l1"), _last_state.get("current_l2"), _last_state.get("current_l3"),
        _last_state.get("voltage_l1"), _last_state.get("voltage_l2"), _last_state.get("voltage_l3"),
    )


def main() -> None:
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=MQTT_CLIENT_ID,
    )
    if LOCAL_USERNAME:
        client.username_pw_set(LOCAL_USERNAME, LOCAL_PASSWORD or None)
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    client.will_set(STATUS_TOPIC, "offline", qos=1, retain=True)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    def _shutdown(*_a) -> None:
        log.info("shutting down")
        try:
            meter_id = str(_last_state.get("meter_id") or METER_ID_DEFAULT)
            if _last_state and meter_id:
                _write_energy_fields()
                client.publish(
                    f"{OUT_PREFIX}/{meter_id}/state", json.dumps(_last_state), qos=1, retain=True
                )
            client.publish(STATUS_TOPIC, "offline", qos=1, retain=True)
        finally:
            client.disconnect()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    log.info("connecting to %s:%s ...", LOCAL_HOST, LOCAL_PORT)
    client.connect(LOCAL_HOST, LOCAL_PORT, keepalive=60)
    client.loop_forever(retry_first_connection=True)


if __name__ == "__main__":
    main()
