"""
Ham Radio Logbook Application
Run with: python main.py
Then open browser to: http://localhost:5000
"""

import sys
import re
import sqlite3
import socket
import threading
import time
import json
import os
import shutil
import struct
import zipfile
import requests
from datetime import datetime, date, timedelta
from flask import Flask, render_template, request, jsonify, send_file, Response
from flask_sock import Sock
import io
import subprocess
import tempfile

# Club Log application key — loaded from gitignored clublog_key.py so it is
# never published to the public repository but is bundled into the compiled exe.
try:
    from clublog_key import CLUBLOG_APP_KEY
except ImportError:
    CLUBLOG_APP_KEY = ""   # key file absent (source-only / contributor build)

app = Flask(__name__)
sock = Sock(app)

# ─── Server Config (config.json) ──────────────────────────────────────────────
# Read before Flask starts so WEB_PORT / WEB_HOST are set at bind time.
_CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
_DEFAULT_CFG = {"web_port": 5000, "web_host": "0.0.0.0"}

def _load_server_cfg():
    try:
        with open(_CONFIG_FILE) as _f:
            return {**_DEFAULT_CFG, **json.load(_f)}
    except Exception:
        return _DEFAULT_CFG.copy()

def _save_server_cfg(cfg):
    with open(_CONFIG_FILE, "w") as _f:
        json.dump(cfg, _f, indent=2)

_srv_cfg = _load_server_cfg()

# ─── App settings persistence ──────────────────────────────────────────────────
# All user-configured settings are held in runtime_settings (dict) and also
# mirrored into named globals. Without persistence these reset to defaults on
# every restart, meaning features like digital UDP auto-log would never activate.
_APP_SETTINGS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "app_settings.json")

# Auto-backup schedule persistence — survives restarts so a Daily run that
# already completed today won't fire again just because the app was restarted.
_AUTO_BACKUP_STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "auto_backup_state.json")

def _save_auto_backup_state():
    """Persist _auto_backup_status to disk (last_run survives restarts)."""
    try:
        with open(_AUTO_BACKUP_STATE_FILE, "w") as _f:
            json.dump({
                "last_run": _auto_backup_status.get("last_run"),
                "ok":       _auto_backup_status.get("ok"),
                "message":  _auto_backup_status.get("message", ""),
                "path":     _auto_backup_status.get("path", ""),
            }, _f, indent=2)
    except Exception as _e:
        print(f"Auto-backup state save error: {_e}")

def _load_auto_backup_state():
    """Read persisted schedule state into _auto_backup_status (call at startup)."""
    try:
        if not os.path.exists(_AUTO_BACKUP_STATE_FILE):
            return
        with open(_AUTO_BACKUP_STATE_FILE, "r") as _f:
            data = json.load(_f) or {}
        for _k in ("last_run", "ok", "message", "path"):
            if _k in data:
                _auto_backup_status[_k] = data[_k]
    except Exception as _e:
        print(f"Auto-backup state load error: {_e}")

def _save_app_settings():
    """Write runtime_settings to disk so they survive restarts."""
    try:
        with open(_APP_SETTINGS_FILE, "w") as _f:
            json.dump(runtime_settings, _f, indent=2)
    except Exception as _e:
        print(f"Settings save error: {_e}")

def _load_app_settings():
    """Read persisted settings and apply them to globals (called once at startup)."""
    global QRZ_USER, QRZ_PASS, QRZ_LOGBOOK_KEY, QRZ_LOGBOOK_UPLOAD_ENABLED
    global HAMQTH_USER, HAMQTH_PASS
    global LOTW_TQSL_PATH, LOTW_STATION_LOCATION, LOTW_UPLOAD_ENABLED
    global CLUBLOG_EMAIL, CLUBLOG_PASSWORD, CLUBLOG_CALLSIGN, CLUBLOG_UPLOAD_ENABLED, CLUBLOG_UPLOAD_DESIGNATOR
    global TELNET_ENABLED, TELNET_SERVER, TELNET_PORT, MY_CALLSIGN, MY_NAME, TCI_ENABLED, TCI_HOST, TCI_PORT, ITU_REGION
    global DIGITAL_UDP_ENABLED, DIGITAL_UDP_PORT, DIGITAL_TCP_ENABLED, DIGITAL_TCP_PORT
    global ROTATOR_ENABLED, ROTATOR_HOST, ROTATOR_PORT, ROTATOR_PROTOCOL, ROTATOR_AUTO, ROTATOR_DEBUG
    global BACKUP_PATH, FLRIG_ENABLED, FLRIG_HOST, FLRIG_PORT, FLRIG_DIGITAL_MODE, FLRIG_RTTY_MODE
    global AUTO_BACKUP_ENABLED, AUTO_BACKUP_INTERVAL, AUTO_BACKUP_RETENTION
    global HAMLIB_ENABLED, HAMLIB_HOST, HAMLIB_PORT
    global WINKEYER_ENABLED, WINKEYER_PORT, WINKEYER_WPM, WINKEYER_KEY_OUT, WINKEYER_MODE, WINKEYER_PTT, WINKEYER_PTT_LEAD, WINKEYER_PTT_TAIL
    global EQSL_USER, EQSL_PASS, EQSL_UPLOAD_ENABLED
    global POTA_MY_PARK, POTA_USER, POTA_PASS
    global SAT_UDP_ENABLED, SAT_UDP_PORT, SAT_ADIF_PORT, SAT_CONTROLLER_IP
    global LIGHTNING_ACCEPTED, LIGHTNING_ENABLED, LIGHTNING_RANGE, LIGHTNING_UNIT, LIGHTNING_BLITZORTUNG, LIGHTNING_NOAA
    global LIGHTNING_AMBIENT, LIGHTNING_AMBIENT_API_KEY, LIGHTNING_AMBIENT_APP_KEY
    global WIND_ENABLED, WIND_NWS_ALERTS, WIND_NWS_METAR, WIND_AMBIENT
    global WIND_THRESH_SUST, WIND_THRESH_GUST, WIND_COOLDOWN_MIN, WIND_METAR_STATION
    try:
        with open(_APP_SETTINGS_FILE) as _f:
            data = json.load(_f)
        runtime_settings.update(data)
        if data.get("qrz_user"):                QRZ_USER                  = data["qrz_user"]
        if data.get("qrz_pass"):                QRZ_PASS                  = data["qrz_pass"]
        if data.get("qrz_logbook_key"):         QRZ_LOGBOOK_KEY           = data["qrz_logbook_key"]
        if data.get("hamqth_user"):             HAMQTH_USER               = data["hamqth_user"]
        if data.get("hamqth_pass"):             HAMQTH_PASS               = data["hamqth_pass"]
        if "qrz_logbook_upload_enabled" in data: QRZ_LOGBOOK_UPLOAD_ENABLED = bool(data["qrz_logbook_upload_enabled"])
        if data.get("lotw_tqsl_path"):           LOTW_TQSL_PATH           = data["lotw_tqsl_path"]
        if "lotw_station_location" in data:     LOTW_STATION_LOCATION    = data["lotw_station_location"]
        if "lotw_upload_enabled" in data:       LOTW_UPLOAD_ENABLED      = bool(data["lotw_upload_enabled"])
        if data.get("clublog_email"):              CLUBLOG_EMAIL              = data["clublog_email"]
        if data.get("clublog_password"):           CLUBLOG_PASSWORD           = data["clublog_password"]
        if "clublog_callsign" in data:             CLUBLOG_CALLSIGN           = data["clublog_callsign"]
        if "clublog_upload_enabled" in data:       CLUBLOG_UPLOAD_ENABLED     = bool(data["clublog_upload_enabled"])
        if data.get("clublog_upload_designator"):  CLUBLOG_UPLOAD_DESIGNATOR  = data["clublog_upload_designator"][:1].upper()
        if "telnet_enabled" in data:            TELNET_ENABLED            = bool(data["telnet_enabled"])
        if data.get("telnet_server"):           TELNET_SERVER             = data["telnet_server"]
        if data.get("telnet_port"):             TELNET_PORT               = int(data["telnet_port"])
        if data.get("callsign"):                MY_CALLSIGN               = data["callsign"]
        if "opname" in data:                    MY_NAME                   = data["opname"]
        if data.get("tci_host"):                TCI_HOST                  = data["tci_host"]
        if data.get("tci_port"):                TCI_PORT                  = int(data["tci_port"])
        if "tci_enabled"  in data:              TCI_ENABLED               = bool(data["tci_enabled"])
        if "itu_region"   in data:              ITU_REGION                = int(data["itu_region"])
        if "digital_udp_enabled" in data:       DIGITAL_UDP_ENABLED       = bool(data["digital_udp_enabled"])
        if data.get("digital_udp_port"):        DIGITAL_UDP_PORT          = int(data["digital_udp_port"])
        if "digital_tcp_enabled" in data:       DIGITAL_TCP_ENABLED       = bool(data["digital_tcp_enabled"])
        if data.get("digital_tcp_port"):        DIGITAL_TCP_PORT          = int(data["digital_tcp_port"])
        if "rotator_enabled" in data:           ROTATOR_ENABLED           = bool(data["rotator_enabled"])
        if data.get("rotator_host"):            ROTATOR_HOST              = data["rotator_host"]
        if data.get("rotator_port"):            ROTATOR_PORT              = int(data["rotator_port"])
        if data.get("rotator_protocol"):        ROTATOR_PROTOCOL          = data["rotator_protocol"]
        if "rotator_auto" in data:              ROTATOR_AUTO              = bool(data["rotator_auto"])
        if "rotator_debug" in data:             ROTATOR_DEBUG             = bool(data["rotator_debug"])
        if "backup_path" in data:               BACKUP_PATH               = data["backup_path"].strip()
        if "auto_backup_enabled"   in data:     AUTO_BACKUP_ENABLED       = bool(data["auto_backup_enabled"])
        if "auto_backup_interval"  in data:
            _v = str(data["auto_backup_interval"]).lower().strip()
            if _v in ("daily", "weekly", "on_exit"): AUTO_BACKUP_INTERVAL = _v
        if "auto_backup_retention" in data:
            try:
                _n = int(data["auto_backup_retention"])
                if _n >= 1: AUTO_BACKUP_RETENTION = _n
            except Exception: pass
        if "flrig_enabled" in data:             FLRIG_ENABLED             = bool(data["flrig_enabled"])
        if data.get("flrig_host"):              FLRIG_HOST                = data["flrig_host"].strip()
        if data.get("flrig_port"):              FLRIG_PORT                = int(data["flrig_port"])
        if "flrig_digital_mode" in data:        FLRIG_DIGITAL_MODE        = data["flrig_digital_mode"].strip()
        if "flrig_rtty_mode"    in data:        FLRIG_RTTY_MODE           = data["flrig_rtty_mode"].strip()
        if "hamlib_enabled" in data:            HAMLIB_ENABLED            = bool(data["hamlib_enabled"])
        if data.get("hamlib_host"):             HAMLIB_HOST               = data["hamlib_host"].strip()
        if data.get("hamlib_port"):             HAMLIB_PORT               = int(data["hamlib_port"])
        if "winkeyer_enabled" in data:          WINKEYER_ENABLED          = bool(data["winkeyer_enabled"])
        if data.get("winkeyer_port"):           WINKEYER_PORT             = data["winkeyer_port"].strip()
        if data.get("winkeyer_wpm"):            WINKEYER_WPM              = int(data["winkeyer_wpm"])
        if data.get("winkeyer_key_out"):        WINKEYER_KEY_OUT          = data["winkeyer_key_out"]
        if data.get("winkeyer_mode"):           WINKEYER_MODE             = data["winkeyer_mode"]
        if "winkeyer_ptt" in data:              WINKEYER_PTT              = bool(data["winkeyer_ptt"])
        if "winkeyer_ptt_lead" in data:         WINKEYER_PTT_LEAD         = int(data["winkeyer_ptt_lead"])
        if "winkeyer_ptt_tail" in data:         WINKEYER_PTT_TAIL         = int(data["winkeyer_ptt_tail"])
        if data.get("eqsl_user"):               EQSL_USER                 = data["eqsl_user"].strip()
        if data.get("eqsl_pass"):               EQSL_PASS                 = data["eqsl_pass"]
        if "eqsl_upload_enabled" in data:       EQSL_UPLOAD_ENABLED       = bool(data["eqsl_upload_enabled"])
        if data.get("pota_my_park"):            POTA_MY_PARK              = data["pota_my_park"].strip().upper()
        if "pota_user" in data:                 POTA_USER                 = data["pota_user"].strip()
        if "pota_pass" in data:                 POTA_PASS                 = data["pota_pass"].strip()
        # SAT settings
        if "sat_udp_enabled"    in data: SAT_UDP_ENABLED           = bool(data["sat_udp_enabled"])
        if data.get("sat_udp_port"):     SAT_UDP_PORT              = int(data["sat_udp_port"])
        if data.get("sat_adif_port"):    SAT_ADIF_PORT             = int(data["sat_adif_port"])
        if "sat_controller_ip"  in data: SAT_CONTROLLER_IP         = data["sat_controller_ip"].strip()
        # Lightning settings
        if "lightning_accepted"        in data: LIGHTNING_ACCEPTED        = bool(data["lightning_accepted"])
        if "lightning_enabled"         in data: LIGHTNING_ENABLED         = bool(data["lightning_enabled"]) and LIGHTNING_ACCEPTED
        if "lightning_range"           in data: LIGHTNING_RANGE           = int(data["lightning_range"])
        if "lightning_unit"            in data: LIGHTNING_UNIT            = data["lightning_unit"]
        if "lightning_blitzortung"     in data: LIGHTNING_BLITZORTUNG     = bool(data["lightning_blitzortung"])
        if "lightning_noaa"            in data: LIGHTNING_NOAA            = bool(data["lightning_noaa"])
        if "lightning_ambient"         in data: LIGHTNING_AMBIENT         = bool(data["lightning_ambient"])
        if data.get("lightning_ambient_api_key"): LIGHTNING_AMBIENT_API_KEY = data["lightning_ambient_api_key"].strip()
        if data.get("lightning_ambient_app_key"): LIGHTNING_AMBIENT_APP_KEY = data["lightning_ambient_app_key"].strip()
        # Wind alerts (v1.08.2-beta) — gated behind lightning acceptance
        if "wind_enabled"         in data: WIND_ENABLED        = bool(data["wind_enabled"]) and LIGHTNING_ACCEPTED
        if "wind_nws_alerts"      in data: WIND_NWS_ALERTS     = bool(data["wind_nws_alerts"])
        if "wind_nws_metar"       in data: WIND_NWS_METAR      = bool(data["wind_nws_metar"])
        if "wind_ambient"         in data: WIND_AMBIENT        = bool(data["wind_ambient"])
        if "wind_thresh_sust"     in data: WIND_THRESH_SUST    = int(data["wind_thresh_sust"])
        if "wind_thresh_gust"     in data: WIND_THRESH_GUST    = int(data["wind_thresh_gust"])
        if "wind_cooldown_min"    in data: WIND_COOLDOWN_MIN   = int(data["wind_cooldown_min"])
        if data.get("wind_metar_station"): WIND_METAR_STATION  = data["wind_metar_station"].strip().upper()
        print(f"Settings restored from {_APP_SETTINGS_FILE}")
    except FileNotFoundError:
        pass  # First run — no saved settings yet, defaults stand
    except Exception as _e:
        print(f"Settings load error: {_e}")
WEB_PORT = int(_srv_cfg.get("web_port", 5000))
WEB_HOST = _srv_cfg.get("web_host", "0.0.0.0")
# ──────────────────────────────────────────────────────────────────────────────

# ─── Configuration ────────────────────────────────────────────────────────────
QRZ_USER = ""                                 # QRZ.com username (your callsign)
QRZ_PASS = ""                                 # QRZ.com password
QRZ_LOGBOOK_KEY = ""                          # Logbook API key — qrz.com/hamlogbook (My Logbook → Settings → API Key)
QRZ_LOGBOOK_UPLOAD_ENABLED = False            # Auto-upload QSOs to QRZ Logbook (off by default — opt-in via Settings)
QRZ_LOOKUP_ENABLED = True

HAMQTH_USER = ""                              # HamQTH.com username (free callsign lookup)
HAMQTH_PASS = ""                              # HamQTH.com password

LOTW_TQSL_PATH         = ""     # Full path to tqsl.exe, or just "tqsl" if it is on the system PATH
LOTW_STATION_LOCATION  = ""     # TQSL Station Location name (as defined in TQSL) — passed as -l; falls back to -c callsign if blank
LOTW_UPLOAD_ENABLED    = False  # Auto-upload QSOs to LoTW via TQSL (off by default — user must configure)

CLUBLOG_EMAIL              = ""    # Club Log account email address
CLUBLOG_PASSWORD           = ""    # Club Log account password or App Password
CLUBLOG_CALLSIGN           = ""    # Callsign to upload under (defaults to MY_CALLSIGN if blank)
CLUBLOG_UPLOAD_ENABLED     = False # Auto-upload QSOs to Club Log (off by default)
CLUBLOG_UPLOAD_DESIGNATOR  = "B"   # Single letter added to uploaded records to prevent duplicate uploads
_clublog_blocked       = False  # Set True on 403 to stop further requests until credentials are corrected

# ─── eQSL Integration ─────────────────────────────────────────────────────────
EQSL_USER            = ""     # eQSL.cc username
EQSL_PASS            = ""     # eQSL.cc password
EQSL_UPLOAD_ENABLED  = False  # Auto-upload QSOs to eQSL.cc (off by default)

TELNET_ENABLED = False          # Set True to enable DX cluster spotting
TELNET_SERVER = "ve7cc.net"
TELNET_PORT = 23
MY_CALLSIGN = "YOURCALLSIGN"    # Your callsign
MY_NAME     = ""                # Operator name (shown in {MYNAME} CW token)

TCI_ENABLED = True          # Set False to disable TCI connection (e.g. when using flrig/HamLib only)
ITU_REGION  = 2             # ITU Region: 1=Europe/Africa/Russia, 2=Americas, 3=Asia-Pacific
TCI_HOST = "127.0.0.1"      # Thetis SDR host
TCI_PORT = 50001            # Thetis TCI WebSocket port (set in SDR: Setup → Network → TCI Server)
DATABASE = "hamlog.db"
VERSION  = "1.10"

# ─── Digital App Integration (WSJT-X / JTDX / MSHV / VarAC etc.) ─────────────
DIGITAL_UDP_ENABLED = False       # Listen for UDP QSOLogged packets (WSJT-X binary / ADIF text)
DIGITAL_UDP_PORT    = 2237        # Default WSJT-X port; VarAC can be pointed here too
DIGITAL_TCP_ENABLED = False       # Listen for TCP ADIF connections (VarAC / Logger32 / DXKeeper style)
DIGITAL_TCP_PORT    = 52001       # Standard TCP ADIF port used by N1MM/DXKeeper

# ─── Rotator Control ──────────────────────────────────────────────────────────
ROTATOR_ENABLED  = False
ROTATOR_HOST     = "127.0.0.1"
ROTATOR_PORT     = 12000          # PstRotator default
ROTATOR_PROTOCOL = "pstrotator"  # pstrotator | gs232 | easycomm | arco_tcp
ROTATOR_AUTO     = True           # Auto-rotate when clicking a DX spot or entering a callsign
ROTATOR_DEBUG    = False          # v1.08.1-beta — verbose ARCO/rotator debug log

# Live azimuth updated by the PstRotator background poller thread.
# None until the first successful status read.
_rot_live_az     = None
_rot_live_az_lock = threading.Lock()

# v1.08.1-beta — ARCO live elevation + fault flag (only set by ARCO poller).
_rot_live_el     = None
_rot_fault       = None       # None = unknown, "" = OK, str = fault description
_rot_state_lock  = threading.Lock()

# v1.08.1-beta — Rotator debug ring buffer (last 200 lines) + file logger.
import collections as _collections
_rot_debug_buf  = _collections.deque(maxlen=200)
_rot_debug_lock = threading.Lock()

def _rot_dbg(msg):
    """Append a timestamped line to the rotator debug buffer (and file when enabled).
    Safe to call from any thread; never raises."""
    try:
        line = time.strftime("%Y-%m-%d %H:%M:%S") + " " + str(msg)
    except Exception:
        line = str(msg)
    try:
        with _rot_debug_lock:
            _rot_debug_buf.append(line)
        if ROTATOR_DEBUG:
            try:
                appdata = os.environ.get("SDRLOGGERPLUS_DATA") or os.path.join(
                    os.environ.get("APPDATA", os.path.expanduser("~")), "SDRLoggerPlus")
                logdir = os.path.join(appdata, "logs")
                os.makedirs(logdir, exist_ok=True)
                with open(os.path.join(logdir, "rotator_debug.log"), "a", encoding="utf-8") as f:
                    f.write(line + "\n")
            except Exception:
                pass
    except Exception:
        pass

# ─── Backup ───────────────────────────────────────────────────────────────────
BACKUP_PATH = ""   # User-configured local folder for DB backups (empty = browser download)

# ─── Auto-Backup Scheduler (v1.09) ────────────────────────────────────────────
# Scheduled backup that bundles all three databases (General, POTA, SAT) as
# BOTH raw .db files AND ADIF exports, dumped into one timestamped subfolder
# per run. Retention is a rolling "keep last N" — the oldest folder is pruned
# only AFTER a new backup writes successfully, so a failed write (NAS offline,
# USB unplugged, etc.) never destroys the last known-good backup.
AUTO_BACKUP_ENABLED   = False
AUTO_BACKUP_INTERVAL  = "daily"   # one of: "daily", "weekly", "on_exit"
AUTO_BACKUP_RETENTION = 14        # keep last N timestamped folders
_auto_backup_lock     = threading.Lock()
_auto_backup_status   = {
    "last_run":   None,   # ISO-8601 UTC string
    "ok":         None,   # True / False / None (never run)
    "message":    "",     # success detail or error text
    "path":       "",     # path written on success
    "next_due":   None,   # ISO-8601 UTC string for next scheduled run
}
# Load persisted schedule state so Daily/Weekly intervals survive app restarts.
_load_auto_backup_state()

# ─── flrig (W1HKJ) XML-RPC Integration ───────────────────────────────────────
FLRIG_ENABLED      = False
FLRIG_HOST         = "127.0.0.1"
FLRIG_PORT         = 12345      # flrig default XML-RPC port
FLRIG_DIGITAL_MODE = ""         # Override digital passthrough mode name (e.g. "USB-D" for Icom, "DATA-U" for Kenwood/Yaesu)
FLRIG_RTTY_MODE    = ""         # Override RTTY mode name sent to flrig (blank = "RTTY"; set "USB-D" for AFSK/fldigi)
                                # Empty = auto-detect via rig.get_modes() at connect time
# ──────────────────────────────────────────────────────────────────────────────

# ─── HamLib (rigctld) Integration ─────────────────────────────────────────────
HAMLIB_ENABLED  = False
HAMLIB_HOST     = "127.0.0.1"
HAMLIB_PORT     = 4532          # rigctld default
# ──────────────────────────────────────────────────────────────────────────────

# ─── K1EL WinKeyer (WKmini / WK2 / WK3 / WKUSB) ─────────────────────────────
# Serial CW keyer using the K1EL WinKeyer protocol (1200 baud, 8N2).
# CW priority order: TCI → WinKeyer → HamLib
WINKEYER_ENABLED   = False
WINKEYER_PORT      = ""         # COM port (e.g. "COM2")
WINKEYER_WPM       = 26         # Default CW speed (5-99)
WINKEYER_KEY_OUT   = "port1"    # "port1", "port2", or "both"
WINKEYER_MODE      = "iambicb"  # "iambica", "iambicb", "ultimatic", "bug"
WINKEYER_PTT       = True       # Enable PTT output
WINKEYER_PTT_LEAD  = 0          # PTT lead-in delay in ms (0-250)
WINKEYER_PTT_TAIL  = 0          # PTT tail delay in ms (0-250)
_wk_serial         = None       # pyserial Serial object (set by background thread)
_wk_lock           = threading.Lock()   # guard serial writes
_wk_is_open        = False      # True when host-mode session is active
_wk_version        = 0          # firmware version returned by Admin:Open
# ──────────────────────────────────────────────────────────────────────────────


# ─── Lightning Detection ─────────────────────────────────────────────────────
LIGHTNING_ACCEPTED         = False  # User accepted the liability disclaimer
LIGHTNING_ENABLED          = False
LIGHTNING_RANGE            = 50     # Alert range
LIGHTNING_UNIT             = "mi"   # "mi" or "km"
LIGHTNING_BLITZORTUNG      = False  # Blitzortung.org real-time strikes
LIGHTNING_NOAA             = False  # NOAA/NWS severe thunderstorm warnings
LIGHTNING_AMBIENT          = False  # Ambient Weather personal station
LIGHTNING_AMBIENT_API_KEY  = ""
LIGHTNING_AMBIENT_APP_KEY  = ""
_lightning_status = {
    "active": False,
    "closest_mi": None,
    "closest_km": None,
    "direction": "",
    "strikes_1hr": 0,
    "sources": [],        # which sources are reporting
    "noaa_warning": "",   # active NOAA warning text
    "last_update": "",
}
_lightning_lock = threading.Lock()

# ─── High-Wind Alerts (v1.08.2-beta) ─────────────────────────────────────────
# Piggybacks on the lightning disclaimer — wind is gated behind the same
# "this is a convenience feature, not a safety system" acceptance.
WIND_ENABLED        = False
WIND_NWS_ALERTS     = False     # NWS active-alerts API: High Wind Warning/Advisory/Watch
WIND_NWS_METAR      = False     # NWS nearest-METAR observation (wind + gust)
WIND_AMBIENT        = False     # User's Ambient Weather PWS wind readings
WIND_THRESH_SUST    = 30        # Sustained wind MPH to trigger HIGH banner
WIND_THRESH_GUST    = 45        # Gust MPH to trigger HIGH banner
WIND_COOLDOWN_MIN   = 20        # Minutes before re-alerting after clearing
WIND_METAR_STATION  = ""        # 4-letter ICAO METAR station (e.g. KLUK, KCVG)

_wind_status = {
    "active": False,
    "severity": "",      # "" | "elevated" | "high" | "extreme"
    "sustained_mph": None,
    "gust_mph": None,
    "direction": "",     # compass e.g. "WSW"
    "sources": [],
    "nws_alert": "",     # event headline e.g. "High Wind Warning"
    "last_update": "",
}
_wind_lock = threading.Lock()
# ──────────────────────────────────────────────────────────────────────────────

# ─── POTA (Parks on the Air) ──────────────────────────────────────────────────
POTA_DATABASE   = "pota.db"     # Separate DB for POTA activations
ACTIVE_MODE     = "general"     # "general" | "pota" | "sat"
POTA_MY_PARK    = ""            # Sticky park reference (e.g. K-1234) for current activation
POTA_USER       = ""            # POTA.app username for self-spotting
POTA_PASS       = ""            # POTA.app password for self-spotting
# ──────────────────────────────────────────────────────────────────────────────

# ─── SAT (Satellite) Mode ────────────────────────────────────────────────────
import collections as _collections  # ensure available before first use below
SAT_UDP_ENABLED = False         # Enable SAT UDP listener
SAT_UDP_PORT    = 9932          # S.A.T. broadcast port
SAT_ADIF_PORT   = 1100          # S.A.T. QSO LOG TYPE ADIF-over-UDP port
SAT_CONTROLLER_IP = ""          # S.A.T. IP address (for display only)
_sat_state = {                  # Live state machine
    "status": "idle",           # idle | tracking | aos | los
    "satellite": "",            # satellite name
    "catno": "",                # catalog number
    "transponder": "",          # transponder name
    "uplink_freq": "",          # uplink frequency Hz
    "downlink_freq": "",        # downlink frequency Hz
    "uplink_mode": "",          # uplink mode
    "downlink_mode": "",        # downlink mode
    "aos_az": "",               # azimuth at AOS
    "los_az": "",               # azimuth at LOS
    "aos_time": None,           # datetime of AOS (for elapsed timer)
    "firmware": "",             # S.A.T. firmware version
    "serial": "",               # S.A.T. serial number
    "last_heard": None,         # datetime of last UDP packet
    "pass_qsos": [],            # QSOs logged during current pass
}
_sat_state_lock = threading.Lock()
_sat_events = _collections.deque(maxlen=50)  # rolling event log for UI
_sat_qso_counter = 0                         # increments on each auto-logged SAT QSO (ADIF or UDP)
# ──────────────────────────────────────────────────────────────────────────────

# Global storage for latest TCI data received
latest_tci     = {"callsign": "", "freq_mhz": "", "mode": ""}
_tci_cw_pitch  = None   # CW sidetone pitch (Hz) last reported by Thetis via TCI

# ─── Debug / error log ────────────────────────────────────────────────────────
import collections as _collections
_debug_log = _collections.deque(maxlen=200)   # rolling buffer of last 200 entries

def _log(msg):
    """Append a timestamped entry to the in-memory debug log and print to console."""
    from datetime import datetime
    entry = f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC] {msg}"
    _debug_log.append(entry)
    print(entry)

# Runtime settings (overridden via /api/settings)
runtime_settings = {}

# TCI send state — kept after connect so routes can push commands to Thetis
tci_active_sock = None
tci_send_lock   = threading.Lock()

# Spot registry: freq_hz → callsign — lets VFO changes identify clicked spots
# when Thetis doesn't fire spot_activated (most firmware versions don't)
tci_spot_registry      = {}   # {freq_hz: callsign} — lookup for VFO-click detection
tci_spot_registry_lock = threading.Lock()

# Digital app event queue — browser polls /api/digital_events to get auto-logged QSOs
import collections, re as _re
_digital_events = collections.deque(maxlen=50)   # {callsign, mode, freq_mhz, source, time}


# ─── Database Setup ────────────────────────────────────────────────────────────
def get_db(db_path=None):
    """Return a DB connection. Uses POTA_DATABASE when in pota mode, general DB for sat mode."""
    if db_path is None:
        db_path = POTA_DATABASE if ACTIVE_MODE == "pota" else DATABASE
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _init_one_db(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS qso_log (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            callsign       TEXT NOT NULL,
            name           TEXT,
            qth            TEXT,
            date_worked    TEXT,
            time_worked    TEXT,
            band           TEXT,
            mode           TEXT,
            freq_mhz       REAL,
            my_rst_sent    TEXT,
            their_rst_rcvd TEXT,
            remarks        TEXT,
            contest_name   TEXT,
            pota_ref       TEXT,
            pota_p2p       TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_callsign ON qso_log(callsign)")
    for col in ["name", "qth", "pota_ref", "pota_p2p", "state", "country", "gridsquare",
                "prop_mode", "sat_name", "sat_catno", "transponder_name",
                "uplink_freq", "downlink_freq", "uplink_mode", "downlink_mode",
                "aos_az", "los_az", "my_grid", "my_lat", "my_lon"]:
        try:
            conn.execute(f"ALTER TABLE qso_log ADD COLUMN {col} TEXT")
        except Exception:
            pass
    conn.commit()
    conn.close()


def init_db():
    _init_one_db(DATABASE)
    _init_one_db(POTA_DATABASE)
    print("Databases ready:", DATABASE, "&", POTA_DATABASE)


# ─── TCI WebSocket Client ──────────────────────────────────────────────────────
# Thetis/Theis SDR uses the TCI protocol over WebSocket (ws://host:port)
# Text frames contain commands like:  vfo:0,0,14074000;  modulation:0,FT8;
# We connect as a client and listen for vfo + modulation updates.

tci_ws_connected = False

# ─── flrig XML-RPC state ──────────────────────────────────────────────────────
latest_flrig    = {"freq_mhz": "", "mode": "", "rig": "",
                   "digital_usb": "DATA-U",   # rig's USB digital mode name (auto-detected)
                   "digital_lsb": "DATA-L"}    # rig's LSB digital mode name (auto-detected)
flrig_connected = False

# Digital mode names flrig may use — checked in priority order at connect time
_FLRIG_DIGITAL_USB_NAMES = ["USB-D", "DATA-U", "PKT-U", "DIGU"]
_FLRIG_DIGITAL_LSB_NAMES = ["LSB-D", "DATA-L", "PKT-L", "DIGL"]

def _flrig_server():
    """Return a connected xmlrpc.client.ServerProxy for flrig."""
    import xmlrpc.client
    return xmlrpc.client.ServerProxy(
        f"http://{FLRIG_HOST}:{FLRIG_PORT}", allow_none=True)

# ── flrig mode name normalisation ──────────────────────────────────────────────
# flrig → SDRLogger+ (for display / QSO form)
_FLRIG_MODE_IN = {
    "USB":"USB",   "LSB":"LSB",
    "CW":"CWU",    "CW-R":"CWL",   "CWR":"CWL",
    "FM":"FM",     "NFM":"NFM",    "AM":"AM",    "SAM":"SAM",
    "RTTY":"RTTY", "RTTYR":"RTTY", "RTTY-R":"RTTY",
    # Generic DATA modes (Kenwood, Yaesu etc.)
    "DATA-U":"DIGU", "DATA-L":"DIGL", "PKT-U":"DIGU", "PKT-L":"DIGL",
    "DIGU":"DIGU",   "DIGL":"DIGL",
    # IC-9100 / Icom USB-D / LSB-D digital passthrough modes
    "USB-D":"DIGU",  "LSB-D":"DIGL",
    "USBD":"DIGU",   "LSBD":"DIGL",
    "FT8":"FT8", "FT4":"FT4", "JS8":"JS8", "WSPR":"WSPR",
    "JT65":"JT65", "JT9":"JT9",
}
# SDRLogger+ → flrig (for tune commands from DX spot / form)
_FLRIG_MODE_OUT = {
    "USB":"USB",   "LSB":"LSB",
    "CWU":"CW",    "CWL":"CW-R",   "CW":"CW",
    "FM":"FM",     "NFM":"FM",     "AM":"AM",    "SAM":"AM",
    # RTTY is a native radio mode (not audio passthrough) — send directly
    "RTTY":"RTTY", "RTTY-R":"RTTY-R", "RTTYR":"RTTY-R",
    # Digital passthrough — these are overridden by FLRIG_DIGITAL_MODE when set
    "DIGU":"DATA-U", "DIGL":"DATA-L",
    "FT8":"DATA-U",  "FT4":"DATA-U",  "JS8":"DATA-U",
    "WSPR":"DATA-U", "JT65":"DATA-U", "JT9":"DATA-U",
    "DIGI":"DATA-U", "PSK31":"DATA-U",
}

def flrig_poller():
    """
    Background thread: polls flrig XML-RPC every 1.5 s for frequency and mode.
    flrig exposes rig.get_vfoA() (Hz string) and rig.get_mode() (mode string).
    Only runs when FLRIG_ENABLED is True.
    """
    global latest_flrig, flrig_connected
    while True:
        if not FLRIG_ENABLED:
            flrig_connected = False
            threading.Event().wait(3)
            continue
        try:
            srv  = _flrig_server()
            freq = srv.rig.get_vfoA()
            mode = srv.rig.get_mode()
            if freq:
                latest_flrig["freq_mhz"] = round(int(freq) / 1_000_000, 6)
            if mode:
                # Normalise flrig mode name → SDRLogger+ dropdown value
                raw = mode.strip().upper()
                latest_flrig["mode"] = _FLRIG_MODE_IN.get(raw, raw)
            # Auto-detect this rig's digital mode names once per connection cycle
            if not flrig_connected:
                try:
                    modes_raw = srv.rig.get_modes() or ""
                    avail = {m.strip().upper() for m in modes_raw.split(',')}
                    for dm in _FLRIG_DIGITAL_USB_NAMES:
                        if dm.upper() in avail:
                            latest_flrig["digital_usb"] = dm
                            break
                    for dm in _FLRIG_DIGITAL_LSB_NAMES:
                        if dm.upper() in avail:
                            latest_flrig["digital_lsb"] = dm
                            break
                    _log(f"flrig digital modes detected — USB: {latest_flrig['digital_usb']}  LSB: {latest_flrig['digital_lsb']}")
                except Exception:
                    pass  # get_modes() not supported on this flrig build — keep defaults
            try:
                latest_flrig["rig"] = srv.rig.get_xcvr() or ""
            except Exception:
                pass
            flrig_connected = True
        except Exception:
            flrig_connected = False
        threading.Event().wait(1.5)

def flrig_set_freq_mode(freq_mhz, mode=""):
    """
    Set frequency and optionally mode via flrig XML-RPC.
    Frequency and mode are set independently — a mode failure will not
    prevent the frequency from being tuned.
    Tries integer first (most flrig versions), falls back to string if needed.
    """
    if not FLRIG_ENABLED:
        return False
    freq_ok = False
    hz = int(float(freq_mhz) * 1_000_000)
    try:
        srv = _flrig_server()
        # flrig XML-RPC set_vfoA requires a double (<double> XML-RPC type), not int or string
        srv.rig.set_vfoA(float(hz))
        freq_ok = True
    except Exception as e:
        _log(f"flrig set_vfoA error ({freq_mhz} MHz → {hz} Hz): {e}")
        return False
    if mode:
        flrig_mode = mode   # fallback label for error logging
        try:
            mode_up = mode.strip().upper()
            # Audio-passthrough digital modes — apply FLRIG_DIGITAL_MODE override if set
            # RTTY/RTTY-R are native radio modes and bypass this; they go to _FLRIG_MODE_OUT directly
            _DIGITAL_PASSTHROUGH_USB = {"FT8","FT4","JS8","WSPR","JT65","JT9","DIGI","PSK31","DIGU","DATA-U","PKT-U","MSK144","Q65","FST4","FST4W","VARAC","OLIVIA","HELL","PACKET","DATA"}
            _DIGITAL_PASSTHROUGH_LSB = {"DIGL","DATA-L","PKT-L"}
            if mode_up in _DIGITAL_PASSTHROUGH_USB:
                # Use manual override first (e.g. "USB-D" for Icom), then auto-detected, then default
                flrig_mode = FLRIG_DIGITAL_MODE if FLRIG_DIGITAL_MODE else latest_flrig.get("digital_usb", "DATA-U")
            elif mode_up in _DIGITAL_PASSTHROUGH_LSB:
                # LSB passthrough (DIGL) — use auto-detected LSB digital name
                flrig_mode = latest_flrig.get("digital_lsb", "DATA-L")
            elif mode_up in {"RTTY", "RTTY-R", "RTTYR"}:
                # RTTY — use override if set (e.g. "USB-D" for AFSK/fldigi), else native RTTY mode
                flrig_mode = FLRIG_RTTY_MODE if FLRIG_RTTY_MODE else _FLRIG_MODE_OUT.get(mode_up, mode_up)
            else:
                # All other native modes (USB, LSB, CW, AM, FM, etc.)
                flrig_mode = _FLRIG_MODE_OUT.get(mode_up, mode_up)
            _log(f"flrig set_mode: SDRLogger+ mode={mode} → flrig mode={flrig_mode}")
            srv.rig.set_mode(flrig_mode)
        except Exception as e:
            _log(f"flrig set_mode error ({mode} → {flrig_mode}): {e}")
            pass   # Mode set failed — frequency still tuned, not a fatal error
    return freq_ok

# ─── HamLib rigctld state ─────────────────────────────────────────────────────
latest_hamlib    = {"freq_mhz": "", "mode": ""}
hamlib_connected = False

def _hamlib_cmd(cmd):
    """Send one command to rigctld, return the response line(s). Returns None on error."""
    global hamlib_connected
    try:
        s = socket.create_connection((HAMLIB_HOST, HAMLIB_PORT), timeout=3)
        s.sendall((cmd + "\n").encode())
        resp = b""
        s.settimeout(2.0)
        while True:
            chunk = s.recv(256)
            if not chunk:
                break
            resp += chunk
            if b"\n" in resp:
                break
        s.close()
        hamlib_connected = True
        return resp.decode("utf-8", errors="ignore").strip()
    except Exception:
        hamlib_connected = False
        return None

def hamlib_poller():
    """
    Background thread: polls rigctld every 1.5 s for frequency and mode.
    Updates latest_hamlib dict which /api/hamlib_data serves to the browser.
    Only runs when HAMLIB_ENABLED is True.
    """
    global latest_hamlib, hamlib_connected
    while True:
        if not HAMLIB_ENABLED:
            threading.Event().wait(3)
            continue
        freq_resp = _hamlib_cmd("\\get_freq")
        mode_resp = _hamlib_cmd("\\get_mode")
        if freq_resp and freq_resp.isdigit() or (freq_resp and freq_resp.replace('.','',1).isdigit()):
            try:
                freq_mhz = round(int(freq_resp) / 1_000_000, 6)
                latest_hamlib["freq_mhz"] = freq_mhz
            except Exception:
                pass
        if mode_resp:
            # rigctld returns "USB 2800\nRPRT 0" — take first word
            mode_line = mode_resp.split("\n")[0].strip()
            if mode_line and not mode_line.startswith("RPRT"):
                mode = mode_line.split()[0].upper()
                # Normalise rigctld mode names → SDRLogger+ dropdown values
                _mode_map = {"USB":"USB","LSB":"LSB","AM":"AM","FM":"FM","NFM":"FM",
                             "CW":"CWU","CWR":"CWL","RTTY":"RTTY","RTTYR":"RTTY",
                             "PKTUSB":"DIGU","PKTLSB":"DIGL","PKTFM":"DIGU",
                             "DIGI":"DIGU","DIGU":"DIGU","DIGL":"DIGL"}
                latest_hamlib["mode"] = _mode_map.get(mode, mode)
        threading.Event().wait(1.5)


def hamlib_set_freq_mode(freq_mhz, mode=""):
    """Send set_freq (and optionally set_mode) commands to rigctld."""
    if not HAMLIB_ENABLED:
        return False
    # SDRLogger+ mode name → rigctld mode name
    _rig_map = {
        "USB":"USB",   "LSB":"LSB",
        "CWU":"CW",    "CWL":"CWR",   "CW":"CW",
        "AM":"AM",     "FM":"FM",     "NFM":"FM",   "SAM":"AM",
        "RTTY":"RTTY", "RTTY-R":"RTTYR","RTTYR":"RTTYR",
        "DIGU":"PKTUSB","DIGL":"PKTLSB",
        "FT8":"PKTUSB","FT4":"PKTUSB","JS8":"PKTUSB","WSPR":"PKTUSB",
        "JT65":"PKTUSB","JT9":"PKTUSB","PSK31":"PKTUSB",
        "MSK144":"PKTUSB","Q65":"PKTUSB","FST4":"PKTUSB","FST4W":"PKTUSB",
        "DIGI":"PKTUSB","DATA":"PKTUSB","VARAC":"PKTUSB",
        "OLIVIA":"PKTUSB","HELL":"PKTUSB","PACKET":"PKTUSB",
    }
    try:
        if freq_mhz is not None:
            freq_hz = int(float(freq_mhz) * 1_000_000)
            _hamlib_cmd(f"\\set_freq {freq_hz}")
        if mode:
            rig_mode = _rig_map.get(mode.upper(), mode.upper())
            _hamlib_cmd(f"\\set_mode {rig_mode} 0")
        return True
    except Exception:
        return False

# ─── TCI WebSocket client ─────────────────────────────────────────────────────

def ws_handshake(sock, host, port):
    """Perform HTTP → WebSocket upgrade handshake."""
    import base64, hashlib
    key = base64.b64encode(os.urandom(16)).decode()
    handshake = (
        f"GET / HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        f"Sec-WebSocket-Version: 13\r\n\r\n"
    )
    sock.sendall(handshake.encode())
    resp = b""
    while b"\r\n\r\n" not in resp:
        resp += sock.recv(1024)
    return b"101" in resp


def ws_send_frame(sock, text):
    """Send a masked WebSocket text frame (client→server direction)."""
    payload = text.encode("utf-8")
    length  = len(payload)
    mask    = os.urandom(4)
    masked  = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    hdr = bytearray([0x81])   # FIN=1, opcode=0x1 (text)
    if length <= 125:
        hdr.append(0x80 | length)
    elif length <= 65535:
        hdr.append(0x80 | 126)
        hdr.extend(struct.pack(">H", length))
    else:
        hdr.append(0x80 | 127)
        hdr.extend(struct.pack(">Q", length))
    hdr.extend(mask)
    sock.sendall(bytes(hdr) + masked)


def send_tci_command(cmd):
    """Send a TCI command string to the connected Thetis instance. Returns True on success."""
    global tci_active_sock
    with tci_send_lock:
        s = tci_active_sock
        if s:
            try:
                ws_send_frame(s, cmd)
                return True
            except Exception as e:
                print(f"TCI send error: {e}")
                tci_active_sock = None
    return False


def ws_recv_frame(sock):
    """Read one WebSocket frame, return (opcode, payload_bytes) or None on error."""
    try:
        hdr = b""
        while len(hdr) < 2:
            chunk = sock.recv(2 - len(hdr))
            if not chunk:
                return None
            hdr += chunk
        b0, b1 = hdr
        opcode = b0 & 0x0F
        masked  = (b1 & 0x80) != 0
        length  = b1 & 0x7F
        if length == 126:
            raw = sock.recv(2)
            length = struct.unpack(">H", raw)[0]
        elif length == 127:
            raw = sock.recv(8)
            length = struct.unpack(">Q", raw)[0]
        if masked:
            mask = sock.recv(4)
        # Read payload in chunks
        payload = b""
        while len(payload) < length:
            chunk = sock.recv(min(4096, length - len(payload)))
            if not chunk:
                return None
            payload += chunk
        if masked:
            payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        return opcode, payload
    except Exception:
        return None


def parse_tci_message(text):
    """
    Parse a TCI text message and update latest_tci.
    TCI commands look like:  vfo:0,0,14074000;  modulation:0,FT8;
    Multiple commands may be separated by semicolons in one frame.
    """
    for part in text.strip().split(";"):
        part = part.strip()
        if not part:
            continue
        ci = part.find(":")
        if ci < 0:
            continue
        cmd  = part[:ci].lower()
        args = part[ci+1:].split(",")

        if cmd == "vfo" and len(args) >= 3:
            # vfo:trx,channel,freq_hz  — channel 0 = VFO A
            try:
                trx, ch, hz = args[0], args[1], int(args[2])
                if trx == "0" and ch == "0":
                    latest_tci["freq_mhz"] = round(hz / 1_000_000, 6)
                    print(f"TCI VFO A → {latest_tci['freq_mhz']} MHz")
                    # VFO-based spot lookup: if the new freq matches a known spot
                    # (within ±1 kHz), treat it as a panadapter click.
                    # This works even when Thetis doesn't send spot_activated.
                    with tci_spot_registry_lock:
                        best_call = None
                        best_delta = 1001   # Hz, 1 kHz tolerance
                        for reg_hz, reg_call in tci_spot_registry.items():
                            delta = abs(hz - reg_hz)
                            if delta < best_delta:
                                best_delta = delta
                                best_call  = reg_call
                    if best_call and not latest_tci.get("callsign"):
                        latest_tci["callsign"] = best_call
                        print(f"TCI VFO match → {best_call} (Δ{best_delta}Hz)")
            except (ValueError, IndexError):
                pass

        elif cmd == "modulation" and len(args) >= 2:
            # modulation:trx,MODE
            if args[0] == "0":
                latest_tci["mode"] = args[1].upper()
                print(f"TCI mode → {latest_tci['mode']}")

        elif cmd == "cw_pitch" and len(args) >= 2:
            # cw_pitch:trx,hz — CW sidetone pitch from Thetis
            try:
                global _tci_cw_pitch
                _tci_cw_pitch = int(float(args[1]))
            except (ValueError, IndexError):
                pass

        elif cmd == "spot_activated" and len(args) >= 3:
            # spot_activated:callsign,mode,freq_hz[,argb]
            # Fired when user clicks a spot label on the Thetis panadapter
            callsign_clicked = args[0].strip().upper()
            if callsign_clicked:
                latest_tci["callsign"] = callsign_clicked
                print(f"TCI spot activated → {callsign_clicked}")


def tci_ws_client():
    """
    Background thread: connects to Thetis TCI WebSocket server and
    listens for VFO / modulation updates.  Auto-reconnects on disconnect.
    """
    global tci_ws_connected, tci_active_sock
    while True:
        if not TCI_ENABLED:
            tci_ws_connected = False
            with tci_send_lock:
                tci_active_sock = None
            threading.Event().wait(3)
            continue
        try:
            _log(f"TCI: connecting to ws://{TCI_HOST}:{TCI_PORT} …")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((TCI_HOST, TCI_PORT))
            sock.settimeout(None)

            if not ws_handshake(sock, TCI_HOST, TCI_PORT):
                _log("TCI: WebSocket handshake failed — retrying in 10s")
                sock.close()
                threading.Event().wait(10)
                continue

            tci_ws_connected = True
            with tci_send_lock:
                tci_active_sock = sock
            _log(f"TCI: connected to Thetis on {TCI_HOST}:{TCI_PORT}")

            while True:
                frame = ws_recv_frame(sock)
                if frame is None:
                    break
                opcode, payload = frame
                if opcode == 0x8:   # close frame
                    break
                if opcode in (0x1, 0x0):  # text or continuation
                    try:
                        parse_tci_message(payload.decode("utf-8", errors="ignore"))
                    except Exception:
                        pass
                # opcode 0x2 = binary (audio/IQ) — ignore

            sock.close()
        except Exception as e:
            _log(f"TCI: connection failed ({e})")
        finally:
            tci_ws_connected = False
            with tci_send_lock:
                tci_active_sock = None

        _log("TCI: disconnected — reconnecting in 10s …")
        threading.Event().wait(10)


def qrz_logbook_upload(qso):
    """
    Upload a single QSO to QRZ Logbook API.
    https://www.qrz.com/docs/logbook/QRZLogbookAPI.html
    Requires a QRZ Logbook API Access Key (different from XML lookup key).
    """
    if not QRZ_LOGBOOK_KEY:
        return None, "No QRZ Logbook API key configured"
    try:
        # Build ADIF string for this QSO
        def adif_field(name, val):
            v = str(val) if val else ""
            return f"<{name}:{len(v)}>{v}" if v else ""

        date_str = (qso.get("date_worked") or "").replace("-", "")
        time_str = (qso.get("time_worked") or "").replace(":", "")[:6]
        adif = (
            adif_field("CALL",            qso.get("callsign", "")) +
            adif_field("STATION_CALLSIGN", MY_CALLSIGN) +
            adif_field("QSO_DATE",        date_str) +
            adif_field("TIME_ON",         time_str) +
            adif_field("BAND",            qso.get("band", "")) +
            adif_field("MODE",            qso.get("mode", "")) +
            adif_field("FREQ",            str(qso.get("freq_mhz", ""))) +
            adif_field("RST_SENT",        qso.get("my_rst_sent", "")) +
            adif_field("RST_RCVD",        qso.get("their_rst_rcvd", "")) +
            adif_field("NAME",            qso.get("name", "")) +
            adif_field("QTH",             qso.get("qth", "")) +
            adif_field("COMMENT",         qso.get("remarks", "")) +
            "<EOR>"
        )

        resp = requests.post(
            "https://logbook.qrz.com/api",
            data=f"KEY={QRZ_LOGBOOK_KEY}&ACTION=INSERT&ADIF={adif}",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": f"HamLog/1.0 ({MY_CALLSIGN})"
            },
            timeout=10
        )
        # Parse response: RESULT=OK&LOGID=xxx&COUNT=1
        result = {}
        for part in resp.text.strip().split("&"):
            if "=" in part:
                k, v = part.split("=", 1)
                result[k] = v

        if result.get("RESULT") == "OK":
            return result.get("LOGID"), None
        else:
            reason = result.get("REASON", resp.text[:200])
            return None, f"QRZ rejected: {reason}"
    except Exception as e:
        return None, str(e)


def lotw_upload(qso):
    """
    Upload a single QSO to ARRL LoTW via the TQSL command-line tool.
    TQSL signs the ADIF with the user's LoTW certificate and POSTs it to lotw.arrl.org.

    TQSL flags used:
      -d  Suppress date-range dialog
      -u  Upload signed file to LoTW after signing
      -x  Exit TQSL after processing (prevents the process hanging)
      -l  Station Location name as defined in TQSL (preferred over -c alone)
      -c  Callsign fallback when no Station Location is configured

    Requires TQSL installed with a valid callsign certificate loaded.
    LOTW_TQSL_PATH can be a full path to tqsl.exe or just "tqsl" if TQSL is on the system PATH.
    https://lotw.arrl.org/lotw-help/developer-submit-qsos/
    """
    if not LOTW_TQSL_PATH:
        return False, "TQSL path not configured — set it in Settings → LoTW"
    # If it looks like a full path (contains a slash or backslash) verify the file exists
    if os.sep in LOTW_TQSL_PATH or "/" in LOTW_TQSL_PATH:
        if not os.path.isfile(LOTW_TQSL_PATH):
            return False, f"TQSL not found at: {LOTW_TQSL_PATH}"

    try:
        def adif_field(name, val):
            v = str(val) if val else ""
            return f"<{name}:{len(v)}>{v}" if v else ""

        date_str = (qso.get("date_worked") or "").replace("-", "")
        time_str = (qso.get("time_worked") or "").replace(":", "")[:6]

        record = (
            adif_field("CALL",             qso.get("callsign", "")) +
            adif_field("STATION_CALLSIGN", MY_CALLSIGN) +
            adif_field("QSO_DATE",         date_str) +
            adif_field("TIME_ON",          time_str) +
            adif_field("BAND",             qso.get("band", "")) +
            adif_field("MODE",             qso.get("mode", "")) +
            adif_field("FREQ",             str(qso.get("freq_mhz", ""))) +
            adif_field("RST_SENT",         qso.get("my_rst_sent", "")) +
            adif_field("RST_RCVD",         qso.get("their_rst_rcvd", "")) +
            "<EOR>\n"
        )
        adif_content = "<ADIF_VER:5>3.1.0\n<EOH>\n" + record

        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".adi", prefix="sdrlogger_lotw_")
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                f.write(adif_content)

            # -d suppress date dialog  -u upload  -x exit after processing
            cmd = [LOTW_TQSL_PATH, "-d", "-u", "-x"]
            if LOTW_STATION_LOCATION:
                cmd += ["-l", LOTW_STATION_LOCATION]   # named Station Location (most precise)
            elif MY_CALLSIGN:
                cmd += ["-c", MY_CALLSIGN]              # callsign cert fallback
            cmd.append(tmp_path)

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                return True, None
            else:
                err = (result.stderr or result.stdout or "Unknown TQSL error").strip()[:200]
                return False, f"TQSL error: {err}"
        finally:
            try: os.unlink(tmp_path)
            except Exception: pass

    except FileNotFoundError:
        return False, "TQSL not found — check the path in Settings or add TQSL to your system PATH"
    except subprocess.TimeoutExpired:
        return False, "TQSL timed out — ensure TQSL exits cleanly and no dialogs are open"
    except Exception as e:
        return False, str(e)


def clublog_upload(qso):
    """
    Upload a single QSO to Club Log via the real-time API (realtime.php).

    IMPORTANT — use realtime.php for per-QSO uploads ONLY.
    putlogs.php is for bulk ADIF imports; calling it repeatedly for single QSOs
    will trigger automatic IP-address blocking by Club Log.

    On HTTP 403 the global _clublog_blocked flag is set to prevent any further
    requests until the user corrects their credentials and re-saves Settings.
    Per Club Log policy, repeated 403s from one IP result in firewall blocking.

    API details: https://clublog.freshdesk.com/support/solutions/articles/54906
    """
    global _clublog_blocked

    if _clublog_blocked:
        return False, "Club Log uploads disabled — authentication failed previously; re-check credentials in Settings"
    if not CLUBLOG_APP_KEY:
        return False, "Club Log application key not available in this build"
    if not CLUBLOG_EMAIL:
        return False, "Club Log email not configured"
    if not CLUBLOG_PASSWORD:
        return False, "Club Log password not configured"

    callsign = CLUBLOG_CALLSIGN or MY_CALLSIGN
    if not callsign:
        return False, "No callsign configured"

    try:
        def adif_field(name, val):
            v = str(val) if val else ""
            return f"<{name}:{len(v)}>{v}" if v else ""

        date_str = (qso.get("date_worked") or "").replace("-", "")
        time_str = (qso.get("time_worked") or "").replace(":", "")[:6]

        adif = (
            adif_field("CALL",             qso.get("callsign", "")) +
            adif_field("STATION_CALLSIGN", callsign) +
            adif_field("QSO_DATE",         date_str) +
            adif_field("TIME_ON",          time_str) +
            adif_field("BAND",             qso.get("band", "")) +
            adif_field("MODE",             qso.get("mode", "")) +
            adif_field("FREQ",             str(qso.get("freq_mhz", ""))) +
            adif_field("RST_SENT",         qso.get("my_rst_sent", "")) +
            adif_field("RST_RCVD",         qso.get("their_rst_rcvd", "")) +
            "<EOR>"
        )

        # Club Log realtime.php uses application/x-www-form-urlencoded.
        # multipart is only for putlogs.php (batch file upload).
        _log(f"[clublog] uploading QSO: {qso.get('callsign','')} {qso.get('band','')} {qso.get('mode','')}")
        resp = requests.post(
            "https://clublog.org/realtime.php",
            data={
                "api":      CLUBLOG_APP_KEY.strip(),
                "email":    CLUBLOG_EMAIL,
                "password": CLUBLOG_PASSWORD,
                "callsign": callsign,
                "adif":     adif,
            },
            headers={"User-Agent": f"SDRLoggerPlus/{VERSION}"},
            timeout=10
        )

        body = resp.text.strip()
        _log(f"[clublog] HTTP {resp.status_code} — {body[:200]}")
        if resp.status_code == 403 or body.startswith("Login rejected"):
            _clublog_blocked = True   # ONE-STRIKE RULE: stop ALL further uploads immediately
            _log("[clublog] 403 received — uploads BLOCKED to prevent IP firewall ban")
            return False, "Club Log: authentication failed (403) — uploads disabled to prevent IP ban. Re-check credentials in Settings."
        elif resp.status_code == 400:
            return False, f"Club Log: QSO rejected — {body[:200]}"
        elif resp.status_code == 500:
            return False, "Club Log: server error (500) — try again later"
        elif resp.status_code == 200:
            # Club Log actual response strings per their API: "OK", "Dupe", "Updated QSO"
            if re.search(r'\bOK\b', body) or re.search(r'\bDupe\b', body) or "Updated QSO" in body:
                return True, None
            else:
                return False, f"Club Log: unexpected response — {body[:200]}"
        else:
            return False, f"Club Log: HTTP {resp.status_code}"

    except Exception as e:
        return False, str(e)


def eqsl_upload(qso):
    """
    Upload a single QSO to eQSL.cc via the ADIF import API.

    POST to https://www.eQSL.cc/qslcard/ImportADIF.cfm with fields:
      EQSL_USER, EQSL_PSWD, ADIFData
    PROGRAMID:10>SDRLogger+ is included in the ADIF header to suppress
    the "Logger not found" warning.  eQSL does not block IPs on errors.
    Returns (True, None) on success or (False, error_string) on failure.
    """
    if not EQSL_USER:
        return False, "eQSL username not configured"
    if not EQSL_PASS:
        return False, "eQSL password not configured"

    try:
        def adif_field(name, val):
            v = str(val) if val else ""
            return f"<{name}:{len(v)}>{v}" if v else ""

        date_str = (qso.get("date_worked") or "").replace("-", "")
        time_str = (qso.get("time_worked") or "").replace(":", "")[:6]

        adif = (
            "<ADIF_VER:5>3.1.0"
            "<PROGRAMID:10>SDRLogger+"
            "<EOH>"
            + adif_field("CALL",       qso.get("callsign", ""))
            + adif_field("QSO_DATE",   date_str)
            + adif_field("TIME_ON",    time_str)
            + adif_field("BAND",       qso.get("band", ""))
            + adif_field("MODE",       qso.get("mode", ""))
            + adif_field("FREQ",       str(qso.get("freq_mhz", "")))
            + adif_field("RST_SENT",   qso.get("my_rst_sent", ""))
            + adif_field("RST_RCVD",   qso.get("their_rst_rcvd", ""))
            + "<EOR>"
        )

        resp = requests.post(
            "https://www.eQSL.cc/qslcard/ImportADIF.cfm",
            data={"EQSL_USER": EQSL_USER, "EQSL_PSWD": EQSL_PASS, "ADIFData": adif},
            timeout=15,
        )

        body = resp.text or ""
        if resp.status_code != 200:
            return False, f"eQSL: HTTP {resp.status_code}"
        if "Record Inserted" in body or "Duplicate" in body:
            return True, None
        if "incorrectly formatted" in body.lower() or "error" in body.lower():
            # Extract short message from HTML
            import re as _re2
            m = _re2.search(r'<font[^>]*>([^<]{5,120})</font>', body, _re2.IGNORECASE)
            snippet = m.group(1).strip() if m else body[:120].strip()
            return False, f"eQSL: {snippet}"
        return True, None

    except Exception as e:
        return False, str(e)


# ─── ADIF string parser ───────────────────────────────────────────────────────
def parse_adif_string(adif):
    """Parse a flat ADIF record string into a dict of lowercase field names → values."""
    fields = {}
    for m in _re.finditer(r'<([^:>]+)(?::(\d+)(?::[^>]*)?)?>([^<]*)', adif, _re.IGNORECASE):
        name   = m.group(1).lower()
        length = int(m.group(2)) if m.group(2) else len(m.group(3))
        value  = m.group(3)[:length].strip()
        if name not in ('eor', 'eoh') and value:
            fields[name] = value
    return fields


# ─── WSJT-X / JTDX / MSHV binary UDP packet parser ──────────────────────────
_WSJTX_MAGIC = 0xADBCCBDA

def _read_qt_string(data, offset):
    """Read a Qt QDataStream UTF-8 string. Returns (string_or_None, new_offset)."""
    if offset + 4 > len(data):
        return None, offset
    length = struct.unpack_from('>I', data, offset)[0]
    offset += 4
    if length == 0xFFFFFFFF:   # Qt null string
        return None, offset
    end = offset + length
    return data[offset:end].decode('utf-8', errors='replace'), end

def _read_qt_datetime(data, offset):
    """Read a Qt QDateTime (8-byte julian + 4-byte ms + 1-byte spec). Returns (datetime_or_None, new_offset)."""
    if offset + 13 > len(data):
        return None, offset + 13
    julian_day = struct.unpack_from('>q', data, offset)[0]
    ms_in_day  = struct.unpack_from('>I', data, offset + 8)[0]
    offset += 13
    if julian_day == 0:
        return None, offset
    try:
        # Julian Day to datetime: JD 2440588 = 1970-01-01
        days_since_epoch = julian_day - 2440588
        from datetime import timedelta, datetime as dt
        d = dt(1970, 1, 1) + timedelta(days=days_since_epoch, milliseconds=ms_in_day)
        return d, offset
    except Exception:
        return None, offset

def parse_wsjtx_binary(data):
    """
    Parse a WSJT-X / JTDX / MSHV UDP binary packet.
    Returns a QSO dict if it is a QSOLogged (type 5) packet, else None.
    """
    if len(data) < 12:
        return None
    magic, schema, msg_type = struct.unpack_from('>III', data, 0)
    if magic != _WSJTX_MAGIC:
        return None
    if msg_type != 5:           # 5 = QSOLogged
        return None
    offset = 12
    client_id, offset = _read_qt_string(data, offset)
    dt_off,    offset = _read_qt_datetime(data, offset)
    dx_call,   offset = _read_qt_string(data, offset)    # DXCall (before frequency)
    dx_grid,   offset = _read_qt_string(data, offset)    # DXGrid (before frequency)
    if offset + 8 > len(data):
        return None
    freq_hz = struct.unpack_from('>Q', data, offset)[0]  # quint64 (Hz, unsigned 64-bit)
    offset += 8
    mode,         offset = _read_qt_string(data, offset)
    rst_sent,     offset = _read_qt_string(data, offset)
    rst_rcvd,     offset = _read_qt_string(data, offset)
    tx_power,     offset = _read_qt_string(data, offset)
    comments,     offset = _read_qt_string(data, offset)
    name,         offset = _read_qt_string(data, offset)
    dt_on,        offset = _read_qt_datetime(data, offset)
    op_call,      offset = _read_qt_string(data, offset)
    my_call,      offset = _read_qt_string(data, offset)
    my_grid,      offset = _read_qt_string(data, offset)
    if not dx_call:
        return None
    freq_mhz = round(freq_hz / 1_000_000, 6) if freq_hz else None
    # Determine band from frequency
    band = freq_to_band(freq_mhz) if freq_mhz else ""
    # Use dt_on for the QSO time if available
    qso_dt = dt_on or dt_off
    return {
        "callsign":      dx_call.strip().upper(),
        "name":          (name or "").strip(),
        "mode":          (mode or "").strip().upper(),
        "freq_mhz":      f"{freq_mhz:.6f}".rstrip('0').rstrip('.') if freq_mhz else "",
        "band":          band,
        "my_rst_sent":   (rst_sent or "").strip(),
        "their_rst_rcvd":(rst_rcvd or "").strip(),
        "remarks":       (comments or "").strip(),
        "date_worked":   qso_dt.strftime("%Y-%m-%d") if qso_dt else date.today().isoformat(),
        "time_worked":   qso_dt.strftime("%H:%M:%S") if qso_dt else datetime.utcnow().strftime("%H:%M:%S"),
        "source":        client_id or "Digital-APP",
    }


def freq_to_band(freq_mhz):
    """Return the amateur band string for a given frequency in MHz."""
    if not freq_mhz:
        return ""
    f = float(freq_mhz)
    bands = [
        (1.8, 2.0, "160m"), (3.5, 4.0, "80m"), (5.3, 5.4, "60m"),
        (7.0, 7.3, "40m"), (10.1, 10.15, "30m"), (14.0, 14.35, "20m"),
        (18.068, 18.168, "17m"), (21.0, 21.45, "15m"), (24.89, 24.99, "12m"),
        (28.0, 29.7, "10m"), (50.0, 54.0, "6m"), (144.0, 148.0, "2m"),
        (420.0, 450.0, "70cm"),
    ]
    for lo, hi, name in bands:
        if lo <= f <= hi:
            return name
    return ""


def adif_to_qso(fields, source="Digital-APP"):
    """Convert parsed ADIF field dict to a QSO dict for saving."""
    callsign = fields.get("call", "").strip().upper()
    if not callsign:
        return None
    # Parse date: ADIF QSO_DATE is YYYYMMDD
    raw_date = fields.get("qso_date", fields.get("qso_date_off", ""))
    try:
        qso_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}" if len(raw_date) == 8 else date.today().isoformat()
    except Exception:
        qso_date = date.today().isoformat()
    # Parse time: ADIF TIME_ON is HHMMSS or HHMM
    raw_time = fields.get("time_on", fields.get("time_off", ""))
    try:
        if len(raw_time) >= 6:
            qso_time = f"{raw_time[:2]}:{raw_time[2:4]}:{raw_time[4:6]}"
        elif len(raw_time) == 4:
            qso_time = f"{raw_time[:2]}:{raw_time[2:4]}:00"
        else:
            qso_time = datetime.utcnow().strftime("%H:%M:%S")
    except Exception:
        qso_time = datetime.utcnow().strftime("%H:%M:%S")
    freq_raw = fields.get("freq", "")
    try:
        freq_mhz = float(freq_raw) if freq_raw else None
    except ValueError:
        freq_mhz = None
    band = fields.get("band", freq_to_band(freq_mhz) if freq_mhz else "")
    mode = fields.get("mode", "")
    if fields.get("submode"):
        mode = fields["submode"]          # prefer submode (e.g. VARA HF vs DYNAMIC)
    return {
        "callsign":       callsign,
        "name":           fields.get("name", "").strip(),
        "qth":            fields.get("qth", fields.get("gridsquare", "")).strip(),
        "mode":           mode.strip().upper(),
        "freq_mhz":       f"{freq_mhz:.6f}".rstrip('0').rstrip('.') if freq_mhz else "",
        "band":           band,
        "my_rst_sent":    fields.get("rst_sent", "").strip(),
        "their_rst_rcvd": fields.get("rst_rcvd", "").strip(),
        "remarks":        fields.get("comment", fields.get("comments", fields.get("notes", ""))).strip(),
        "date_worked":    qso_date,
        "time_worked":    qso_time,
        "source":         source,
    }


def digital_save_qso(qso_dict):
    """Save a QSO from a digital app to the database and queue a browser event."""
    if not qso_dict or not qso_dict.get("callsign"):
        return
    source = qso_dict.pop("source", "Digital-APP")
    try:
        conn = get_db()
        conn.execute("""
            INSERT INTO qso_log
                (callsign, name, qth, date_worked, time_worked, band, mode,
                 freq_mhz, my_rst_sent, their_rst_rcvd, remarks, contest_name)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            qso_dict.get("callsign", ""),
            qso_dict.get("name", ""),
            qso_dict.get("qth", ""),
            qso_dict.get("date_worked") or date.today().isoformat(),
            qso_dict.get("time_worked") or datetime.utcnow().strftime("%H:%M:%S"),
            qso_dict.get("band", ""),
            qso_dict.get("mode", ""),
            float(qso_dict["freq_mhz"]) if qso_dict.get("freq_mhz") else None,
            qso_dict.get("my_rst_sent", ""),
            qso_dict.get("their_rst_rcvd", ""),
            qso_dict.get("remarks", ""),
            "",
        ))
        conn.commit()
        conn.close()
        _digital_events.append({
            "callsign": qso_dict["callsign"],
            "mode":     qso_dict.get("mode", ""),
            "band":     qso_dict.get("band", ""),
            "freq_mhz": qso_dict.get("freq_mhz", ""),
            "source":   source,
        })
        print(f"Digital-APP QSO logged: {qso_dict['callsign']} via {source}")
        # Optional QRZ upload
        if QRZ_LOGBOOK_KEY and QRZ_LOGBOOK_UPLOAD_ENABLED:
            threading.Thread(target=qrz_logbook_upload,
                             args=(qso_dict | {"callsign": qso_dict["callsign"]},),
                             daemon=True).start()
        # Optional LoTW upload
        if LOTW_TQSL_PATH and LOTW_UPLOAD_ENABLED:
            threading.Thread(target=lotw_upload,
                             args=(qso_dict | {"callsign": qso_dict["callsign"]},),
                             daemon=True).start()
        # Optional Club Log upload
        if CLUBLOG_UPLOAD_ENABLED and not _clublog_blocked:
            threading.Thread(target=clublog_upload,
                             args=(qso_dict | {"callsign": qso_dict["callsign"]},),
                             daemon=True).start()
        # Optional eQSL upload
        if EQSL_UPLOAD_ENABLED:
            threading.Thread(target=eqsl_upload,
                             args=(qso_dict | {"callsign": qso_dict["callsign"]},),
                             daemon=True).start()
    except Exception as e:
        print(f"digital_save_qso error: {e}")


# ─── Digital App UDP Listener ─────────────────────────────────────────────────
def digital_udp_listener():
    """
    Background thread: listens on UDP for QSOLogged packets from WSJT-X / JTDX /
    MSHV (binary Qt format) or VarAC / Log4OM-style ADIF text datagrams.
    Detects format automatically by checking for the WSJT-X magic number.
    """
    import time as _t
    while True:
        if not DIGITAL_UDP_ENABLED:
            _t.sleep(2)
            continue
        s = None
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("0.0.0.0", DIGITAL_UDP_PORT))
            s.settimeout(2.0)
            print(f"Digital-APP: UDP listener active on port {DIGITAL_UDP_PORT}")
            while DIGITAL_UDP_ENABLED:
                try:
                    data, addr = s.recvfrom(65535)
                    if not data:
                        continue
                    # Detect format: WSJT-X binary starts with magic 0xADBCCBDA
                    if len(data) >= 4 and struct.unpack_from('>I', data, 0)[0] == _WSJTX_MAGIC:
                        msg_type = struct.unpack_from('>I', data, 8)[0] if len(data) >= 12 else 0
                        print(f"Digital-APP: binary UDP pkt from {addr[0]}, {len(data)}B, msg_type={msg_type}")
                        qso = parse_wsjtx_binary(data)
                        if qso:
                            src = qso.pop("source", "WSJT-X/JTDX")
                            qso["source"] = src
                            print(f"Digital-APP: logged {qso.get('callsign')} via {src}")
                            digital_save_qso(qso)
                        else:
                            print(f"Digital-APP: binary pkt ignored (not type-5 QSOLogged or parse error)")
                    else:
                        # Try as ADIF text (VarAC, Log4OM etc.)
                        text = data.decode('utf-8', errors='replace')
                        print(f"Digital-APP: ADIF UDP pkt from {addr[0]}, {len(data)}B")
                        if '<' in text and '>' in text:
                            fields = parse_adif_string(text)
                            qso    = adif_to_qso(fields, source=f"Digital-APP ({addr[0]})")
                            if qso:
                                print(f"Digital-APP: logged {qso.get('callsign')} via ADIF UDP")
                                digital_save_qso(qso)
                except socket.timeout:
                    continue
        except Exception as e:
            print(f"Digital-APP UDP error (port {DIGITAL_UDP_PORT}): {e}")
            _t.sleep(5)
        finally:
            if s:
                try: s.close()
                except: pass


# ─── Digital App TCP Server ────────────────────────────────────────────────────
def digital_tcp_server():
    """
    Background thread: TCP server that accepts ADIF QSO records sent by
    VarAC (DXKeeper/N1MM/Logger32 mode) and other loggers on port 52001.
    Each connection may send one or more ADIF records terminated by <EOR>.
    """
    while True:
        if not DIGITAL_TCP_ENABLED:
            threading.Event().wait(5)
            continue
        srv = None
        try:
            srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind(("0.0.0.0", DIGITAL_TCP_PORT))
            srv.listen(5)
            srv.settimeout(2.0)
            print(f"Digital-APP: TCP server on port {DIGITAL_TCP_PORT}")
            while DIGITAL_TCP_ENABLED:
                try:
                    conn, addr = srv.accept()
                    threading.Thread(
                        target=_handle_tcp_client,
                        args=(conn, addr),
                        daemon=True
                    ).start()
                except socket.timeout:
                    continue
        except Exception as e:
            print(f"Digital-APP TCP error: {e}")
        finally:
            if srv:
                try: srv.close()
                except Exception: pass
        threading.Event().wait(5)


def _handle_tcp_client(conn, addr):
    buf = ""
    try:
        conn.settimeout(10.0)
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            buf += chunk.decode('utf-8', errors='replace')
            # Process all complete ADIF records (terminated by <EOR>)
            while True:
                eor = _re.search(r'<EOR>', buf, _re.IGNORECASE)
                if not eor:
                    break
                record = buf[:eor.end()]
                buf    = buf[eor.end():]
                fields = parse_adif_string(record)
                qso    = adif_to_qso(fields, source=f"Digital-APP (TCP {addr[0]})")
                if qso:
                    digital_save_qso(qso)
    except Exception as e:
        print(f"Digital-APP TCP client error: {e}")
    finally:
        try: conn.close()
        except Exception: pass


# ─── PstRotator live AZ poller ───────────────────────────────────────────────
def _pstrotator_poller():
    """
    Daemon thread: polls live azimuth from PstRotator via UDP query/reply.

    PstRotator protocol (documented external control interface):
      - Send query  : UDP to ROTATOR_PORT (default 12000): <PST>AZ?</PST>
      - Receive reply: UDP on ROTATOR_PORT+1 (default 12001): AZ:xxx.x<CR>

    Commands (move/stop) continue to use rotator_send_azimuth() / rotator_send_stop().
    """
    global _rot_live_az
    import time as _t

    while True:
        if not ROTATOR_ENABLED or ROTATOR_PROTOCOL != "pstrotator":
            with _rot_live_az_lock:
                _rot_live_az = None
            _t.sleep(5)
            continue

        listen_port = ROTATOR_PORT + 1  # PstRotator replies on command_port + 1
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(0.8)   # 0.8 s — enough for any local response; limits stall on missed reply
            sock.bind(("0.0.0.0", listen_port))
            print(f"RotatorPoller: UDP listening on port {listen_port}, "
                  f"querying {ROTATOR_HOST}:{ROTATOR_PORT}")

            while ROTATOR_ENABLED and ROTATOR_PROTOCOL == "pstrotator":
                # Send AZ query
                try:
                    sock.sendto(b"<PST>AZ?</PST>", (ROTATOR_HOST, ROTATOR_PORT))
                except Exception as send_exc:
                    print(f"RotatorPoller send error: {send_exc}")

                # Wait for reply
                try:
                    data, _ = sock.recvfrom(256)
                    text = data.decode("ascii", errors="ignore").strip()
                    # Format: AZ:xxx.x  (colon-separated, no XML)
                    if text.upper().startswith("AZ:"):
                        try:
                            az = float(text[3:].strip())
                            with _rot_live_az_lock:
                                _rot_live_az = az
                        except ValueError:
                            pass
                except socket.timeout:
                    pass  # no reply this cycle — keep trying
                except ConnectionResetError:
                    pass  # Windows: ICMP port-unreachable when PstRotator not running — ignore

                _t.sleep(0.15)  # poll every 150 ms — smooth real-time AZ display

        except Exception as exc:
            _log(f"RotatorPoller: {exc}")   # debug report only — not console spam
            with _rot_live_az_lock:
                _rot_live_az = None
        finally:
            if sock:
                try: sock.close()
                except: pass

        _t.sleep(5)  # wait before rebinding


# Start the poller immediately as a daemon thread (safe to run from source
# or when imported by launcher.py — it sleeps until rotator is enabled).
threading.Thread(target=_pstrotator_poller, daemon=True,
                 name="PstRotatorPoller").start()


# ─── ARCO TCP poller (v1.08.1-beta) ──────────────────────────────────────────
def _arco_tcp_poller():
    """
    Daemon thread: polls live AZ + EL from a microHAM ARCO controller via TCP
    using its GS-232A emulation mode.

    Protocol (GS-232A over TCP):
      Query  : "C2\\r"            (combined AZ + EL query)
      Reply  : "+0nnn+0eee\\r\\n"  (AZ then EL, both 3-digit)

    ARCO supports up to 4 simultaneous TCP connections so this poller will not
    starve the S.A.T. controller or any other client.

    On any I/O error the poller raises the fault flag (visible as a red dot in
    the UI) and reconnects after a short backoff.
    """
    global _rot_live_az, _rot_live_el, _rot_fault
    import time as _t
    import re as _re

    LINE_RE = _re.compile(r"([+\-]?\d{3,4})\s*([+\-]?\d{3,4})?")

    while True:
        if not ROTATOR_ENABLED or ROTATOR_PROTOCOL != "arco_tcp":
            with _rot_state_lock:
                _rot_live_az = None
                _rot_live_el = None
                _rot_fault   = None
            _t.sleep(2)
            continue

        sock = None
        try:
            _rot_dbg(f"ARCO: connecting to {ROTATOR_HOST}:{ROTATOR_PORT}")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3.0)
            sock.connect((ROTATOR_HOST, ROTATOR_PORT))
            sock.settimeout(1.5)
            _rot_dbg("ARCO: connected")
            with _rot_state_lock:
                _rot_fault = ""   # connection good

            buf = b""
            while ROTATOR_ENABLED and ROTATOR_PROTOCOL == "arco_tcp":
                # Send combined AZ+EL query
                try:
                    sock.sendall(b"C2\r")
                    if ROTATOR_DEBUG:
                        _rot_dbg("ARCO TX: C2")
                except Exception as e:
                    _rot_dbg(f"ARCO TX error: {e}")
                    raise

                # Read reply (may need multiple recv calls)
                try:
                    chunk = sock.recv(256)
                    if not chunk:
                        _rot_dbg("ARCO: socket closed by peer")
                        raise OSError("peer closed")
                    buf += chunk
                except socket.timeout:
                    _rot_dbg("ARCO: recv timeout (no reply to C2)")
                    chunk = b""

                # Parse any complete lines
                while b"\r" in buf or b"\n" in buf:
                    # split on first CR or LF
                    for sep in (b"\r\n", b"\r", b"\n"):
                        if sep in buf:
                            line, buf = buf.split(sep, 1)
                            break
                    else:
                        break
                    text = line.decode("ascii", errors="ignore").strip()
                    if not text:
                        continue
                    if ROTATOR_DEBUG:
                        _rot_dbg(f"ARCO RX: {text}")
                    m = LINE_RE.search(text)
                    if m:
                        try:
                            az = float(m.group(1))
                            el = float(m.group(2)) if m.group(2) else None
                            with _rot_state_lock:
                                _rot_live_az = az
                                if el is not None:
                                    _rot_live_el = el
                        except ValueError:
                            pass

                _t.sleep(0.20)   # ~5 Hz poll

        except Exception as exc:
            _rot_dbg(f"ARCO poller error: {exc}")
            with _rot_state_lock:
                _rot_live_az = None
                _rot_live_el = None
                _rot_fault   = str(exc) or "Connection lost"
        finally:
            if sock:
                try: sock.close()
                except: pass

        _t.sleep(3)   # backoff before reconnect


threading.Thread(target=_arco_tcp_poller, daemon=True,
                 name="ArcoTcpPoller").start()


# ─── Rotator Control ──────────────────────────────────────────────────────────
def rotator_send_azimuth(azimuth):
    """
    Send an azimuth command to the rotator controller.

    PstRotator : UDP datagram to ROTATOR_PORT (default 12000).
                 Format: <PST><AZIMUTH>270</AZIMUTH></PST>
                 This is the documented external control interface used by
                 N1MM Logger+, Swisslog, and Logger32.  UDP is connectionless
                 so there is no TCP handshake to refuse.
    GS-232     : TCP, M command  e.g. M270\r\n
    EasyComm II: TCP, AZ command e.g. AZ270.0\r\n
    """
    if not ROTATOR_ENABLED:
        return False, "Rotator not enabled"
    try:
        az = round(float(azimuth), 1)
        if ROTATOR_PROTOCOL == "gs232":
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3.0)
            s.connect((ROTATOR_HOST, ROTATOR_PORT))
            s.sendall(f"M{int(az):03d}\r\n".encode())
            s.close()
        elif ROTATOR_PROTOCOL == "arco_tcp":
            # microHAM ARCO — GS-232A emulation over TCP. M command sets azimuth.
            cmd = f"M{int(az):03d}\r".encode()
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3.0)
            s.connect((ROTATOR_HOST, ROTATOR_PORT))
            s.sendall(cmd)
            s.close()
            _rot_dbg(f"ARCO TX: M{int(az):03d}  (set AZ={az:.1f})")
        elif ROTATOR_PROTOCOL == "easycomm":
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3.0)
            s.connect((ROTATOR_HOST, ROTATOR_PORT))
            s.sendall(f"AZ{az:.1f}\r\n".encode())
            s.close()
        else:  # pstrotator — UDP, documented external control interface
            cmd = f"<PST><AZIMUTH>{az:.1f}</AZIMUTH></PST>".encode()
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(cmd, (ROTATOR_HOST, ROTATOR_PORT))
            s.close()
        print(f"Rotator: sent AZ={az:.1f} via {ROTATOR_PROTOCOL}")
        return True, None
    except Exception as e:
        print(f"Rotator error: {e}")
        return False, str(e)


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.after_request
def no_cache_html(response):
    """Prevent browser from caching the main app pages so updates take effect
    immediately after a hard refresh (Ctrl+Shift+R) without needing a PC restart."""
    if request.path in ("/", "/help"):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"]        = "no-cache"
        response.headers["Expires"]       = "0"
    return response


@app.route("/")
def index():
    return render_template("index.html", version=VERSION)

@app.route("/help")
def help_page():
    return render_template("help.html", version=VERSION)


@app.route("/changelog")
def changelog():
    import pathlib
    candidates = [
        # Frozen exe: sys.executable = C:\SDRLoggerPlus\SDRLoggerPlus.exe → parent = C:\SDRLoggerPlus\
        pathlib.Path(sys.executable).parent / "CHANGELOG.txt",
        # Dev mode: beside main.py
        pathlib.Path(__file__).parent / "CHANGELOG.txt",
        # AppData (zip-updated copy)
        pathlib.Path(os.environ.get("SDRLOGGERPLUS_DATA", "")) / "CHANGELOG.txt",
    ]
    text = None
    for p in candidates:
        if p.exists():
            text = p.read_text(encoding="utf-8")
            break
    if text is None:
        text = "CHANGELOG.txt not found."
    # Beta banner — shown only when VERSION contains "beta"
    beta_banner = ""
    if "beta" in VERSION.lower():
        beta_banner = (
            '<div style="background:#3a1f00;border:1px solid #ffae00;color:#ffd17a;'
            'padding:12px 16px;border-radius:6px;margin-bottom:20px;'
            'font-family:sans-serif;font-size:13px;line-height:1.5;">'
            '<strong style="color:#ffae00;">⚠ PRIVATE BETA BUILD — ' + VERSION + '</strong><br>'
            'This build is for invited testers only. Not for public distribution. '
            'Please report issues with the rotator <em>📋 Logs</em> snapshot to N8SDR.'
            '</div>'
        )
    # Serve as a styled pre-formatted HTML page
    html = f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<title>SDRLogger+ — Change Log</title>
<style>
  body{{background:#0a0d12;color:#cdd9e5;font-family:'Courier New',monospace;font-size:14px;
    line-height:1.8;padding:40px;max-width:820px;margin:0 auto;}}
  h1{{color:#00e5ff;font-family:sans-serif;letter-spacing:2px;margin-bottom:4px;}}
  p.sub{{color:#5a7080;font-size:12px;margin-bottom:30px;}}
  pre{{white-space:pre-wrap;word-break:break-word;}}
  a{{color:#00e5ff;}}
</style></head><body>
<h1>SDRLogger+ — Change Log</h1>
<p class="sub"><a href="/help">← Back to Quick Start Guide</a></p>
{beta_banner}
<pre>{text}</pre>
</body></html>"""
    return html


# ── K1EL WinKeyer serial management ──────────────────────────────────────────

def _wk_build_pincfg():
    """Build the PINCFG byte from user settings (Key port, PTT enable)."""
    cfg = 0
    if WINKEYER_KEY_OUT in ("port1", "both"):
        cfg |= 0x04          # Bit 2 = KEYPORT1
    if WINKEYER_KEY_OUT in ("port2", "both"):
        cfg |= 0x08          # Bit 3 = KEYPORT0 (Port 2)
    if WINKEYER_PTT:
        cfg |= 0x01          # Bit 0 = USEPTT
    return cfg


def _wk_connect():
    """Open the serial port, sync the parser, enter host mode, configure."""
    global _wk_serial, _wk_is_open, _wk_version
    import serial as _ser

    if _wk_is_open:
        return True
    if not WINKEYER_PORT:
        return False

    try:
        _log(f"WinKeyer: opening {WINKEYER_PORT} at 1200 baud 8N2 ...")
        ser = _ser.Serial(
            port=WINKEYER_PORT,
            baudrate=1200,
            bytesize=_ser.EIGHTBITS,
            parity=_ser.PARITY_NONE,
            stopbits=_ser.STOPBITS_TWO,  # 8N2 per K1EL spec
            timeout=1.0,
            write_timeout=3.0,
            dsrdtr=False,                # manual DTR — avoids DSR hang
        )
        ser.dtr = True                   # WKmini requires DTR asserted
        import time
        time.sleep(0.1)

        # ── Phase 1: Admin:Close any stale session ──
        with _wk_lock:
            ser.write(b'\x00\x03')       # Admin:Close
            ser.flush()
        time.sleep(0.5)
        ser.reset_input_buffer()

        # ── Phase 2: parser sync — four null commands ──
        with _wk_lock:
            ser.write(b'\x13\x13\x13\x13')
            ser.flush()
        time.sleep(0.1)
        ser.reset_input_buffer()

        # ── Phase 3: Admin:Open — WinKeyer returns firmware version byte ──
        with _wk_lock:
            ser.write(b'\x00\x02')
            ser.flush()
        version_byte = ser.read(1)
        if not version_byte:
            _log("WinKeyer: no version response — device not found or wrong port")
            ser.close()
            return False

        _wk_version = ord(version_byte)
        _log(f"WinKeyer: connected — firmware version {_wk_version}")

        _wk_serial = ser

        # ── Phase 4: configure device ──
        _wk_configure_locked()

        _wk_is_open = True
        return True

    except Exception as e:
        _log(f"WinKeyer: connect failed — {e}")
        _wk_is_open = False
        _wk_version = 0
        if _wk_serial:
            try:
                _wk_serial.close()
            except Exception:
                pass
            _wk_serial = None
        return False


def _wk_configure_locked():
    """Push current user settings to the connected WinKeyer."""
    ser = _wk_serial
    if not ser or not ser.is_open:
        return
    try:
        with _wk_lock:
            # 0x09 = PINCFG (Port routing + PTT)
            pincfg = _wk_build_pincfg()
            ser.write(bytes([0x09, pincfg]))
            ser.flush()

            # 0x0E = WinKeyer Mode register (keyer mode)
            mode_byte = {"iambicb": 0x00, "iambica": 0x01,
                         "ultimatic": 0x02, "bug": 0x03}.get(WINKEYER_MODE, 0x00)
            ser.write(bytes([0x0E, mode_byte]))
            ser.flush()

            # 0x04 = PTT Lead/Tail (units of 10 ms)
            lead = max(0, min(250, int(WINKEYER_PTT_LEAD / 10)))
            tail = max(0, min(250, int(WINKEYER_PTT_TAIL / 10)))
            ser.write(bytes([0x04, lead, tail]))
            ser.flush()

            # 0x02 = WPM speed
            wpm = max(5, min(99, WINKEYER_WPM))
            ser.write(bytes([0x02, wpm]))
            ser.flush()

        _log(f"WinKeyer: configured — pincfg=0x{pincfg:02X}(cmd 0x09) "
             f"mode=0x{mode_byte:02X}(cmd 0x0E) "
             f"ptt_lead={WINKEYER_PTT_LEAD}ms ptt_tail={WINKEYER_PTT_TAIL}ms "
             f"wpm={WINKEYER_WPM}")
    except Exception as e:
        _log(f"WinKeyer: configure error — {e}")


def _wk_disconnect():
    """Close host-mode session and release the serial port."""
    global _wk_serial, _wk_is_open, _wk_version
    ser = _wk_serial
    if ser:
        try:
            with _wk_lock:
                ser.write(b'\x00\x03')   # Admin:Close
                ser.flush()
            import time
            time.sleep(0.1)
            ser.close()
        except Exception:
            pass
    _wk_serial = None
    _wk_is_open = False
    _wk_version = 0
    _log("WinKeyer: disconnected")


def _wk_send_text(text):
    """Send ASCII text to WinKeyer for CW transmission (max 32-char chunks)."""
    ser = _wk_serial
    if not ser or not ser.is_open or not _wk_is_open:
        return False
    try:
        data = text.upper().encode("ascii", errors="ignore")
        with _wk_lock:
            # Send in 32-byte chunks (WinKeyer internal buffer limit)
            for i in range(0, len(data), 32):
                ser.write(data[i:i+32])
                ser.flush()
                if i + 32 < len(data):
                    import time
                    time.sleep(0.05)   # brief pause between chunks
        return True
    except Exception as e:
        _log(f"WinKeyer: send error — {e}")
        return False


def _wk_abort():
    """Clear the WinKeyer buffer — stops transmission immediately."""
    ser = _wk_serial
    if not ser or not ser.is_open:
        return
    try:
        with _wk_lock:
            ser.write(b'\x0A')           # Clear Buffer command
            ser.flush()
    except Exception:
        pass


def _wk_set_speed(wpm):
    """Update WinKeyer WPM speed on the fly."""
    ser = _wk_serial
    if not ser or not ser.is_open or not _wk_is_open:
        return
    wpm = max(5, min(99, int(wpm)))
    try:
        with _wk_lock:
            ser.write(bytes([0x02, wpm]))
            ser.flush()
    except Exception:
        pass


def winkeyer_manager():
    """Background thread: auto-connect / reconnect WinKeyer when enabled."""
    import time
    while True:
        if WINKEYER_ENABLED and WINKEYER_PORT and not _wk_is_open:
            _wk_connect()
        elif not WINKEYER_ENABLED and _wk_is_open:
            _wk_disconnect()
        time.sleep(5)


# ── CW Keyer / Decoder ────────────────────────────────────────────────────────
_cw_wpm      = 20
_cw_break_in = False
_cw_serial   = 1

_CW_SERIAL_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cw_serial.json")

def _load_cw_serial():
    global _cw_serial
    try:
        if os.path.exists(_CW_SERIAL_FILE):
            with open(_CW_SERIAL_FILE) as _f:
                _cw_serial = max(1, int(json.load(_f).get("serial", 1)))
    except Exception:
        _cw_serial = 1

def _save_cw_serial():
    try:
        with open(_CW_SERIAL_FILE, "w") as _f:
            json.dump({"serial": _cw_serial}, _f)
    except Exception:
        pass

@app.route("/cw")
def cw_page():
    return render_template("cw.html", version=VERSION)

# ─── Propagation Forecast (VOACAP-style) ────────────────────────────────────
# Opens as a standalone popup window (like /cw). Accepts optional ?call= to
# pre-fill the target callsign — the QSO entry panel's 📡 VOACAP button
# passes the currently-looked-up callsign here. Without ?call= the page is
# a standalone planning tool: any grid/callsign in, band-by-band forecast out.
@app.route("/propagation")
def propagation_page():
    return render_template("propagation.html", version=VERSION)

@app.route("/api/cw_send", methods=["POST"])
def api_cw_send():
    global _cw_wpm
    data = request.json or {}
    text = str(data.get("text", "")).strip()
    wpm  = int(data.get("wpm", _cw_wpm))
    if not text:
        return jsonify({"ok": False, "error": "No text provided"})
    _cw_wpm = wpm
    if tci_ws_connected:
        r1 = send_tci_command(f"cw_macros_speed:{wpm}")
        r2 = send_tci_command(f"cw_macros:0,{text}")
        _log(f"TCI CW: speed_cmd={'OK' if r1 else 'FAIL'} macros_cmd={'OK' if r2 else 'FAIL'} wpm={wpm} text='{text}'")
        return jsonify({"ok": True, "path": "tci"})
    if WINKEYER_ENABLED and _wk_is_open:
        _log(f"WinKeyer: sending CW — wpm={wpm} text='{text}'")
        _wk_set_speed(wpm)
        if _wk_send_text(text):
            _log("WinKeyer: send OK")
            return jsonify({"ok": True, "path": "winkeyer"})
        else:
            _log("WinKeyer: send FAILED — _wk_send_text returned False")
    if HAMLIB_ENABLED and hamlib_connected:
        resp = _hamlib_cmd(f"\\send_morse {text}")
        if resp is not None:
            return jsonify({"ok": True, "path": "hamlib"})
    return jsonify({"ok": False, "error": "No CW keyer active — enable TCI, WinKeyer, or HamLib in Settings"})

@app.route("/api/cw_stop", methods=["POST"])
def api_cw_stop():
    if tci_ws_connected:
        send_tci_command("cw_macros_stop")
        return jsonify({"ok": True, "path": "tci"})
    if WINKEYER_ENABLED and _wk_is_open:
        _wk_abort()
        return jsonify({"ok": True, "path": "winkeyer"})
    if FLRIG_ENABLED and flrig_connected:
        try:
            _flrig_server().rig.stop_morse()
            return jsonify({"ok": True, "path": "flrig"})
        except Exception:
            pass
    if HAMLIB_ENABLED and hamlib_connected:
        _hamlib_cmd("\\stop")
        return jsonify({"ok": True, "path": "hamlib"})
    return jsonify({"ok": False, "error": "Not connected"})

@app.route("/api/cw_speed", methods=["GET", "POST"])
def api_cw_speed():
    global _cw_wpm
    if request.method == "POST":
        _cw_wpm = max(5, min(60, int((request.json or {}).get("wpm", _cw_wpm))))
        if tci_ws_connected:
            send_tci_command(f"cw_macros_speed:{_cw_wpm}")
        if WINKEYER_ENABLED and _wk_is_open:
            _wk_set_speed(_cw_wpm)
    return jsonify({"wpm": _cw_wpm, "tci": tci_ws_connected,
                    "flrig":   bool(FLRIG_ENABLED and flrig_connected),
                    "hamlib":  bool(HAMLIB_ENABLED and hamlib_connected),
                    "winkeyer": bool(WINKEYER_ENABLED and _wk_is_open)})

@app.route("/api/cw_breakin", methods=["POST"])
def api_cw_breakin():
    global _cw_break_in
    _cw_break_in = bool((request.json or {}).get("enabled", False))
    # Note: TCI does not have a break-in command — break-in is configured in Thetis directly
    return jsonify({"ok": True, "enabled": _cw_break_in})

@app.route("/api/cw_status")
def api_cw_status():
    # Station-metadata fields are macro-source-only — pulled from runtime_settings,
    # used solely by the CW keyer's {MYRIG}/{MYANT}/{MYPWR}/{MYSTATE}/{MYCNTY}/{MYGRID}
    # token expander. They are NEVER stamped onto QSO records or ADIF exports.
    return jsonify({"tci":      tci_ws_connected,
                    "flrig":    bool(FLRIG_ENABLED and flrig_connected),
                    "hamlib":   bool(HAMLIB_ENABLED and hamlib_connected),
                    "winkeyer": bool(WINKEYER_ENABLED and _wk_is_open),
                    "wpm": _cw_wpm, "breakin": _cw_break_in,
                    "mycall":  MY_CALLSIGN,
                    "myname":  MY_NAME,
                    "mygrid":  runtime_settings.get("grid", ""),
                    "myrig":   runtime_settings.get("myrig", ""),
                    "myant":   runtime_settings.get("myantenna", ""),
                    "mypwr":   runtime_settings.get("mypower", ""),
                    "mystate": runtime_settings.get("mystate", ""),
                    "mycnty":  runtime_settings.get("mycounty", ""),
                    "serial": _cw_serial,
                    "cw_pitch": _tci_cw_pitch})

@app.route("/api/cw_serial", methods=["GET", "POST"])
def api_cw_serial():
    global _cw_serial
    if request.method == "POST":
        data = request.json or {}
        if "reset" in data:
            _cw_serial = max(1, int(data["reset"]))
        elif data.get("increment"):
            _cw_serial += 1
        elif "set" in data:
            _cw_serial = max(1, int(data["set"]))
        _save_cw_serial()
    return jsonify({"serial": _cw_serial})

@app.route("/api/cw_tone", methods=["POST"])
def api_cw_tone():
    """Set CW sidetone pitch in Thetis via TCI (cw_pitch:0,<hz>)."""
    data = request.json or {}
    hz = max(300, min(1200, int(data.get("hz", 700))))
    send_tci_command(f"cw_pitch:0,{hz};")
    return jsonify({"ok": True, "hz": hz})

@app.route("/api/tci_info")
def api_tci_info():
    return jsonify({"host": TCI_HOST, "port": TCI_PORT, "connected": tci_ws_connected})


@app.route("/api/save_qso", methods=["POST"])
def save_qso():
    data = request.json
    callsign = data.get("callsign", "").strip().upper()
    if not callsign:
        return jsonify({"ok": False, "error": "Callsign is required"}), 400

    conn = get_db()
    conn.execute("""
        INSERT INTO qso_log
            (callsign, name, qth, date_worked, time_worked, band, mode,
             freq_mhz, my_rst_sent, their_rst_rcvd, remarks, contest_name,
             pota_ref, pota_p2p, state, country, gridsquare,
             prop_mode, sat_name, sat_catno, transponder_name,
             uplink_freq, downlink_freq, uplink_mode, downlink_mode,
             aos_az, los_az, my_grid, my_lat, my_lon)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        callsign,
        data.get("name", ""),
        data.get("qth", ""),
        data.get("date_worked") or date.today().isoformat(),
        data.get("time_worked") or datetime.utcnow().strftime("%H:%M:%S"),
        data.get("band", ""),
        data.get("mode", ""),
        float(data["freq_mhz"]) if data.get("freq_mhz") else None,
        data.get("my_rst_sent", "59"),
        data.get("their_rst_rcvd", "59"),
        data.get("remarks", ""),
        data.get("contest_name", ""),
        data.get("pota_ref", "") or "",
        data.get("pota_p2p", "") or "",
        data.get("state", ""),
        data.get("country", ""),
        data.get("gridsquare", ""),
        data.get("prop_mode", ""),
        data.get("sat_name", ""),
        data.get("sat_catno", ""),
        data.get("transponder_name", ""),
        data.get("uplink_freq", ""),
        data.get("downlink_freq", ""),
        data.get("uplink_mode", ""),
        data.get("downlink_mode", ""),
        data.get("aos_az", ""),
        data.get("los_az", ""),
        data.get("my_grid", ""),
        data.get("my_lat", ""),
        data.get("my_lon", ""),
    ))
    conn.commit()
    conn.close()

    # Update worked DXCC cache
    _worked_cache_add(callsign, data.get("band", ""), data.get("mode", ""))

    # Optional telnet spot
    if TELNET_ENABLED:
        threading.Thread(target=send_telnet_spot,
                         args=(callsign, data.get("freq_mhz"), data.get("mode")),
                         daemon=True).start()

    # Auto-upload to all configured services in background (non-blocking)
    qso_data = data | {"callsign": callsign}
    def _bg_uploads(qd):
        if QRZ_LOGBOOK_KEY and QRZ_LOGBOOK_UPLOAD_ENABLED:
            _, err = qrz_logbook_upload(qd)
            print(f"QRZ Logbook upload {'failed: '+err if err else 'OK — '+qd['callsign']}")
        if LOTW_TQSL_PATH and LOTW_UPLOAD_ENABLED:
            _, err = lotw_upload(qd)
            print(f"LoTW upload {'failed: '+err if err else 'OK — '+qd['callsign']}")
        if CLUBLOG_UPLOAD_ENABLED and not _clublog_blocked:
            _, err = clublog_upload(qd)
            print(f"Club Log upload {'failed: '+err if err else 'OK — '+qd['callsign']}")
        if EQSL_UPLOAD_ENABLED:
            _, err = eqsl_upload(qd)
            print(f"eQSL upload {'failed: '+err if err else 'OK — '+qd['callsign']}")
    threading.Thread(target=_bg_uploads, args=(qso_data,), daemon=True).start()

    return jsonify({"ok": True, "callsign": callsign})


@app.route("/api/worked_before/<callsign>")
def worked_before(callsign):
    callsign = callsign.upper()
    conn = get_db()
    rows = conn.execute(
        "SELECT date_worked, band, mode FROM qso_log WHERE callsign=? ORDER BY date_worked DESC",
        (callsign,)
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/tci_data")
def tci_data():
    """Browser polls this to get latest data pushed from Thetis SDR."""
    data = dict(latest_tci)
    data["connected"] = tci_ws_connected and TCI_ENABLED
    data["enabled"]   = TCI_ENABLED
    latest_tci["callsign"] = ""   # clear callsign after reading (one-shot)
    # keep freq_mhz and mode — they stay until SDR sends new values
    return jsonify(data)


@app.route("/api/debug_log")
def debug_log():
    """Return the recent in-memory error/event log as JSON."""
    return jsonify({"entries": list(_debug_log), "version": VERSION})


@app.route("/api/debug_report")
def debug_report():
    """Generate a plain-text debug report for bug reporting."""
    import platform
    from datetime import datetime
    lines = [
        "=" * 60,
        "SDRLogger+ Debug Report",
        f"Version  : {VERSION}",
        f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        f"OS       : {platform.system()} {platform.release()} ({platform.version()})",
        f"Python   : {platform.python_version()}",
        "=" * 60,
        "",
        "=== Rig Status ===",
        f"TCI      : {'ENABLED' if TCI_ENABLED else 'disabled'} | connected={tci_ws_connected}",
        f"flrig    : {'ENABLED' if FLRIG_ENABLED else 'disabled'} | connected={flrig_connected} | host={FLRIG_HOST}:{FLRIG_PORT}",
        f"HamLib   : {'ENABLED' if HAMLIB_ENABLED else 'disabled'} | connected={hamlib_connected} | host={HAMLIB_HOST}:{HAMLIB_PORT}",
        f"WinKeyer : {'ENABLED' if WINKEYER_ENABLED else 'disabled'} | open={_wk_is_open} | port={WINKEYER_PORT} | v{_wk_version}",
        "",
        "=== Settings (sanitized) ===",
        f"Callsign : {MY_CALLSIGN}",
        f"ITU Rgn  : {ITU_REGION}",
        f"TCI      : {TCI_HOST}:{TCI_PORT}",
        f"QRZ      : user={'set' if QRZ_USER else 'not set'} | logbook={'enabled' if QRZ_LOGBOOK_UPLOAD_ENABLED else 'off'}",
        f"LoTW     : {'enabled' if LOTW_UPLOAD_ENABLED else 'off'} | tqsl={'set' if LOTW_TQSL_PATH else 'not set'}",
        f"ClubLog  : {'enabled' if CLUBLOG_UPLOAD_ENABLED else 'off'} | email={'set' if CLUBLOG_EMAIL else 'not set'} | blocked={_clublog_blocked}",
        f"eQSL     : {'enabled' if EQSL_UPLOAD_ENABLED else 'off'} | user={'set' if EQSL_USER else 'not set'}",
        f"Rotator  : {'enabled' if ROTATOR_ENABLED else 'off'} | {ROTATOR_HOST}:{ROTATOR_PORT} ({ROTATOR_PROTOCOL})",
        f"Telnet   : {'enabled' if TELNET_ENABLED else 'off'} | {TELNET_SERVER}:{TELNET_PORT}",
        "",
        "=== Recent Log (newest last) ===",
    ]
    lines.extend(list(_debug_log) if _debug_log else ["  (no entries)"])
    lines += ["", "=" * 60, "End of report", "=" * 60]
    report_text = "\n".join(lines)
    from flask import Response
    return Response(
        report_text,
        mimetype="text/plain",
        headers={"Content-Disposition": f"attachment; filename=SDRLoggerPlus_debug_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt"}
    )


@app.route("/api/log_js_error", methods=["POST"])
def log_js_error():
    """Receive a JavaScript error from the browser and add it to the debug log."""
    data = request.json or {}
    msg  = data.get("message", "unknown JS error")
    url  = data.get("url", "")
    line = data.get("line", "")
    _log(f"JS ERROR: {msg} | {url} line {line}")
    return jsonify({"ok": True})


# ─── QRZ XML Session Manager ──────────────────────────────────────────────────
# Per the official spec (v1.34): authenticate once with username+password to get
# a session key, then reuse that key for all callsign lookups until it expires.
# The "API key" users enter IS their QRZ subscription password used at login.
# Their callsign is used as the username.
# Ref: https://www.qrz.com/page/current_spec.html

from xml.etree import ElementTree as ET

_qrz_session_key = None          # cached session key
_qrz_session_lock = threading.Lock()
QRZ_XML_URL = "https://xmldata.qrz.com/xml/current/"
QRZ_AGENT   = "HamLog/1.0"


def _qrz_parse_xml(text):
    """Parse QRZ XML response, return (root, session_node, error_string)."""
    try:
        root = ET.fromstring(text)
    except ET.ParseError as e:
        return None, None, f"XML parse error: {e}"
    ns = {"q": "http://xmldata.qrz.com"}
    session = root.find(".//q:Session", ns)
    # Check for session-level error
    err = root.find(".//q:Session/q:Error", ns)
    if err is not None and err.text:
        return root, session, err.text
    return root, session, None


def _qrz_login(username, password):
    """
    Login to QRZ XML service. Returns (session_key, error).
    username = your callsign, password = your QRZ XML subscription key.
    """
    try:
        resp = requests.get(
            QRZ_XML_URL,
            params={"username": username, "password": password, "agent": QRZ_AGENT},
            timeout=5
        )
        root, session, err = _qrz_parse_xml(resp.text)
        if err:
            return None, err
        ns = {"q": "http://xmldata.qrz.com"}
        key_el = root.find(".//q:Session/q:Key", ns)
        if key_el is None or not key_el.text:
            return None, "Login failed — no session key returned"
        return key_el.text, None
    except Exception as e:
        return None, str(e)


def _qrz_lookup_with_key(session_key, callsign):
    """
    Look up a callsign using an existing session key.
    Returns (data_dict, error, session_expired).
    """
    try:
        resp = requests.get(
            QRZ_XML_URL,
            params={"s": session_key, "callsign": callsign.upper(), "agent": QRZ_AGENT},
            timeout=5
        )
        root, session, err = _qrz_parse_xml(resp.text)
        ns = {"q": "http://xmldata.qrz.com"}

        # Check if session expired
        if err and ("session" in err.lower() or "timeout" in err.lower() or "invalid" in err.lower()):
            return None, err, True

        cs_el = root.find(".//q:Callsign", ns) if root is not None else None
        if cs_el is None:
            not_found = err or "Callsign not found"
            return None, not_found, False

        def _t(tag):
            el = cs_el.find(f"q:{tag}", ns)
            return (el.text or "").strip() if el is not None else ""

        fname = _t("fname")
        lname = _t("name")
        nickname = _t("nickname")
        name = f"{fname} {lname}".strip()
        if nickname:
            name = f"{name} ({nickname})" if name else nickname

        data = {
            "callsign": _t("call"),
            "name":     name,
            "qth":      ", ".join(filter(None, [_t("addr2"), _t("state"), _t("country")])),
            "grid":     _t("grid"),
            "email":    _t("email"),
            "class":    _t("class"),
            "country":  _t("country"),
            "cqzone":   _t("cqzone"),
            "ituzone":  _t("ituzone"),
            "lotw":     _t("lotw"),
            "eqsl":     _t("eqsl"),
        }
        return data, None, False
    except Exception as e:
        return None, str(e), False


def qrz_lookup(callsign):
    """
    Public function: look up a callsign on QRZ XML service.
    Handles session management, login, and automatic re-login on expiry.
    Returns (data_dict, error_string).
    Requires QRZ_USER = qrz.com username, QRZ_PASS = qrz.com password.
    """
    global _qrz_session_key

    if not QRZ_USER:
        return None, "QRZ username not configured — open Settings and enter your QRZ login"
    if not QRZ_PASS:
        return None, "QRZ password not configured — open Settings and enter your QRZ password"

    with _qrz_session_lock:
        if not _qrz_session_key:
            key, err = _qrz_login(QRZ_USER, QRZ_PASS)
            if err:
                return None, f"QRZ login failed: {err}"
            _qrz_session_key = key
            print(f"QRZ: logged in as {QRZ_USER}")

        data, err, expired = _qrz_lookup_with_key(_qrz_session_key, callsign)

        if expired:
            print("QRZ: session expired, re-logging in")
            _qrz_session_key = None
            key, login_err = _qrz_login(QRZ_USER, QRZ_PASS)
            if login_err:
                return None, f"QRZ re-login failed: {login_err}"
            _qrz_session_key = key
            data, err, _ = _qrz_lookup_with_key(_qrz_session_key, callsign)

        return data, err


@app.route("/api/qrz_lookup/<callsign>")
def qrz_lookup_route(callsign):
    if not QRZ_LOOKUP_ENABLED:
        return jsonify({"error": "QRZ lookup disabled"}), 503
    data, err = qrz_lookup(callsign.upper())
    if err:
        return jsonify({"error": err}), 400 if "not configured" in err or "callsign" in err.lower() else 404
    return jsonify(data)


@app.route("/api/qrz_test_lookup")
def qrz_test_lookup():
    """Test QRZ login with provided username and password, then look up W1AW."""
    global _qrz_session_key
    user = request.args.get("user", QRZ_USER)
    pwd  = request.args.get("pass", QRZ_PASS)

    if not user:
        return jsonify({"ok": False, "error": "Enter your QRZ username (callsign)"})
    if not pwd:
        return jsonify({"ok": False, "error": "Enter your QRZ password"})

    with _qrz_session_lock:
        session_key, err = _qrz_login(user, pwd)
        if err:
            return jsonify({"ok": False, "error": f"Login failed: {err}"})
        _qrz_session_key = session_key

    data, err, _ = _qrz_lookup_with_key(session_key, "W1AW")
    if err:
        return jsonify({"ok": False, "error": err})
    return jsonify({"ok": True, "callsign": data["callsign"], "name": data["name"], "qth": data["qth"]})


# ─── HamQTH Callsign Lookup ────────────────────────────────────────────────────
_hamqth_session_key  = None
_hamqth_session_lock = threading.Lock()
_HAMQTH_URL          = "https://www.hamqth.com/xml.php"


def _hamqth_login(username, password):
    """Login to HamQTH XML service. Returns (session_id, error)."""
    try:
        resp = requests.get(_HAMQTH_URL,
                            params={"u": username, "p": password},
                            timeout=5)
        root = ET.fromstring(resp.text)
        ns   = {"h": "https://www.hamqth.com"}
        sid  = root.find(".//h:session_id", ns)
        if sid is not None and sid.text:
            return sid.text.strip(), None
        err = root.find(".//h:error", ns)
        return None, (err.text.strip() if err is not None else "Login failed")
    except Exception as e:
        return None, str(e)


def _hamqth_lookup_with_key(session_id, callsign):
    """
    Look up a callsign using an existing HamQTH session.
    Returns (data_dict, error, session_expired).
    """
    try:
        resp = requests.get(_HAMQTH_URL,
                            params={"id": session_id, "callsign": callsign.upper(),
                                    "prg": "SDRLogger+"},
                            timeout=5)
        root = ET.fromstring(resp.text)
        ns   = {"h": "https://www.hamqth.com"}

        err_el = root.find(".//h:error", ns)
        if err_el is not None and err_el.text:
            msg = err_el.text.strip()
            expired = "session" in msg.lower() or "wrong session" in msg.lower()
            return None, msg, expired

        def _t(tag):
            el = root.find(f".//h:{tag}", ns)
            return (el.text or "").strip() if el is not None else ""

        # Name: prefer full name from adr_name, fall back to nick
        full_name = _t("adr_name")
        nick      = _t("nick")
        name      = full_name if full_name else nick

        qth_parts = [_t("qth"), _t("country")]
        data = {
            "callsign": _t("callsign") or callsign.upper(),
            "name":     name,
            "qth":      ", ".join(p for p in qth_parts if p),
            "grid":     _t("grid"),
            "country":  _t("country"),
            "cqzone":   _t("cq"),
            "ituzone":  _t("itu"),
            "source":   "hamqth",
        }
        if not data["name"] and not data["qth"]:
            return None, "Callsign not found on HamQTH", False
        return data, None, False
    except Exception as e:
        return None, str(e), False


def hamqth_lookup(callsign):
    """
    Public function: look up a callsign on HamQTH XML service.
    Handles session management and automatic re-login on expiry.
    Returns (data_dict, error_string).
    """
    global _hamqth_session_key

    if not HAMQTH_USER:
        return None, "HamQTH username not configured"
    if not HAMQTH_PASS:
        return None, "HamQTH password not configured"

    with _hamqth_session_lock:
        if not _hamqth_session_key:
            key, err = _hamqth_login(HAMQTH_USER, HAMQTH_PASS)
            if err:
                return None, f"HamQTH login failed: {err}"
            _hamqth_session_key = key
            print(f"HamQTH: logged in as {HAMQTH_USER}")

        data, err, expired = _hamqth_lookup_with_key(_hamqth_session_key, callsign)

        if expired:
            print("HamQTH: session expired, re-logging in")
            _hamqth_session_key = None
            key, login_err = _hamqth_login(HAMQTH_USER, HAMQTH_PASS)
            if login_err:
                return None, f"HamQTH re-login failed: {login_err}"
            _hamqth_session_key = key
            data, err, _ = _hamqth_lookup_with_key(_hamqth_session_key, callsign)

        return data, err


@app.route("/api/hamqth_lookup/<callsign>")
def hamqth_lookup_route(callsign):
    data, err = hamqth_lookup(callsign.upper())
    if err:
        return jsonify({"error": err}), 400 if "not configured" in err else 404
    return jsonify(data)


@app.route("/api/hamqth_test_lookup")
def hamqth_test_lookup():
    """Test HamQTH login with provided credentials, then look up W1AW."""
    global _hamqth_session_key
    user = request.args.get("user", HAMQTH_USER)
    pwd  = request.args.get("pass", HAMQTH_PASS)
    if not user:
        return jsonify({"ok": False, "error": "Enter your HamQTH username"})
    if not pwd:
        return jsonify({"ok": False, "error": "Enter your HamQTH password"})
    with _hamqth_session_lock:
        sid, err = _hamqth_login(user, pwd)
        if err:
            return jsonify({"ok": False, "error": f"Login failed: {err}"})
        _hamqth_session_key = sid
    data, err, _ = _hamqth_lookup_with_key(sid, "W1AW")
    if err:
        return jsonify({"ok": False, "error": err})
    return jsonify({"ok": True, "callsign": data["callsign"], "name": data["name"], "qth": data["qth"]})


@app.route("/api/clublog_test")
def clublog_test():
    """Test Club Log credentials using getlotwstate.php — a safe read-only
    endpoint that verifies api + email + password + callsign and returns
    HTTP 200 on valid credentials.

    NEVER use realtime.php for testing — repeated failed POSTs trigger
    Club Log's reactive IP firewall.

    getlotwstate.php is a lightweight GET that returns LoTW sync state.
    We only care about 200 (valid) vs 403 (invalid).

    Ref: https://clublog.freshdesk.com/support/solutions/articles/3000064882
    """
    global _clublog_blocked
    if not CLUBLOG_APP_KEY:
        return jsonify({"ok": False, "error": "Club Log application key not available in this build"})
    if not CLUBLOG_EMAIL:
        return jsonify({"ok": False, "error": "Save your Club Log email in Settings first"})
    if not CLUBLOG_PASSWORD:
        return jsonify({"ok": False, "error": "Save your Club Log password in Settings first"})
    callsign = (CLUBLOG_CALLSIGN or MY_CALLSIGN).strip().upper()
    if not callsign:
        return jsonify({"ok": False, "error": "Save your callsign in Settings first"})
    try:
        _log(f"[clublog_test] verifying via getlotwstate.php — "
             f"email='{CLUBLOG_EMAIL}' | password len={len(CLUBLOG_PASSWORD)} | "
             f"callsign='{callsign}' | api key len={len(CLUBLOG_APP_KEY.strip())}")
        resp = requests.get(
            "https://clublog.org/getlotwstate.php",
            params={
                "api":      CLUBLOG_APP_KEY.strip(),
                "email":    CLUBLOG_EMAIL,
                "password": CLUBLOG_PASSWORD,
                "callsign": callsign,
            },
            headers={"User-Agent": f"SDRLoggerPlus/{VERSION}"},
            timeout=8
        )
        body = resp.text.strip()[:400]
        is_nginx = "nginx" in body.lower() and "<html" in body.lower()
        _log(f"[clublog_test] HTTP {resp.status_code} — {body[:200]}")
        if resp.status_code == 200:
            _clublog_blocked = False       # credentials good — clear any prior block
            return jsonify({"ok": True, "note": "Credentials verified — Club Log is ready"})
        if resp.status_code == 403:
            if is_nginx:
                return jsonify({"ok": False, "error": "Your IP is temporarily blocked by Club Log's firewall (nginx 403). "
                                "This is from earlier failed attempts — wait approximately 1 hour, then test again."})
            return jsonify({"ok": False, "error": "Authentication failed (403) — check your Club Log email, App Password, and callsign"})
        return jsonify({"ok": False, "error": f"Unexpected response (HTTP {resp.status_code}) — {body[:200]}"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/lotw_test")
def lotw_test():
    """Check that the configured TQSL path exists and is executable."""
    path = request.args.get("path", LOTW_TQSL_PATH).strip()
    if not path:
        return jsonify({"ok": False, "error": "No TQSL path configured"})
    if not os.path.isfile(path):
        return jsonify({"ok": False, "error": f"File not found: {path}"})
    try:
        r = subprocess.run([path, "--version"], capture_output=True, text=True, timeout=8)
        ver = (r.stdout or r.stderr or "").strip().split("\n")[0][:80]
        return jsonify({"ok": True, "version": ver or "TQSL found"})
    except subprocess.TimeoutExpired:
        return jsonify({"ok": False, "error": "TQSL did not respond in time"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/callsign_lookup/<path:callsign>")
def callsign_lookup_route(callsign):
    """
    Unified callsign lookup: tries QRZ first, falls back to HamQTH.
    Returns the first successful result with a 'source' field ('qrz' or 'hamqth').
    """
    cs = callsign.upper()
    # Try QRZ if configured
    if QRZ_USER and QRZ_PASS and QRZ_LOOKUP_ENABLED:
        data, err = qrz_lookup(cs)
        if data:
            data["source"] = "qrz"
            return jsonify(data)
    # Try HamQTH if configured
    if HAMQTH_USER and HAMQTH_PASS:
        data, err = hamqth_lookup(cs)
        if data:
            return jsonify(data)  # source='hamqth' already set
        return jsonify({"error": err or "Not found"}), 404
    return jsonify({"error": "No callsign lookup service configured — add QRZ or HamQTH credentials in Settings"}), 503


@app.route("/api/settings", methods=["POST"])
def update_settings():
    global QRZ_USER, QRZ_PASS, QRZ_LOGBOOK_KEY, QRZ_LOGBOOK_UPLOAD_ENABLED, TELNET_ENABLED, TELNET_SERVER, TELNET_PORT, MY_CALLSIGN, MY_NAME, TCI_ENABLED, TCI_HOST, TCI_PORT, ITU_REGION, _qrz_session_key
    global HAMQTH_USER, HAMQTH_PASS, _hamqth_session_key
    global LOTW_TQSL_PATH, LOTW_STATION_LOCATION, LOTW_UPLOAD_ENABLED
    global CLUBLOG_EMAIL, CLUBLOG_PASSWORD, CLUBLOG_CALLSIGN, CLUBLOG_UPLOAD_ENABLED, CLUBLOG_UPLOAD_DESIGNATOR, _clublog_blocked
    global DIGITAL_UDP_ENABLED, DIGITAL_UDP_PORT, DIGITAL_TCP_ENABLED, DIGITAL_TCP_PORT
    global ROTATOR_ENABLED, ROTATOR_HOST, ROTATOR_PORT, ROTATOR_PROTOCOL, ROTATOR_AUTO, ROTATOR_DEBUG
    global BACKUP_PATH
    global AUTO_BACKUP_ENABLED, AUTO_BACKUP_INTERVAL, AUTO_BACKUP_RETENTION
    global FLRIG_ENABLED, FLRIG_HOST, FLRIG_PORT, FLRIG_DIGITAL_MODE, FLRIG_RTTY_MODE
    global HAMLIB_ENABLED, HAMLIB_HOST, HAMLIB_PORT
    global WINKEYER_ENABLED, WINKEYER_PORT, WINKEYER_WPM, WINKEYER_KEY_OUT, WINKEYER_MODE, WINKEYER_PTT, WINKEYER_PTT_LEAD, WINKEYER_PTT_TAIL
    global EQSL_USER, EQSL_PASS, EQSL_UPLOAD_ENABLED
    global POTA_MY_PARK, POTA_USER, POTA_PASS
    global SAT_UDP_ENABLED, SAT_UDP_PORT, SAT_ADIF_PORT, SAT_CONTROLLER_IP
    global LIGHTNING_ACCEPTED, LIGHTNING_ENABLED, LIGHTNING_RANGE, LIGHTNING_UNIT, LIGHTNING_BLITZORTUNG, LIGHTNING_NOAA
    global LIGHTNING_AMBIENT, LIGHTNING_AMBIENT_API_KEY, LIGHTNING_AMBIENT_APP_KEY
    global WIND_ENABLED, WIND_NWS_ALERTS, WIND_NWS_METAR, WIND_AMBIENT
    global WIND_THRESH_SUST, WIND_THRESH_GUST, WIND_COOLDOWN_MIN, WIND_METAR_STATION
    data = request.json or {}
    runtime_settings.update(data)
    if data.get("qrz_user"):
        QRZ_USER = data["qrz_user"]
        _qrz_session_key = None  # force re-login
    if data.get("qrz_pass"):
        QRZ_PASS = data["qrz_pass"]
        _qrz_session_key = None  # force re-login
    if data.get("qrz_logbook_key"): QRZ_LOGBOOK_KEY  = data["qrz_logbook_key"]
    if data.get("hamqth_user"):
        HAMQTH_USER = data["hamqth_user"]
        _hamqth_session_key = None  # force re-login
    if data.get("hamqth_pass"):
        HAMQTH_PASS = data["hamqth_pass"]
        _hamqth_session_key = None  # force re-login
    if "qrz_logbook_upload_enabled" in data: QRZ_LOGBOOK_UPLOAD_ENABLED = bool(data["qrz_logbook_upload_enabled"])
    if "lotw_tqsl_path"      in data: LOTW_TQSL_PATH         = data["lotw_tqsl_path"].strip()
    if "lotw_station_location" in data: LOTW_STATION_LOCATION = data["lotw_station_location"].strip()
    if "lotw_upload_enabled" in data: LOTW_UPLOAD_ENABLED    = bool(data["lotw_upload_enabled"])
    if "clublog_email"    in data:
        CLUBLOG_EMAIL    = data["clublog_email"].strip()
        _clublog_blocked = False   # credential change — clear block
    if "clublog_password" in data:
        CLUBLOG_PASSWORD = data["clublog_password"]
        _clublog_blocked = False
    if "clublog_callsign" in data:            CLUBLOG_CALLSIGN           = data["clublog_callsign"].strip().upper()
    if "clublog_upload_enabled" in data:      CLUBLOG_UPLOAD_ENABLED     = bool(data["clublog_upload_enabled"])
    if data.get("clublog_upload_designator"): CLUBLOG_UPLOAD_DESIGNATOR  = data["clublog_upload_designator"][:1].upper()
    if "telnet_enabled" in data:    TELNET_ENABLED   = bool(data["telnet_enabled"]) if not isinstance(data["telnet_enabled"], str) else data["telnet_enabled"].lower() == "true"
    if data.get("telnet_server"):   TELNET_SERVER    = data["telnet_server"]
    if data.get("telnet_port"):     TELNET_PORT      = int(data["telnet_port"])
    if data.get("callsign"):        MY_CALLSIGN      = data["callsign"]
    if "opname" in data:            MY_NAME          = data["opname"]
    tci_changed = False
    if data.get("tci_host") and data["tci_host"] != TCI_HOST:
        TCI_HOST = data["tci_host"]
        tci_changed = True
    if data.get("tci_port") and int(data["tci_port"]) != TCI_PORT:
        TCI_PORT = int(data["tci_port"])
        tci_changed = True
    if tci_changed:
        # Drop the active socket so tci_ws_client reconnects with new host/port
        with tci_send_lock:
            if tci_active_sock:
                try: tci_active_sock.close()
                except Exception: pass
    if "tci_enabled" in data:
        TCI_ENABLED = bool(data["tci_enabled"])
        if not TCI_ENABLED:
            with tci_send_lock:
                if tci_active_sock:
                    try: tci_active_sock.close()
                    except Exception: pass
    if "itu_region" in data:
        ITU_REGION = int(data["itu_region"])
    # Digital app integration settings
    if "digital_udp_enabled" in data: DIGITAL_UDP_ENABLED = bool(data["digital_udp_enabled"])
    if data.get("digital_udp_port"):  DIGITAL_UDP_PORT    = int(data["digital_udp_port"])
    if "digital_tcp_enabled" in data: DIGITAL_TCP_ENABLED = bool(data["digital_tcp_enabled"])
    if data.get("digital_tcp_port"):  DIGITAL_TCP_PORT    = int(data["digital_tcp_port"])
    # Rotator settings
    if "rotator_enabled"  in data: ROTATOR_ENABLED  = bool(data["rotator_enabled"])
    if data.get("rotator_host"):    ROTATOR_HOST     = data["rotator_host"]
    if data.get("rotator_port"):    ROTATOR_PORT     = int(data["rotator_port"])
    if data.get("rotator_protocol"):ROTATOR_PROTOCOL = data["rotator_protocol"]
    if "rotator_auto"     in data: ROTATOR_AUTO     = bool(data["rotator_auto"])
    if "rotator_debug"    in data: ROTATOR_DEBUG    = bool(data["rotator_debug"])
    if "backup_path"      in data: BACKUP_PATH      = data["backup_path"].strip()
    if "auto_backup_enabled"   in data: AUTO_BACKUP_ENABLED   = bool(data["auto_backup_enabled"])
    if "auto_backup_interval"  in data:
        _v = str(data["auto_backup_interval"]).lower().strip()
        if _v in ("daily", "weekly", "on_exit"): AUTO_BACKUP_INTERVAL = _v
    if "auto_backup_retention" in data:
        try:
            _n = int(data["auto_backup_retention"])
            if _n >= 1: AUTO_BACKUP_RETENTION = _n
        except Exception: pass
    # flrig settings
    if "flrig_enabled"      in data: FLRIG_ENABLED      = bool(data["flrig_enabled"])
    if data.get("flrig_host"):       FLRIG_HOST          = data["flrig_host"].strip()
    if data.get("flrig_port"):       FLRIG_PORT          = int(data["flrig_port"])
    if "flrig_digital_mode" in data: FLRIG_DIGITAL_MODE  = data["flrig_digital_mode"].strip()
    if "flrig_rtty_mode"    in data: FLRIG_RTTY_MODE     = data["flrig_rtty_mode"].strip()
    # HamLib settings
    if "hamlib_enabled"   in data: HAMLIB_ENABLED   = bool(data["hamlib_enabled"])
    if data.get("hamlib_host"):    HAMLIB_HOST       = data["hamlib_host"].strip()
    if data.get("hamlib_port"):    HAMLIB_PORT       = int(data["hamlib_port"])
    # WinKeyer settings
    if "winkeyer_enabled"  in data: WINKEYER_ENABLED  = bool(data["winkeyer_enabled"])
    if data.get("winkeyer_port"):   WINKEYER_PORT     = data["winkeyer_port"].strip()
    if data.get("winkeyer_wpm"):    WINKEYER_WPM      = int(data["winkeyer_wpm"])
    if data.get("winkeyer_key_out"):WINKEYER_KEY_OUT  = data["winkeyer_key_out"]
    if data.get("winkeyer_mode"):   WINKEYER_MODE     = data["winkeyer_mode"]
    if "winkeyer_ptt"      in data: WINKEYER_PTT      = bool(data["winkeyer_ptt"])
    if "winkeyer_ptt_lead" in data: WINKEYER_PTT_LEAD = int(data["winkeyer_ptt_lead"])
    if "winkeyer_ptt_tail" in data: WINKEYER_PTT_TAIL = int(data["winkeyer_ptt_tail"])
    # eQSL settings
    if data.get("eqsl_user"):          EQSL_USER           = data["eqsl_user"].strip()
    if "eqsl_pass"         in data:    EQSL_PASS           = data["eqsl_pass"]
    if "eqsl_upload_enabled" in data:  EQSL_UPLOAD_ENABLED = bool(data["eqsl_upload_enabled"])
    # POTA settings
    if data.get("pota_my_park"):   POTA_MY_PARK      = data["pota_my_park"].strip().upper()
    if "pota_user"        in data: POTA_USER         = data["pota_user"].strip()
    if "pota_pass"        in data: POTA_PASS         = data["pota_pass"].strip()
    # SAT settings
    if "sat_udp_enabled"    in data: SAT_UDP_ENABLED    = bool(data["sat_udp_enabled"])
    if data.get("sat_udp_port"):     SAT_UDP_PORT       = int(data["sat_udp_port"])
    if data.get("sat_adif_port"):    SAT_ADIF_PORT      = int(data["sat_adif_port"])
    if "sat_controller_ip"  in data: SAT_CONTROLLER_IP  = data["sat_controller_ip"].strip()
    # Lightning settings
    if "lightning_accepted"        in data: LIGHTNING_ACCEPTED        = bool(data["lightning_accepted"])
    if "lightning_enabled"         in data: LIGHTNING_ENABLED         = bool(data["lightning_enabled"]) and LIGHTNING_ACCEPTED
    if "lightning_range"           in data: LIGHTNING_RANGE           = int(data["lightning_range"])
    if "lightning_unit"            in data: LIGHTNING_UNIT            = data["lightning_unit"]
    if "lightning_blitzortung"     in data: LIGHTNING_BLITZORTUNG     = bool(data["lightning_blitzortung"])
    if "lightning_noaa"            in data: LIGHTNING_NOAA            = bool(data["lightning_noaa"])
    if "lightning_ambient"         in data: LIGHTNING_AMBIENT         = bool(data["lightning_ambient"])
    if data.get("lightning_ambient_api_key"): LIGHTNING_AMBIENT_API_KEY = data["lightning_ambient_api_key"].strip()
    if data.get("lightning_ambient_app_key"): LIGHTNING_AMBIENT_APP_KEY = data["lightning_ambient_app_key"].strip()
    # Wind alerts (v1.08.2-beta)
    if "wind_enabled"         in data: WIND_ENABLED        = bool(data["wind_enabled"]) and LIGHTNING_ACCEPTED
    if "wind_nws_alerts"      in data: WIND_NWS_ALERTS     = bool(data["wind_nws_alerts"])
    if "wind_nws_metar"       in data: WIND_NWS_METAR      = bool(data["wind_nws_metar"])
    if "wind_ambient"         in data: WIND_AMBIENT        = bool(data["wind_ambient"])
    if "wind_thresh_sust"     in data: WIND_THRESH_SUST    = int(data["wind_thresh_sust"])
    if "wind_thresh_gust"     in data: WIND_THRESH_GUST    = int(data["wind_thresh_gust"])
    if "wind_cooldown_min"    in data: WIND_COOLDOWN_MIN   = int(data["wind_cooldown_min"])
    if data.get("wind_metar_station"): WIND_METAR_STATION  = data["wind_metar_station"].strip().upper()
    # Immediately clear lightning/wind status so banners hide when sources change
    if any(k in data for k in ("lightning_enabled", "lightning_blitzortung", "lightning_noaa", "lightning_ambient")):
        with _lightning_lock:
            _lightning_status["active"] = False
            _lightning_status["closest_km"] = None
            _lightning_status["closest_mi"] = None
            _lightning_status["direction"] = ""
            _lightning_status["strikes_1hr"] = 0
            _lightning_status["sources"] = []
            _lightning_status["noaa_warning"] = ""
    if any(k in data for k in ("wind_enabled", "wind_nws_alerts", "wind_nws_metar", "wind_ambient")):
        with _wind_lock:
            _wind_status["active"] = False
            _wind_status["severity"] = ""
            _wind_status["sustained_mph"] = None
            _wind_status["gust_mph"] = None
            _wind_status["direction"] = ""
            _wind_status["sources"] = []
            _wind_status["nws_alert"] = ""
    _save_app_settings()
    return jsonify({"ok": True})


@app.route("/api/settings", methods=["GET"])
def get_settings():
    # Merge runtime_settings with hard-coded defaults for fields that may not
    # exist in older saved settings files (new fields added in later versions).
    defaults = {
        "clublog_upload_designator":  CLUBLOG_UPLOAD_DESIGNATOR,
    }
    return jsonify({**defaults, **runtime_settings})


# ─── File / Directory Browse Dialogs ─────────────────────────────────────────
@app.route("/api/browse_file")
def browse_file():
    """Open a native OS file picker dialog and return the selected path."""
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        # Parse optional filetypes param: "ADIF Files:*.adi|Executables:*.exe"
        ft_param = request.args.get('filetypes', '')
        ft = []
        if ft_param:
            for pair in ft_param.split('|'):
                parts = pair.split(':')
                if len(parts) == 2:
                    ft.append((parts[0].strip(), parts[1].strip()))
        ft.append(("All Files", "*.*"))
        path = filedialog.askopenfilename(filetypes=ft)
        root.destroy()
        return jsonify({"path": path or ""})
    except Exception as e:
        return jsonify({"path": "", "error": str(e)})

@app.route("/api/browse_dir")
def browse_dir():
    """Open a native OS directory picker dialog and return the selected path."""
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        path = filedialog.askdirectory()
        root.destroy()
        return jsonify({"path": path or ""})
    except Exception as e:
        return jsonify({"path": "", "error": str(e)})


@app.route("/api/backup_db", methods=["GET", "POST"])
def backup_db():
    """
    GET  — sends hamlog.db as a browser download.
    POST — saves a timestamped copy to the configured BACKUP_PATH (or downloads if none set).
    Body (optional): {"backup_path": "C:\\Radio\\Backups"}
    """
    db_src  = os.path.abspath(DATABASE)
    ts      = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"hamlog_backup_{ts}.db"

    if request.method == "POST":
        body = request.json or {}
        path = (body.get("backup_path") or BACKUP_PATH or "").strip()
        if path:
            try:
                os.makedirs(path, exist_ok=True)
                dest = os.path.join(path, filename)
                shutil.copy2(db_src, dest)
                return jsonify({"ok": True, "path": dest, "filename": filename})
            except Exception as e:
                return jsonify({"ok": False, "error": str(e)}), 500

    # GET, or POST with no path — browser download
    return send_file(db_src, as_attachment=True,
                     download_name=filename,
                     mimetype="application/octet-stream")


# ─── Auto-Backup Runner (v1.09) ───────────────────────────────────────────────
def _adif_tag(name, val):
    val = str(val) if val else ""
    return f"<{name}:{len(val)}>{val} " if val else ""


def _export_db_to_adif(db_path, label):
    """Read a database file and render its qso_log rows as ADIF text.

    label is either "general" or "pota" — used only for the ADIF header
    comment. Returns the ADIF text as a string (empty on error)."""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM qso_log ORDER BY date_worked, time_worked").fetchall()
        conn.close()
    except Exception:
        return ""

    is_pota = (label == "pota")
    prog_id = "SDRLogger+"
    lines = [f"SDRLogger+ ADIF Export — {'POTA Activation' if is_pota else 'General Log'}",
             f"<PROGRAMID:{len(prog_id)}>{prog_id}\n<EOH>\n"]
    for r in rows:
        pota_ref = ((r["pota_ref"] if "pota_ref" in r.keys() else "") or "").strip() if is_pota else ""
        pota_p2p = ((r["pota_p2p"] if "pota_p2p" in r.keys() else "") or "").strip() if is_pota else ""
        keys = set(r.keys())
        def _col(name):
            return r[name] if name in keys else ""
        line = (
            _adif_tag("CALL", r["callsign"]) +
            _adif_tag("QSO_DATE", (r["date_worked"] or "").replace("-", "")) +
            _adif_tag("TIME_ON", (r["time_worked"] or "").replace(":", "")[:6]) +
            _adif_tag("BAND", r["band"]) +
            _adif_tag("MODE", r["mode"]) +
            _adif_tag("FREQ", r["freq_mhz"]) +
            _adif_tag("RST_SENT", r["my_rst_sent"]) +
            _adif_tag("RST_RCVD", r["their_rst_rcvd"]) +
            _adif_tag("COMMENT", r["remarks"]) +
            _adif_tag("CONTEST_ID", r["contest_name"]) +
            (_adif_tag("MY_SIG", "POTA") + _adif_tag("MY_SIG_INFO", pota_ref) if pota_ref else "") +
            (_adif_tag("SIG", "POTA") + _adif_tag("SIG_INFO", pota_p2p) if pota_p2p else "") +
            _adif_tag("NAME", r["name"]) +
            _adif_tag("QTH", r["qth"]) +
            _adif_tag("STATE", _col("state")) +
            _adif_tag("COUNTRY", _col("country")) +
            _adif_tag("GRIDSQUARE", _col("gridsquare")) +
            (_adif_tag("PROP_MODE", _col("prop_mode")) if _col("prop_mode") else "") +
            (_adif_tag("SAT_NAME", _col("sat_name")) if _col("sat_name") else "") +
            (_adif_tag("FREQ_RX", _col("uplink_freq")) if _col("uplink_freq") else "") +
            "<EOR>"
        )
        lines.append(line)
    return "\n".join(lines)


def _auto_backup_default_dir():
    """Fallback auto-backup destination when user hasn't configured one."""
    try:
        appdata = os.environ.get("APPDATA") or os.path.expanduser("~")
        return os.path.join(appdata, "SDRLoggerPlus", "backups")
    except Exception:
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), "backups")


def _run_auto_backup(trigger="scheduled"):
    """Run one scheduled auto-backup pass.

    Bundles all databases (General + POTA) as BOTH raw .db files AND ADIF
    exports into a single timestamped subfolder. Retention prune runs ONLY
    after a successful write, so a failed run never destroys prior backups.

    trigger is a free-form label ("scheduled", "manual", "on_exit") recorded
    in the status dict for UI display.
    """
    global _auto_backup_status
    with _auto_backup_lock:
        dest_root = (BACKUP_PATH or "").strip() or _auto_backup_default_dir()
        ts = datetime.utcnow().strftime("%Y-%m-%d_%H%M")
        folder = os.path.join(dest_root, f"SDRLoggerPlus-{ts}")
        try:
            os.makedirs(folder, exist_ok=True)
        except Exception as e:
            msg = f"Cannot create backup folder: {e}"
            _auto_backup_status.update({
                "last_run": datetime.utcnow().isoformat() + "Z",
                "ok": False, "message": msg, "path": folder
            })
            _save_auto_backup_state()
            try: _rot_dbg(f"[auto-backup] {trigger} FAILED: {msg}")
            except Exception: pass
            return False, msg

        written = []
        failures = []

        # General DB + ADIF
        try:
            gen_src = os.path.abspath(DATABASE)
            if os.path.exists(gen_src):
                shutil.copy2(gen_src, os.path.join(folder, "general.db"))
                written.append("general.db")
                adif = _export_db_to_adif(gen_src, "general")
                if adif:
                    with open(os.path.join(folder, "general.adi"), "w", encoding="utf-8") as _af:
                        _af.write(adif)
                    written.append("general.adi")
        except Exception as e:
            failures.append(f"general: {e}")

        # POTA DB + ADIF
        try:
            pota_src = os.path.abspath(POTA_DATABASE)
            if os.path.exists(pota_src):
                shutil.copy2(pota_src, os.path.join(folder, "pota.db"))
                written.append("pota.db")
                adif = _export_db_to_adif(pota_src, "pota")
                if adif:
                    with open(os.path.join(folder, "pota.adi"), "w", encoding="utf-8") as _af:
                        _af.write(adif)
                    written.append("pota.adi")
        except Exception as e:
            failures.append(f"pota: {e}")

        if not written:
            msg = "No databases written. " + (" ; ".join(failures) if failures else "")
            _auto_backup_status.update({
                "last_run": datetime.utcnow().isoformat() + "Z",
                "ok": False, "message": msg, "path": folder
            })
            _save_auto_backup_state()
            try: _rot_dbg(f"[auto-backup] {trigger} FAILED: {msg}")
            except Exception: pass
            return False, msg

        # Retention prune — ONLY runs after a successful write above.
        pruned = 0
        try:
            if AUTO_BACKUP_RETENTION >= 1 and os.path.isdir(dest_root):
                sibs = []
                for name in os.listdir(dest_root):
                    if not name.startswith("SDRLoggerPlus-"):
                        continue
                    full = os.path.join(dest_root, name)
                    if os.path.isdir(full):
                        sibs.append((os.path.getmtime(full), full))
                sibs.sort(reverse=True)  # newest first
                for _mt, path in sibs[AUTO_BACKUP_RETENTION:]:
                    try:
                        shutil.rmtree(path)
                        pruned += 1
                    except Exception:
                        pass
        except Exception:
            pass

        ok_msg = f"Wrote {len(written)} file(s): {', '.join(written)}"
        if failures:
            ok_msg += f"  (partial — {' ; '.join(failures)})"
        if pruned:
            ok_msg += f"  [pruned {pruned} old folder(s)]"

        _auto_backup_status.update({
            "last_run": datetime.utcnow().isoformat() + "Z",
            "ok": True, "message": ok_msg, "path": folder
        })
        _save_auto_backup_state()
        try: _rot_dbg(f"[auto-backup] {trigger} OK → {folder}")
        except Exception: pass
        return True, ok_msg


def _auto_backup_interval_td():
    """Return the configured interval as a timedelta (None for on_exit)."""
    if AUTO_BACKUP_INTERVAL == "on_exit":
        return None
    if AUTO_BACKUP_INTERVAL == "weekly":
        return timedelta(days=7)
    return timedelta(days=1)  # daily default


def _auto_backup_parse_last_run():
    """Return last_run as a naive UTC datetime, or None if never run / unparseable."""
    raw = _auto_backup_status.get("last_run")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).rstrip("Z"))
    except Exception:
        return None


def _auto_backup_compute_next_due(from_dt=None):
    """Return ISO-8601 UTC timestamp of next scheduled run.

    Schedule is anchored to the last successful run (persisted across
    restarts), NOT to app boot time — so a Daily backup that already fired
    today won't fire again just because the user reopened the app.

    If no prior run exists, next_due is "now" (i.e. fire on next tick).
    """
    td = _auto_backup_interval_td()
    if td is None:
        return None
    # Explicit from_dt anchors next_due to that moment (used right after a run)
    if from_dt is not None:
        return (from_dt + td).isoformat() + "Z"
    last = _auto_backup_parse_last_run()
    if last is None:
        return datetime.utcnow().isoformat() + "Z"
    return (last + td).isoformat() + "Z"


def _auto_backup_daemon():
    """Wake once a minute; fire when (now - last_run) >= interval.

    On first-enable with no prior run history, fires on the next tick.
    """
    global _auto_backup_status
    # Seed next_due on boot so the UI has a meaningful display
    _auto_backup_status["next_due"] = _auto_backup_compute_next_due()
    while True:
        try:
            time.sleep(60)
            if not AUTO_BACKUP_ENABLED:
                continue
            td = _auto_backup_interval_td()
            if td is None:
                continue  # on_exit — fires only via /api/explicit_close
            now = datetime.utcnow()
            last = _auto_backup_parse_last_run()
            if last is None or (now - last) >= td:
                _run_auto_backup(trigger="scheduled")
            # Always refresh next_due so the UI countdown stays accurate
            _auto_backup_status["next_due"] = _auto_backup_compute_next_due()
        except Exception:
            pass


threading.Thread(target=_auto_backup_daemon, daemon=True).start()


@app.route("/api/auto_backup/status", methods=["GET"])
def auto_backup_status():
    return jsonify({
        "enabled":   AUTO_BACKUP_ENABLED,
        "interval":  AUTO_BACKUP_INTERVAL,
        "retention": AUTO_BACKUP_RETENTION,
        "dest":      (BACKUP_PATH or "").strip() or _auto_backup_default_dir(),
        "last_run":  _auto_backup_status.get("last_run"),
        "ok":        _auto_backup_status.get("ok"),
        "message":   _auto_backup_status.get("message"),
        "path":      _auto_backup_status.get("path"),
        "next_due":  _auto_backup_status.get("next_due"),
    })


@app.route("/api/auto_backup/run_now", methods=["POST"])
def auto_backup_run_now():
    ok, msg = _run_auto_backup(trigger="manual")
    st = _auto_backup_status
    return jsonify({
        "ok": ok, "message": msg,
        "path": st.get("path"),
        "last_run": st.get("last_run"),
        "next_due": st.get("next_due"),
    })


@app.route("/api/restore_db", methods=["POST"])
def restore_db():
    """
    Upload a previously backed-up .db file to replace the active database.
    The current database is automatically saved as a timestamped safety copy
    in BACKUP_PATH (or alongside the active db) before being overwritten.
    Validates the uploaded file is a SQLite3 database before touching anything.
    """
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "No file uploaded"}), 400

    upload   = request.files["file"]
    active_db = POTA_DATABASE if ACTIVE_MODE == "pota" else DATABASE
    db_src   = os.path.abspath(active_db)
    ts       = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    data     = upload.read()

    # Validate: SQLite3 files start with "SQLite format 3\000"
    if not data[:16] == b"SQLite format 3\x00":
        return jsonify({"ok": False, "error": "File does not appear to be a valid SQLite database"}), 400

    # Auto-save a safety copy of the current database before overwriting
    try:
        safety_dir = BACKUP_PATH.strip() if BACKUP_PATH.strip() else os.path.dirname(db_src)
        os.makedirs(safety_dir, exist_ok=True)
        db_label    = "pota" if ACTIVE_MODE == "pota" else "hamlog"
        safety_path = os.path.join(safety_dir, f"{db_label}_pre_restore_{ts}.db")
        shutil.copy2(db_src, safety_path)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Could not save safety backup: {e}"}), 500

    # Write the uploaded database
    try:
        with open(db_src, "wb") as f:
            f.write(data)
        return jsonify({"ok": True, "safety_backup": safety_path})
    except Exception as e:
        return jsonify({"ok": False, "error": f"Could not write database: {e}"}), 500


@app.route("/api/export_settings", methods=["POST"])
def export_settings_file():
    """Save the current settings JSON to the backup folder (or download if no folder set)."""
    body = request.json or {}
    path = (body.get("backup_path") or BACKUP_PATH or "").strip()
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"hamlog_settings_{ts}.json"

    if path:
        try:
            os.makedirs(path, exist_ok=True)
            dest = os.path.join(path, filename)
            with open(dest, "w") as f:
                json.dump(runtime_settings, f, indent=2)
            return jsonify({"ok": True, "path": dest, "filename": filename})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    # No path — return as download
    buf = io.BytesIO(json.dumps(runtime_settings, indent=2).encode("utf-8"))
    buf.seek(0)
    return send_file(buf, mimetype="application/json",
                     as_attachment=True, download_name=filename)


@app.route("/api/save_server_config", methods=["POST"])
def save_server_config():
    """Save web port and bind address to config.json. Requires restart to take effect."""
    data = request.json or {}
    cfg  = _load_server_cfg()

    if "web_port" in data:
        try:
            port = int(data["web_port"])
            if not (1024 <= port <= 65535):
                return jsonify({"ok": False, "error": "Port must be 1024–65535"}), 400
            cfg["web_port"] = port
        except ValueError:
            return jsonify({"ok": False, "error": "Invalid port number"}), 400

    if "web_allow_network" in data:
        cfg["web_host"] = "0.0.0.0" if data["web_allow_network"] else "127.0.0.1"

    _save_server_cfg(cfg)
    return jsonify({"ok": True, "restart_required": True,
                    "web_port": cfg["web_port"], "web_host": cfg["web_host"]})


@app.route("/api/server_config")
def get_server_config():
    """Return current server config so the Settings UI can display it."""
    cfg = _load_server_cfg()
    return jsonify({
        "web_port":         cfg.get("web_port", 5000),
        "web_allow_network": cfg.get("web_host", "0.0.0.0") == "0.0.0.0",
    })


@app.route("/api/hamlib_data")
def hamlib_data():
    """Browser polls this to get latest frequency/mode from rigctld."""
    return jsonify({
        "connected": hamlib_connected,
        "enabled":   HAMLIB_ENABLED,
        "freq_mhz":  latest_hamlib.get("freq_mhz", ""),
        "mode":      latest_hamlib.get("mode", ""),
    })


@app.route("/api/hamlib_tune", methods=["POST"])
def hamlib_tune():
    """Tune rig to given frequency and/or mode via rigctld."""
    data     = request.json or {}
    freq_mhz = data.get("freq_mhz")
    mode     = data.get("mode", "")
    if freq_mhz is None and not mode:
        return jsonify({"ok": False, "error": "freq_mhz or mode required"}), 400
    ok = hamlib_set_freq_mode(freq_mhz, mode)
    return jsonify({"ok": ok, "connected": hamlib_connected})


@app.route("/api/hamlib_test", methods=["POST"])
def hamlib_test():
    """Test rigctld connection — returns rig info string."""
    resp = _hamlib_cmd("\\dump_state")
    if resp is None:
        return jsonify({"ok": False, "error": f"Cannot connect to rigctld at {HAMLIB_HOST}:{HAMLIB_PORT}"})
    # First line of dump_state is protocol version — just confirm connected
    return jsonify({"ok": True, "info": f"rigctld connected at {HAMLIB_HOST}:{HAMLIB_PORT}"})


@app.route("/api/serial_ports")
def serial_ports():
    """Return a list of available COM ports on this machine."""
    try:
        from serial.tools.list_ports import comports
        ports = [{"port": p.device, "desc": p.description} for p in sorted(comports())]
    except Exception:
        ports = []
    return jsonify({"ports": ports})


@app.route("/api/winkeyer_test", methods=["POST"])
def winkeyer_test():
    """Test WinKeyer connection — open, read version, close if not already open."""
    global WINKEYER_ENABLED
    try:
        data = request.get_json(silent=True) or {}
    except Exception:
        data = {}
    port = (data.get("port") or WINKEYER_PORT or "").strip()
    if not port:
        return jsonify({"ok": False, "error": "No COM port specified"})
    # If already connected on this port, just report the version
    if _wk_is_open and WINKEYER_PORT == port:
        return jsonify({"ok": True, "version": _wk_version, "info": f"WinKeyer connected on {port} — firmware version {_wk_version}"})
    # Otherwise do a fresh connect test
    import serial as _ser
    try:
        ser = _ser.Serial(port=port, baudrate=1200, bytesize=_ser.EIGHTBITS,
                          parity=_ser.PARITY_NONE, stopbits=_ser.STOPBITS_TWO,
                          timeout=1.0, write_timeout=3.0, dsrdtr=False)
        ser.dtr = True
        import time
        time.sleep(0.1)
        ser.write(b'\x00\x03')  # close any stale session
        ser.flush()
        time.sleep(0.3)
        ser.reset_input_buffer()
        ser.write(b'\x13\x13\x13\x13')  # parser sync
        ser.flush()
        time.sleep(0.1)
        ser.reset_input_buffer()
        ser.write(b'\x00\x02')  # Admin:Open
        ser.flush()
        ver = ser.read(1)
        if ver:
            v = ord(ver)
            ser.write(b'\x00\x03')  # Admin:Close
            ser.flush()
            time.sleep(0.1)
            ser.close()
            return jsonify({"ok": True, "version": v, "info": f"WinKeyer found on {port} — firmware version {v}"})
        else:
            ser.close()
            return jsonify({"ok": False, "error": f"No response from {port} — check WinKeyer is powered and connected"})
    except Exception as e:
        return jsonify({"ok": False, "error": f"Cannot open {port} — {e}"})


@app.route("/api/flrig_data")
def flrig_data():
    """Browser polls this to get latest frequency/mode/rig from flrig."""
    return jsonify({
        "connected": flrig_connected,
        "enabled":   FLRIG_ENABLED,
        "freq_mhz":  latest_flrig.get("freq_mhz", ""),
        "mode":      latest_flrig.get("mode", ""),
        "rig":       latest_flrig.get("rig", ""),
    })


@app.route("/api/flrig_tune", methods=["POST"])
def flrig_tune():
    """Tune flrig rig to the requested frequency and/or mode.
    freq_mhz is optional — omit to change mode only."""
    if not FLRIG_ENABLED:
        return jsonify({"ok": False, "error": "flrig not enabled", "connected": False})
    if not flrig_connected:
        return jsonify({"ok": False, "error": "flrig not connected", "connected": False})
    data     = request.json or {}
    freq_mhz = data.get("freq_mhz")
    mode     = str(data.get("mode", "") or "")
    if freq_mhz is None and not mode:
        return jsonify({"ok": False, "error": "freq_mhz or mode required"}), 400
    if freq_mhz is not None:
        # Frequency (and optional mode) change
        _log(f"flrig_tune called: freq={freq_mhz} MHz  mode={mode!r}  digital_override={FLRIG_DIGITAL_MODE!r}")
        ok = flrig_set_freq_mode(freq_mhz, mode)
    else:
        # Mode-only change — use same digital mode logic as flrig_set_freq_mode
        ok = False
        flrig_mode = mode   # fallback label for error logging
        try:
            srv = _flrig_server()
            mode_up = mode.strip().upper()
            _DIGITAL_PASSTHROUGH_USB = {"FT8","FT4","JS8","WSPR","JT65","JT9","DIGI","PSK31","DIGU","DATA-U","PKT-U","MSK144","Q65","FST4","FST4W","VARAC","OLIVIA","HELL","PACKET","DATA"}
            _DIGITAL_PASSTHROUGH_LSB = {"DIGL","DATA-L","PKT-L"}
            if mode_up in _DIGITAL_PASSTHROUGH_USB:
                flrig_mode = FLRIG_DIGITAL_MODE if FLRIG_DIGITAL_MODE else latest_flrig.get("digital_usb", "DATA-U")
            elif mode_up in _DIGITAL_PASSTHROUGH_LSB:
                flrig_mode = latest_flrig.get("digital_lsb", "DATA-L")
            elif mode_up in {"RTTY", "RTTY-R", "RTTYR"}:
                flrig_mode = FLRIG_RTTY_MODE if FLRIG_RTTY_MODE else _FLRIG_MODE_OUT.get(mode_up, mode_up)
            else:
                flrig_mode = _FLRIG_MODE_OUT.get(mode_up, mode_up)
            _log(f"flrig set_mode (mode-only): SDRLogger+ mode={mode} → flrig mode={flrig_mode}")
            srv.rig.set_mode(flrig_mode)
            ok = True
        except Exception as e:
            _log(f"flrig set_mode error ({mode} → {flrig_mode}): {e}")
    return jsonify({"ok": ok, "connected": flrig_connected,
                    "error": "" if ok else "flrig tune failed — check Python console"})


@app.route("/api/list_serial_ports")
def list_serial_ports():
    """Return available COM/serial ports."""
    try:
        import serial.tools.list_ports as _lp
        ports = [{"port": p.device, "desc": p.description or p.device}
                 for p in sorted(_lp.comports(), key=lambda x: x.device)]
        return jsonify({"ok": True, "ports": ports})
    except Exception as e:
        return jsonify({"ok": False, "ports": [], "error": str(e)})


@app.route("/api/flrig_test", methods=["POST"])
def flrig_test():
    """Test flrig XML-RPC connection — returns rig name and current frequency."""
    host = (request.json or {}).get("host", FLRIG_HOST) or FLRIG_HOST
    port = int((request.json or {}).get("port", FLRIG_PORT) or FLRIG_PORT)
    try:
        import xmlrpc.client
        srv     = xmlrpc.client.ServerProxy(f"http://{host}:{port}", allow_none=True)
        rig     = srv.rig.get_xcvr() or "Unknown rig"
        freq_hz = srv.rig.get_vfoA()
        mode    = srv.rig.get_mode() or ""
        info    = f"flrig connected — {rig}"
        if freq_hz and int(freq_hz) > 0:
            info += f" — {round(int(freq_hz)/1_000_000, 3)} MHz {mode}".strip()
        return jsonify({"ok": True, "info": info})
    except Exception as e:
        return jsonify({"ok": False, "error": f"Cannot connect to flrig at {host}:{port} — {e}"})


@app.route("/api/eqsl_test")
def eqsl_test():
    """Test eQSL.cc credentials with a minimal ADIF POST."""
    user = request.args.get("user", EQSL_USER).strip()
    pwd  = request.args.get("pass", EQSL_PASS)
    if not user or not pwd:
        return jsonify({"ok": False, "error": "Enter eQSL username and password first"})
    try:
        adif = "<ADIF_VER:5>3.1.0<PROGRAMID:10>SDRLogger+<EOH>"
        resp = requests.post(
            "https://www.eQSL.cc/qslcard/ImportADIF.cfm",
            data={"EQSL_USER": user, "EQSL_PSWD": pwd, "ADIFData": adif},
            timeout=10,
        )
        body = resp.text or ""
        if resp.status_code != 200:
            return jsonify({"ok": False, "error": f"HTTP {resp.status_code}"})
        if "password" in body.lower() or "invalid user" in body.lower() or "not found" in body.lower():
            return jsonify({"ok": False, "error": "Authentication failed — check username and password"})
        return jsonify({"ok": True, "note": f"eQSL credentials accepted for {user}"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/set_mode", methods=["POST"])
def set_mode():
    """Switch active logging mode between 'general', 'pota', and 'sat'."""
    global ACTIVE_MODE, POTA_MY_PARK
    data = request.json or {}
    mode = data.get("mode", "general")
    if mode not in ("general", "pota", "sat"):
        return jsonify({"ok": False, "error": "Invalid mode"}), 400
    ACTIVE_MODE  = mode
    if "pota_my_park" in data:
        POTA_MY_PARK = data["pota_my_park"].strip().upper()
    # Make sure the target DB schema is ready
    if mode == "pota":
        _init_one_db(POTA_DATABASE)
    else:
        _init_one_db(DATABASE)
    return jsonify({"ok": True, "mode": ACTIVE_MODE, "pota_my_park": POTA_MY_PARK})


@app.route("/api/pota_spot", methods=["POST"])
def pota_spot():
    """
    Submit a self-spot to POTA.app API.
    Requires POTA username/password stored in settings.
    POST: { activator, reference, freq_mhz, mode, comment }
    """
    data      = request.json or {}
    activator = (data.get("activator") or MY_CALLSIGN or "").strip().upper()
    reference = (data.get("reference") or POTA_MY_PARK or "").strip().upper()
    freq_mhz  = data.get("freq_mhz", "")
    mode      = data.get("mode", "").strip().upper()
    comment   = data.get("comment", "").strip()
    pota_user = (data.get("pota_user") or POTA_USER or "").strip()
    pota_pass = (data.get("pota_pass") or POTA_PASS or "").strip()

    if not activator:
        return jsonify({"ok": False, "error": "Activator callsign required — set your callsign in Settings"}), 400
    if not reference:
        return jsonify({"ok": False, "error": "Park reference required — set My Park in Settings or the POTA banner"}), 400
    if not freq_mhz:
        return jsonify({"ok": False, "error": "Frequency required"}), 400
    try:
        freq_khz = round(float(freq_mhz) * 1000, 3)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid frequency"}), 400

    payload = {
        "activator": activator,
        "spotter":   activator,          # self-spot
        "frequency": str(freq_khz),
        "mode":      mode or "SSB",
        "reference": reference,
        "source":    "SDRLogger+",
        "comments":  comment,
    }
    auth = (pota_user, pota_pass) if pota_user and pota_pass else None
    try:
        resp = requests.post(
            "https://api.pota.app/spot",
            json=payload,
            auth=auth,
            timeout=10,
        )
        if resp.status_code in (200, 201):
            return jsonify({"ok": True, "msg": f"Spotted {activator} at {reference} on {freq_khz} kHz"})
        return jsonify({"ok": False, "error": f"POTA API: HTTP {resp.status_code} — {resp.text[:200]}"}), 502
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/spot_spothole", methods=["POST"])
def spot_spothole():
    """
    Spothole.app is a read-only spot aggregator — it has no public API for
    receiving externally submitted spots. This route returns a clear error so
    the user knows to use C1 (Cluster 1 telnet) for spot sending instead.
    """
    return jsonify({
        "ok": False,
        "error": "Spothole does not accept submitted spots — it is a read-only aggregator. "
                 "Use C1 (Cluster 1 telnet) to send spots to the DX cluster network."
    }), 400


@app.route("/api/tci_tune", methods=["POST"])
def tci_tune():
    """Tune Thetis SDR to a given frequency and/or mode via TCI commands."""
    if not TCI_ENABLED:
        return jsonify({"ok": False, "connected": False})
    data    = request.json or {}
    freq_mhz = data.get("freq_mhz")
    mode     = data.get("mode", "").strip().upper()
    sent     = False
    if freq_mhz:
        try:
            freq_hz = round(float(freq_mhz) * 1_000_000)
            if send_tci_command(f"vfo:0,0,{freq_hz};"):
                sent = True
                print(f"TCI tune → {freq_hz} Hz")
        except (ValueError, TypeError):
            return jsonify({"ok": False, "error": "Invalid frequency"}), 400
    if mode:
        # Map logger/log-only mode names to Thetis TCI modulation names.
        # Thetis accepts: USB LSB CWU CWL AM FM NFM DIGU DIGL SAM DSB SPEC DRM
        _TCI_MODE_MAP = {
            "SSB":  "USB",
            "CW":   "CWU",
            "FT8":  "DIGU", "FT4":  "DIGU", "JS8":  "DIGU",
            "MSK144":"DIGU", "Q65": "DIGU", "FST4":"DIGU", "FST4W":"DIGU",
            "WSPR": "DIGU", "JT65": "DIGU", "JT9":  "DIGU",
            "PSK31":"DIGU", "DIGI": "DIGU", "DATA": "DIGU", "VARAC":"DIGU",
            "OLIVIA":"DIGU","HELL":"DIGU", "PACKET":"DIGU",
            "RTTY": "DIGU", "RTTY-R":"DIGL", "RTTYR":"DIGL",
        }
        tci_mode = _TCI_MODE_MAP.get(mode, mode)
        send_tci_command(f"modulation:0,{tci_mode};")
        _log(f"TCI mode → {mode} → {tci_mode}")
    return jsonify({"ok": sent, "connected": tci_ws_connected})


@app.route("/api/tci_spot", methods=["POST"])
def tci_spot_route():
    """
    Forward a DX cluster spot to the Thetis waterfall via TCI spot command.
    Correct Thetis TCI format: spot:callsign,mode,freq_hz,argb_color;
    argb_color is an unsigned 32-bit integer (alpha=255, R, G, B).
    """
    data     = request.json or {}
    freq_khz = data.get("freq_khz")
    callsign = data.get("callsign", "").upper().strip()
    mode     = data.get("mode", "ssb").strip().lower() or "ssb"
    argb     = data.get("argb", 4278248959)   # default = cyan #00E5FF, alpha=255
    if not freq_khz or not callsign:
        return jsonify({"ok": False, "error": "freq_khz and callsign required"}), 400
    try:
        freq_hz = int(float(freq_khz) * 1000)
        argb    = int(argb)
    except (ValueError, TypeError):
        return jsonify({"ok": False, "error": "Invalid frequency or color"}), 400
    # Store in spot registry so VFO changes can reverse-lookup the callsign
    with tci_spot_registry_lock:
        tci_spot_registry[freq_hz] = callsign
    # Thetis TCI spec: spot:callsign,mode,freq_hz,argb;
    ok = send_tci_command(f"spot:{callsign},{mode},{freq_hz},{argb};")
    return jsonify({"ok": ok, "connected": tci_ws_connected})


@app.route("/api/rotator_turn", methods=["POST"])
def rotator_turn():
    """Turn rotator to a given azimuth. Body: {azimuth: float}"""
    data = request.json or {}
    az   = data.get("azimuth")
    if az is None:
        return jsonify({"ok": False, "error": "azimuth required"}), 400
    ok, err = rotator_send_azimuth(az)
    return jsonify({"ok": ok, "azimuth": az, "error": err})


def rotator_send_stop():
    """
    Halt rotation via TCP.

    GS-232     : sends 'A' + CR  (GS-232A azimuth stop — CR only, no LF).
    EasyComm II: sends 'SA\r\n' (stop azimuth).
    PstRotator : PstRotator's external control interface is UDP-based.
                 Stop is sent as <PST><STOP>1</STOP></PST> via UDP to
                 ROTATOR_PORT (default 12000) — the documented command used
                 by N1MM Logger+, Swisslog, etc.  If the background UDP
                 status poller has a live AZ reading, a TCP re-command to
                 that AZ is also sent as belt-and-suspenders insurance.
    """
    if not ROTATOR_ENABLED:
        return False, "Rotator not enabled"
    try:
        if ROTATOR_PROTOCOL == "gs232":
            # GS-232A spec: 'A' = stop azimuth rotation, CR terminator only
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3.0)
            s.connect((ROTATOR_HOST, ROTATOR_PORT))
            s.sendall(b"A\r")
            s.close()
            print("Rotator: sent GS-232 STOP (A\\r)")

        elif ROTATOR_PROTOCOL == "arco_tcp":
            # microHAM ARCO — 'S' = stop ALL motion (AZ + EL), GS-232A emulation
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3.0)
            s.connect((ROTATOR_HOST, ROTATOR_PORT))
            s.sendall(b"S\r")
            s.close()
            _rot_dbg("ARCO TX: S  (STOP all motion)")

        elif ROTATOR_PROTOCOL == "easycomm":
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3.0)
            s.connect((ROTATOR_HOST, ROTATOR_PORT))
            s.sendall(b"SA\r\n")
            s.close()
            print("Rotator: sent EasyComm STOP (SA)")

        else:  # pstrotator
            # PstRotator's documented external stop command is UDP-based.
            # Send <PST><STOP>1</STOP></PST> via UDP to ROTATOR_PORT (12000).
            # This matches the protocol used by N1MM Logger+, Swisslog, etc.
            us = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            us.sendto(b"<PST><STOP>1</STOP></PST>", (ROTATOR_HOST, ROTATOR_PORT))
            us.close()
            print("Rotator: sent PstRotator UDP STOP <PST><STOP>1</STOP></PST>")

            # Belt-and-suspenders: also re-command live AZ over TCP if we have it
            with _rot_live_az_lock:
                live_az = _rot_live_az
            if live_az is not None:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(3.0)
                    s.connect((ROTATOR_HOST, ROTATOR_PORT))
                    s.sendall(f"<rotate>{live_az:.1f}</rotate>\r\n".encode())
                    s.close()
                    print(f"Rotator: PstRotator TCP re-command AZ {live_az:.1f}°")
                except Exception:
                    pass   # UDP stop already sent — TCP is just belt-and-suspenders

        return True, None
    except Exception as e:
        print(f"Rotator stop error: {e}")
        return False, str(e)


@app.route("/api/rotator_stop", methods=["POST"])
def rotator_stop():
    """Send a stop command to the rotator controller."""
    ok, err = rotator_send_stop()
    return jsonify({"ok": ok, "error": err})


@app.route("/api/digital_events")
def digital_events():
    """Return and clear any pending auto-logged digital app QSOs."""
    events = list(_digital_events)
    _digital_events.clear()
    return jsonify({"events": events})


@app.route("/api/rotator_az")
def rotator_az():
    """Return live AZ (and EL + fault flag for ARCO) read by the background poller.
    Returns null fields when the protocol's poller has not yet connected.
    Frontend polls every 150 ms to animate Position display while antenna moves."""
    if ROTATOR_PROTOCOL == "arco_tcp":
        with _rot_state_lock:
            az    = _rot_live_az
            el    = _rot_live_el
            fault = _rot_fault
        return jsonify({"az": az, "el": el, "fault": fault,
                        "protocol": ROTATOR_PROTOCOL,
                        "enabled": ROTATOR_ENABLED})
    # Legacy PstRotator path
    with _rot_live_az_lock:
        az = _rot_live_az
    return jsonify({"az": az, "el": None, "fault": None,
                    "protocol": ROTATOR_PROTOCOL,
                    "enabled": ROTATOR_ENABLED})


@app.route("/api/rotator_debug_tail")
def rotator_debug_tail():
    """Return the last N lines of the rotator debug ring buffer (v1.08.1-beta).
    Used by the Rotator panel's '📋 Copy Logs' button to grab a snapshot
    for ARCO beta testers to send back."""
    try:
        n = int(request.args.get("n", "100"))
    except Exception:
        n = 100
    n = max(1, min(n, 200))
    with _rot_debug_lock:
        lines = list(_rot_debug_buf)[-n:]
    return jsonify({
        "lines": lines,
        "debug_enabled": ROTATOR_DEBUG,
        "protocol": ROTATOR_PROTOCOL,
        "host": ROTATOR_HOST,
        "port": ROTATOR_PORT,
        "version": VERSION,
    })


@app.route("/api/digital_status")
def digital_status():
    return jsonify({
        "udp_enabled": DIGITAL_UDP_ENABLED, "udp_port": DIGITAL_UDP_PORT,
        "tcp_enabled": DIGITAL_TCP_ENABLED, "tcp_port": DIGITAL_TCP_PORT,
        "rotator_enabled": ROTATOR_ENABLED, "rotator_host": ROTATOR_HOST,
        "rotator_port": ROTATOR_PORT, "rotator_protocol": ROTATOR_PROTOCOL,
        "rotator_auto": ROTATOR_AUTO, "rotator_debug": ROTATOR_DEBUG,
    })


# ─── Graceful shutdown via keepalive heartbeat ────────────────────────────────
# Each browser page sends POST /api/keepalive every 20 s.
# A background thread exits the process after KEEPALIVE_TIMEOUT seconds of
# silence — meaning all browser tabs/windows have been closed.
# This replaces the old pagehide/sendBeacon/cancel approach which suffered from
# a race condition: navigating between pages (e.g. index → help) could fire
# sendBeacon before the new page cancelled the timer, killing the process.

import time as _time

KEEPALIVE_TIMEOUT = 3600   # seconds — 60 min; forgives walk-aways, PC sleep, Chrome background-tab freeze
EXPLICIT_CLOSE_GRACE = 10  # seconds — on tab-close beacon, schedule shutdown this far out so
                           #           intra-app navigation (index↔help) can cancel it via the
                           #           immediate-on-load keepalive fired by the new page
_last_keepalive   = _time.monotonic()
_keepalive_lock   = threading.Lock()

@app.route("/api/keepalive", methods=["POST"])
def api_keepalive():
    """Heartbeat sent by every page every 20 s (and immediately on page load)
    to keep the server alive. Immediate-on-load call also cancels any pending
    explicit_close shutdown scheduled by a just-unloaded sibling page."""
    global _last_keepalive
    with _keepalive_lock:
        _last_keepalive = _time.monotonic()
    return "", 204


@app.route("/api/explicit_close", methods=["POST"])
def api_explicit_close():
    """Tab close beacon (navigator.sendBeacon on pagehide).
    We do NOT exit immediately — instead we rewind the last-keepalive timestamp
    so the watchdog's next check (up to 10 s later) will see us as idle past
    the EXPLICIT_CLOSE_GRACE window. If another page of our app loads within
    that window (page-to-page nav, browser refresh), its immediate-on-load
    /api/keepalive call resets the clock and we stay alive. Only a true tab
    close with nothing replacing it results in shutdown."""
    global _last_keepalive
    with _keepalive_lock:
        # Arrange for watchdog to fire shortly after EXPLICIT_CLOSE_GRACE
        _last_keepalive = _time.monotonic() - (KEEPALIVE_TIMEOUT - EXPLICIT_CLOSE_GRACE)
    # Fire an on-exit auto-backup if the user has chosen that interval.
    # Runs in a daemon thread so we can still return the 204 quickly and let
    # the browser beacon close cleanly.
    try:
        if AUTO_BACKUP_ENABLED and AUTO_BACKUP_INTERVAL == "on_exit":
            threading.Thread(target=_run_auto_backup, args=("on_exit",), daemon=True).start()
    except Exception:
        pass
    return "", 204

def _keepalive_watchdog():
    """Exit the process when no keepalive has arrived for KEEPALIVE_TIMEOUT s.
    Polls every 2 s so an explicit_close tab-beacon results in shutdown within
    ~EXPLICIT_CLOSE_GRACE seconds (default 10 s) of the user closing the tab."""
    while True:
        _time.sleep(2)    # check every 2 s — tight enough for the grace window
        with _keepalive_lock:
            idle = _time.monotonic() - _last_keepalive
        if idle >= KEEPALIVE_TIMEOUT:
            print(f"[SDRLogger+] No browser keepalive for {int(idle)}s — shutting down.")
            os._exit(0)

_ka_watchdog = threading.Thread(target=_keepalive_watchdog, daemon=True)
_ka_watchdog.start()


@app.route("/api/tci_spots_clear", methods=["POST"])
def tci_spots_clear():
    """Clear all spot markers from the Thetis panadapter and the spot registry."""
    with tci_spot_registry_lock:
        tci_spot_registry.clear()
    ok = send_tci_command("spot_clear;")
    return jsonify({"ok": ok, "connected": tci_ws_connected})


@app.route("/api/qrz_test_logbook")
def qrz_test_logbook():
    """Test the Logbook API key using STATUS action."""
    key = request.args.get("key", QRZ_LOGBOOK_KEY)
    if not key:
        return jsonify({"ok": False, "error": "No Logbook API key provided"})
    try:
        resp = requests.post(
            "https://logbook.qrz.com/api",
            data=f"KEY={key}&ACTION=STATUS",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": f"HamLog/1.0 ({MY_CALLSIGN})"
            },
            timeout=5
        )
        result = {}
        for part in resp.text.strip().split("&"):
            if "=" in part:
                k, v = part.split("=", 1)
                result[k] = v
        if result.get("RESULT") == "OK":
            data_str = result.get("DATA", "")
            info = {}
            for item in data_str.split("&"):
                if "=" in item:
                    k, v = item.split("=", 1)
                    info[k] = v
            return jsonify({
                "ok": True,
                "callsign":   info.get("BOOK_CALLSIGN", info.get("CALLSIGN", "?")),
                "total_qsos": info.get("TOTQSOS", "?"),
                "confirmed":  info.get("CONFIRMS", "?"),
            })
        else:
            reason = result.get("REASON", resp.text[:300])
            return jsonify({"ok": False, "error": reason})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/log")
def get_log():
    limit  = int(request.args.get("limit",  200))
    offset = int(request.args.get("offset", 0))
    search = request.args.get("search", "").strip()
    conn   = get_db()
    # SAT mode: only show SAT QSOs (prop_mode = 'SAT')
    sat_filter = " AND (prop_mode = 'SAT' OR prop_mode = 'sat')" if ACTIVE_MODE == "sat" else ""
    if search:
        like  = f"%{search}%"
        where = ("callsign LIKE ? OR band LIKE ? OR mode LIKE ? "
                 "OR contest_name LIKE ? OR remarks LIKE ?")
        params = (like, like, like, like, like)
        total = conn.execute(
            f"SELECT COUNT(*) FROM qso_log WHERE ({where}){sat_filter}", params
        ).fetchone()[0]
        rows  = conn.execute(
            f"SELECT * FROM qso_log WHERE ({where}){sat_filter} "
            "ORDER BY date_worked DESC, time_worked DESC LIMIT ? OFFSET ?",
            params + (limit, offset)
        ).fetchall()
    else:
        sat_where = "WHERE prop_mode = 'SAT' OR prop_mode = 'sat'" if ACTIVE_MODE == "sat" else ""
        total = conn.execute(f"SELECT COUNT(*) FROM qso_log {sat_where}").fetchone()[0]
        rows  = conn.execute(
            f"SELECT * FROM qso_log {sat_where} ORDER BY date_worked DESC, time_worked DESC LIMIT ? OFFSET ?",
            (limit, offset)
        ).fetchall()
    conn.close()
    return jsonify({"rows": [dict(r) for r in rows], "total": total, "offset": offset, "limit": limit})


@app.route("/api/qso/<int:qso_id>", methods=["DELETE"])
def delete_qso(qso_id):
    conn = get_db()
    conn.execute("DELETE FROM qso_log WHERE id=?", (qso_id,))
    conn.commit()
    conn.close()
    threading.Thread(target=_rebuild_worked_cache, daemon=True).start()
    return jsonify({"ok": True})


@app.route("/api/qso/<int:qso_id>", methods=["PUT"])
def update_qso(qso_id):
    data = request.json
    conn = get_db()
    conn.execute("""
        UPDATE qso_log SET
            callsign=?, name=?, qth=?, date_worked=?, time_worked=?,
            band=?, mode=?, freq_mhz=?, my_rst_sent=?, their_rst_rcvd=?,
            remarks=?, contest_name=?, state=?, country=?, gridsquare=?
        WHERE id=?
    """, (
        data.get("callsign","").upper(),
        data.get("name",""),
        data.get("qth",""),
        data.get("date_worked",""),
        data.get("time_worked",""),
        data.get("band",""),
        data.get("mode",""),
        float(data["freq_mhz"]) if data.get("freq_mhz") else None,
        data.get("my_rst_sent",""),
        data.get("their_rst_rcvd",""),
        data.get("remarks",""),
        data.get("contest_name",""),
        data.get("state",""),
        data.get("country",""),
        data.get("gridsquare",""),
        qso_id,
    ))
    conn.commit()
    conn.close()
    _worked_cache_add(data.get("callsign",""), data.get("band",""), data.get("mode",""))
    return jsonify({"ok": True})


def _safe_col(row, col):
    """Safely get a column from a sqlite3.Row, returning '' if column doesn't exist."""
    try:
        return row[col] or ""
    except (IndexError, KeyError):
        return ""


@app.route("/api/export_adif")
def export_adif():
    conn = get_db()
    rows = conn.execute("SELECT * FROM qso_log ORDER BY date_worked, time_worked").fetchall()
    conn.close()

    is_pota = (ACTIVE_MODE == "pota")
    prog_id = "SDRLogger+"
    lines = [f"SDRLogger+ ADIF Export — {'POTA Activation' if is_pota else 'General Log'}",
             f"<PROGRAMID:{len(prog_id)}>{prog_id}\n<EOH>\n"]
    for r in rows:
        def tag(name, val):
            val = str(val) if val else ""
            return f"<{name}:{len(val)}>{val} " if val else ""
        pota_ref = (r["pota_ref"] or "").strip() if is_pota else ""
        pota_p2p = (r["pota_p2p"] or "").strip() if is_pota else ""
        line = (
            tag("CALL", r["callsign"]) +
            tag("QSO_DATE", (r["date_worked"] or "").replace("-", "")) +
            tag("TIME_ON", (r["time_worked"] or "").replace(":", "")[:6]) +
            tag("BAND", r["band"]) +
            tag("MODE", r["mode"]) +
            tag("FREQ", r["freq_mhz"]) +
            tag("RST_SENT", r["my_rst_sent"]) +
            tag("RST_RCVD", r["their_rst_rcvd"]) +
            tag("COMMENT", r["remarks"]) +
            tag("CONTEST_ID", r["contest_name"]) +
            (tag("MY_SIG", "POTA") + tag("MY_SIG_INFO", pota_ref) if pota_ref else "") +
            (tag("SIG", "POTA") + tag("SIG_INFO", pota_p2p) if pota_p2p else "") +
            tag("NAME", r["name"]) +
            tag("QTH", r["qth"]) +
            tag("STATE", _safe_col(r, "state")) +
            tag("COUNTRY", _safe_col(r, "country")) +
            tag("GRIDSQUARE", _safe_col(r, "gridsquare")) +
            (tag("PROP_MODE", _safe_col(r, "prop_mode")) if _safe_col(r, "prop_mode") else "") +
            (tag("SAT_NAME", _safe_col(r, "sat_name")) if _safe_col(r, "sat_name") else "") +
            (tag("FREQ_RX", _safe_col(r, "uplink_freq")) if _safe_col(r, "uplink_freq") else "") +
            "<EOR>"
        )
        lines.append(line)

    adif_text = "\n".join(lines)
    buf = io.BytesIO(adif_text.encode("utf-8"))
    buf.seek(0)
    prefix = f"pota_{POTA_MY_PARK}_{date.today()}" if ACTIVE_MODE == "pota" else f"hamlog_{date.today()}"
    return send_file(buf, mimetype="text/plain",
                     as_attachment=True,
                     download_name=f"{prefix}.adi")


@app.route("/api/import_adif", methods=["POST"])
def import_adif():
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "No file uploaded"}), 400
    content  = request.files["file"].read().decode("utf-8", errors="ignore").upper()
    dup_mode = request.form.get("dup_mode", "skip")  # skip | replace | keep_both

    import re
    # Strip ADIF header (everything before <EOH>) to avoid picking up
    # header fields (ADIF_VER, PROGRAMID, etc.) as QSO data
    eoh_pos = content.find("<EOH>")
    if eoh_pos >= 0:
        content = content[eoh_pos + 5:]
    records = content.split("<EOR>")
    imported = 0
    skipped  = 0
    replaced = 0

    def get_field(rec, field):
        m = re.search(rf"<{field}:(\d+)>([^<]*)", rec)
        if m:
            length = int(m.group(1))
            return m.group(2)[:length].strip()
        return ""

    conn = get_db()
    for rec in records:
        callsign = get_field(rec, "CALL")
        if not callsign:
            continue
        raw_date = get_field(rec, "QSO_DATE")
        date_worked = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}" if len(raw_date) == 8 else raw_date
        raw_time = get_field(rec, "TIME_ON")
        time_worked = f"{raw_time[:2]}:{raw_time[2:4]}:{raw_time[4:6]}" if len(raw_time) >= 4 else raw_time
        band    = get_field(rec, "BAND")
        mode    = get_field(rec, "MODE")
        freq_str = get_field(rec, "FREQ")
        try:
            freq = float(freq_str)
        except Exception:
            freq = None

        # Duplicate detection: same callsign + date + band + mode
        dup_row = conn.execute(
            "SELECT id FROM qso_log WHERE callsign=? AND date_worked=? AND band=? AND mode=?",
            (callsign, date_worked, band, mode)
        ).fetchone()

        if dup_row:
            if dup_mode == "skip":
                skipped += 1
                continue
            elif dup_mode == "replace":
                conn.execute("DELETE FROM qso_log WHERE id=?", (dup_row["id"],))
                replaced += 1
            # keep_both: fall through to INSERT

        # ADIF NAME = contacted operator's name; QTH = their city
        # COMMENT or NOTES (plural per ADIF 3.x spec) for remarks
        name_val    = get_field(rec, "NAME")
        qth_val     = get_field(rec, "QTH")
        remarks_val = get_field(rec, "COMMENT") or get_field(rec, "NOTES") or get_field(rec, "QSLMSG")
        state_val   = get_field(rec, "STATE")
        # Fallback: extract state from CNTY field (format "OH,Butler")
        if not state_val:
            cnty = get_field(rec, "CNTY")
            if cnty and "," in cnty:
                state_val = cnty.split(",")[0].strip()
        country_val = get_field(rec, "COUNTRY")
        grid_val    = get_field(rec, "GRIDSQUARE")

        conn.execute("""
            INSERT INTO qso_log
                (callsign, name, qth, date_worked, time_worked, band, mode,
                 freq_mhz, my_rst_sent, their_rst_rcvd, remarks, contest_name,
                 state, country, gridsquare)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            callsign,
            name_val,
            qth_val,
            date_worked,
            time_worked,
            band,
            mode,
            freq,
            get_field(rec, "RST_SENT"),
            get_field(rec, "RST_RCVD"),
            remarks_val,
            get_field(rec, "CONTEST_ID"),
            state_val,
            country_val,
            grid_val,
        ))
        imported += 1
    conn.commit()
    conn.close()
    # Rebuild worked DXCC cache after bulk import
    threading.Thread(target=_rebuild_worked_cache, daemon=True).start()
    return jsonify({"ok": True, "imported": imported, "skipped": skipped, "replaced": replaced})


# ─── DX Cluster WebSocket Proxy ───────────────────────────────────────────────
@sock.route("/api/cluster_ws")
def cluster_ws(ws):
    """
    Browser connects here via WebSocket.
    Proxies a telnet connection to the DX cluster and streams spots to the browser.

    Reliability features:
    - Auto-reconnects to the telnet cluster if it drops (idle timeout, restart, etc.)
      WITHOUT closing the browser WebSocket — the browser stays connected throughout.
    - Keepalive thread sends \\r\\n to the cluster every 3 minutes to prevent
      the cluster server from timing out the idle connection.
    - Exponential back-off (5 → 10 → 20 → … → 120 s) between reconnect attempts.
    """
    server   = request.args.get("server",   "ve7cc.net")
    port     = int(request.args.get("port", "23"))
    callsign = request.args.get("callsign", "").upper() or MY_CALLSIGN

    def open_telnet():
        t = socket.create_connection((server, port), timeout=15)
        t.settimeout(0.3)
        return t

    print(f"Cluster: connecting to {server}:{port} as {callsign}")
    try:
        _tel = open_telnet()
    except Exception as e:
        try: ws.send(f"ERROR: Cannot connect to {server}:{port} — {e}")
        except: pass
        return

    # tel_ref[0] is the live telnet socket; use tel_lock whenever reassigning it
    tel_ref   = [_tel]
    tel_lock  = threading.Lock()
    stop_evt  = threading.Event()
    logged_in = threading.Event()

    # ── telnet reader — auto-reconnects on cluster-side drops ─────────────────
    def telnet_reader():
        buf            = ""
        reconnect_tries = 0

        while not stop_evt.is_set():
            try:
                with tel_lock:
                    t = tel_ref[0]
                chunk = t.recv(4096).decode("utf-8", errors="ignore")

                if not chunk:
                    raise OSError("cluster closed connection")

                reconnect_tries = 0   # successful read — reset back-off
                buf += chunk

                # Login prompt — send callsign once
                lower = chunk.lower()
                if not logged_in.is_set():
                    if any(p in lower for p in ("login:", "call:", "enter your call", "callsign")):
                        t.sendall((callsign + "\r\n").encode())
                        print(f"Cluster: sent callsign {callsign}")
                        logged_in.set()
                    elif "password" in lower:
                        t.sendall(b"\r\n")

                # Forward complete lines to the browser WebSocket
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    line = line.rstrip("\r").strip()
                    if line:
                        try:
                            ws.send(line)
                        except Exception:
                            stop_evt.set()
                            return

            except socket.timeout:
                continue   # normal — no data right now, loop again

            except OSError as exc:
                # ── Telnet cluster dropped — attempt to reconnect ──────────
                if reconnect_tries >= 8:
                    print("Cluster: too many reconnect failures — closing")
                    stop_evt.set()
                    break

                delay = min(5 * (2 ** reconnect_tries), 120)
                reconnect_tries += 1
                print(f"Cluster: {server} disconnected ({exc}) — "
                      f"reconnect #{reconnect_tries} in {delay}s")

                try:
                    ws.send(f"--- {server} disconnected — "
                            f"reconnecting in {delay}s (attempt {reconnect_tries}) ---")
                except Exception:
                    stop_evt.set()
                    break

                try:
                    with tel_lock:
                        tel_ref[0].close()
                except Exception:
                    pass

                if stop_evt.wait(delay):   # wait returns True if event was set
                    break

                try:
                    with tel_lock:
                        tel_ref[0] = open_telnet()
                    logged_in.clear()
                    buf = ""
                    print(f"Cluster: reconnected to {server}:{port}")
                    try:
                        ws.send(f"--- Reconnected to {server}:{port} ---")
                    except Exception:
                        stop_evt.set()
                        break
                except Exception as e2:
                    print(f"Cluster: reconnect failed: {e2}")
                    # delay already waited; loop will try again with next back-off

    # ── Keepalive — send \\r\\n every 3 min to prevent idle timeout ────────────
    def keepalive():
        while not stop_evt.wait(180):   # wakes immediately if stop_evt is set
            try:
                with tel_lock:
                    tel_ref[0].sendall(b"\r\n")
            except Exception:
                pass   # telnet_reader will handle any reconnect needed

    reader    = threading.Thread(target=telnet_reader, daemon=True)
    ka_thread = threading.Thread(target=keepalive,     daemon=True)
    reader.start()
    ka_thread.start()

    # ── Main loop — forward browser commands → telnet; exit on WS disconnect ──
    while not stop_evt.is_set():
        try:
            msg = ws.receive(timeout=30)
            if msg is None:
                break   # browser closed cleanly
            with tel_lock:
                tel_ref[0].sendall((msg.strip() + "\r\n").encode())
        except Exception as e:
            err = str(e).lower()
            if any(x in err for x in ("timed out", "timeout", "time out")):
                continue   # receive timeout — browser still alive, loop again
            print(f"Cluster: browser WebSocket closed ({e})")
            break

    stop_evt.set()
    try:
        with tel_lock:
            tel_ref[0].close()
    except Exception:
        pass
    print("Cluster: session ended")


# ─── Spothole.app API WebSocket Proxy ─────────────────────────────────────────
@sock.route("/api/spothole_ws")
def spothole_ws(ws):
    """
    Streams DX spots from the public Spothole.app REST API to the browser.
    Polls GET https://spothole.app/api/v1/spots every 30 s and pushes
    new spots as JSON  { "type": "spot", "spot": {...} }  to the browser.

    Query params:
      max_age   seconds — how old spots to include on first load (default 600)
      band      comma-separated band filter e.g. "20m,15m,10m"
      source    comma-separated source filter e.g. "Cluster,POTA"
    """
    import time as _time
    max_age  = int(request.args.get("max_age", "600"))
    band_f   = request.args.get("band", "")
    source_f = request.args.get("source", "Cluster")

    stop_evt      = threading.Event()
    last_received = [None]   # mutable container for thread access

    def poll():
        while not stop_evt.is_set():
            try:
                params = {"limit": 300}
                if last_received[0] is None:
                    params["max_age"] = max_age
                else:
                    # Use received_since to get only NEW spots (avoids duplicates)
                    params["received_since"] = last_received[0]
                if band_f:
                    params["band"] = band_f
                if source_f:
                    params["source"] = source_f

                resp = requests.get(
                    "https://spothole.app/api/v1/spots",
                    params=params, timeout=15
                )
                if resp.ok:
                    spots = resp.json()
                    for spot in spots:
                        rt = spot.get("received_time")
                        if rt and (last_received[0] is None or rt > last_received[0]):
                            last_received[0] = rt
                        try:
                            ws.send(json.dumps({"type": "spot", "spot": spot}))
                        except Exception:
                            stop_evt.set()
                            return
                    if spots:
                        print(f"Spothole: forwarded {len(spots)} spot(s)")
                else:
                    print(f"Spothole: HTTP {resp.status_code}")
            except Exception as e:
                print(f"Spothole poll error: {e}")

            stop_evt.wait(30)   # poll every 30 s

    t = threading.Thread(target=poll, daemon=True)
    t.start()

    # Keep main thread alive; browser can send pings but we mostly just wait
    try:
        while not stop_evt.is_set():
            try:
                msg = ws.receive(timeout=45)
                if msg is None:
                    break
            except Exception as e:
                err = str(e).lower()
                if any(x in err for x in ("timed out", "timeout", "time out")):
                    continue
                break
    finally:
        stop_evt.set()
    print("Spothole: session ended")


# ─── RBN (Reverse Beacon Network) Band-Opening Alerts ────────────────────────
_rbn_skimmer_cache = {}   # callsign -> grid (persisted in memory for session)

def _rbn_lookup_grid(callsign):
    """Look up a skimmer's grid via QRZ/HamQTH, return grid str or None."""
    cs = callsign.upper().rstrip("-#1234567890")  # strip "-#" suffix from skimmer calls
    if cs in _rbn_skimmer_cache:
        return _rbn_skimmer_cache[cs]
    # Try unified lookup (QRZ first, then HamQTH)
    grid = None
    if QRZ_USER and QRZ_PASS and QRZ_LOOKUP_ENABLED:
        data, _ = qrz_lookup(cs)
        if data and data.get("grid"):
            grid = data["grid"]
    if not grid and HAMQTH_USER and HAMQTH_PASS:
        data, _ = hamqth_lookup(cs)
        if data and data.get("grid"):
            grid = data["grid"]
    _rbn_skimmer_cache[cs] = grid   # cache even None to avoid repeated lookups
    if grid:
        print(f"RBN: cached grid for skimmer {cs} -> {grid}")
    return grid


@sock.route("/api/rbn_ws")
def rbn_ws(ws):
    """
    Connects to the Reverse Beacon Network via telnet and streams VHF/UHF
    spots to the browser for band-opening alerts.

    Query params:
      callsign  — login callsign
      bands     — comma-separated band list e.g. "6m,2m,70cm" or "10m,6m,2m,70cm"
    """
    callsign = (request.args.get("callsign", "") or MY_CALLSIGN).upper()
    bands    = request.args.get("bands", "6m,2m,70cm")
    RBN_HOST = request.args.get("server", "telnet.reversebeacon.net")
    RBN_PORT = int(request.args.get("port", "7000"))

    # Build DX Spider frequency filter ranges for requested bands
    band_ranges = []
    for b in bands.split(","):
        b = b.strip().lower()
        if b == "10m":  band_ranges.append(("28000", "29700"))
        elif b == "6m": band_ranges.append(("50000", "54000"))
        elif b == "2m": band_ranges.append(("144000", "148000"))
        elif b in ("70cm", "440"): band_ranges.append(("420000", "450000"))

    if not band_ranges:
        try: ws.send(json.dumps({"type": "error", "msg": "No valid bands selected"}))
        except: pass
        return

    def open_telnet():
        t = socket.create_connection((RBN_HOST, RBN_PORT), timeout=15)
        t.settimeout(0.3)
        return t

    print(f"RBN: connecting to {RBN_HOST}:{RBN_PORT} as {callsign} for bands {bands}")
    try:
        _tel = open_telnet()
    except Exception as e:
        try: ws.send(json.dumps({"type": "error", "msg": f"Cannot connect to RBN: {e}"}))
        except: pass
        return

    tel_ref   = [_tel]
    tel_lock  = threading.Lock()
    stop_evt  = threading.Event()
    logged_in = threading.Event()
    filters_set = threading.Event()

    import re as _re
    rbn_spot_re = _re.compile(
        r"DX\s+de\s+(\S+?):\s+"          # spotter (skimmer) call
        r"(\d+\.?\d*)\s+"                 # freq in kHz
        r"(\S+)\s+"                       # DX callsign
        r"(\S+)\s+"                       # mode (CW, RTTY, FT8, etc.)
        r"(\d+)\s+dB\s+"                  # SNR
        r"(\d+)\s+(?:WPM|BPS)\s+"         # speed
        r"(\S+)\s+"                       # type (CQ, NCDXF, etc.)
        r"(\d{4})Z",                      # time
        _re.IGNORECASE
    )

    def telnet_reader():
        buf = ""
        reconnect_tries = 0

        while not stop_evt.is_set():
            try:
                with tel_lock:
                    t = tel_ref[0]
                chunk = t.recv(4096).decode("utf-8", errors="ignore")
                if not chunk:
                    raise OSError("RBN closed connection")

                reconnect_tries = 0
                buf += chunk

                # Login prompt
                lower = chunk.lower()
                if not logged_in.is_set():
                    if any(p in lower for p in ("login:", "call:", "enter your call", "callsign", "please enter")):
                        t.sendall((callsign + "\r\n").encode())
                        print(f"RBN: sent callsign {callsign}")
                        logged_in.set()
                        # Set frequency filters after login (small delay for server readiness)
                        import time as _time
                        _time.sleep(1)
                        # Reject all spots first, then accept only our bands
                        t.sendall(b"set/noskimmer\r\n")
                        _time.sleep(0.3)
                        t.sendall(b"reject/spots all\r\n")
                        _time.sleep(0.3)
                        for i, (lo, hi) in enumerate(band_ranges, 1):
                            cmd = f"accept/spots {i} on freq {lo}/{hi}\r\n"
                            t.sendall(cmd.encode())
                            _time.sleep(0.2)
                        filters_set.set()
                        print(f"RBN: frequency filters set for {bands}")

                # Parse complete lines
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    line = line.rstrip("\r").strip()
                    if not line:
                        continue

                    # Only process spot lines after filters are set
                    if not filters_set.is_set():
                        continue

                    m = rbn_spot_re.search(line)
                    if not m:
                        continue

                    skimmer  = m.group(1).rstrip(":")
                    freq_khz = m.group(2)
                    dx_call  = m.group(3).upper()
                    mode     = m.group(4).upper()
                    snr      = int(m.group(5))
                    speed    = m.group(6)
                    spot_type = m.group(7)
                    utc_time = m.group(8)

                    # Determine band from frequency
                    f = float(freq_khz)
                    if   28000 <= f <= 29700:  band = "10m"
                    elif 50000 <= f <= 54000:  band = "6m"
                    elif 144000 <= f <= 148000: band = "2m"
                    elif 420000 <= f <= 450000: band = "70cm"
                    else: continue

                    # Look up skimmer grid (cached)
                    skimmer_grid = _rbn_lookup_grid(skimmer)

                    spot_msg = {
                        "type":     "rbn_spot",
                        "dxCall":   dx_call,
                        "freqKhz":  freq_khz,
                        "band":     band,
                        "mode":     mode,
                        "snr":      snr,
                        "speed":    speed,
                        "spotType": spot_type,
                        "skimmer":  skimmer,
                        "skimmerGrid": skimmer_grid,
                        "time":     f"{utc_time[:2]}:{utc_time[2:]}"
                    }

                    try:
                        ws.send(json.dumps(spot_msg))
                    except Exception:
                        stop_evt.set()
                        return

            except socket.timeout:
                continue
            except OSError as exc:
                if reconnect_tries >= 8:
                    print("RBN: too many reconnect failures — closing")
                    stop_evt.set()
                    break

                delay = min(5 * (2 ** reconnect_tries), 120)
                reconnect_tries += 1
                print(f"RBN: disconnected ({exc}) — reconnect #{reconnect_tries} in {delay}s")

                try:
                    ws.send(json.dumps({"type": "status", "msg":
                        f"RBN disconnected — reconnecting in {delay}s (attempt {reconnect_tries})"}))
                except Exception:
                    stop_evt.set()
                    break

                try:
                    with tel_lock:
                        tel_ref[0].close()
                except Exception:
                    pass

                if stop_evt.wait(delay):
                    break

                try:
                    with tel_lock:
                        tel_ref[0] = open_telnet()
                    logged_in.clear()
                    filters_set.clear()
                    buf = ""
                    print(f"RBN: reconnected to {RBN_HOST}:{RBN_PORT}")
                    try:
                        ws.send(json.dumps({"type": "status", "msg": "Reconnected to RBN"}))
                    except Exception:
                        stop_evt.set()
                        break
                except Exception as e2:
                    print(f"RBN: reconnect failed: {e2}")

    # Keepalive every 3 minutes
    def keepalive():
        while not stop_evt.wait(180):
            try:
                with tel_lock:
                    tel_ref[0].sendall(b"\r\n")
            except Exception:
                pass

    reader    = threading.Thread(target=telnet_reader, daemon=True)
    ka_thread = threading.Thread(target=keepalive,     daemon=True)
    reader.start()
    ka_thread.start()

    # Main loop — keep alive until browser disconnects
    while not stop_evt.is_set():
        try:
            msg = ws.receive(timeout=30)
            if msg is None:
                break
        except Exception as e:
            err = str(e).lower()
            if any(x in err for x in ("timed out", "timeout", "time out")):
                continue
            break

    stop_evt.set()
    try:
        with tel_lock:
            tel_ref[0].close()
    except Exception:
        pass
    print("RBN: session ended")


# ─── POTA Spots API ───────────────────────────────────────────────────────────
_pota_spots_cache = {"data": [], "ts": 0}

@app.route("/api/pota_spots")
def pota_spots():
    """Proxy POTA activator spots with 60-second cache."""
    import time
    now = time.time()
    if now - _pota_spots_cache["ts"] < 60 and _pota_spots_cache["data"]:
        return jsonify(_pota_spots_cache["data"])
    try:
        resp = requests.get("https://api.pota.app/spot/activator", timeout=10)
        if resp.ok:
            spots = resp.json()
            _pota_spots_cache["data"] = spots
            _pota_spots_cache["ts"] = now
            return jsonify(spots)
    except Exception as e:
        _log(f"POTA spots fetch failed: {e}")
    return jsonify(_pota_spots_cache["data"])  # return stale cache on error


# ─── Delete Database ──────────────────────────────────────────────────────────
@app.route("/api/delete_db", methods=["POST"])
def delete_db():
    """Delete (wipe) the active database. Creates a safety backup first."""
    target = request.json.get("target", "active")  # "general" | "pota" | "active"
    if target == "pota":
        db_path = POTA_DATABASE
        label = "pota"
    elif target == "general":
        db_path = DATABASE
        label = "general"
    else:
        db_path = POTA_DATABASE if ACTIVE_MODE == "pota" else DATABASE
        label = ACTIVE_MODE

    if not os.path.exists(db_path):
        return jsonify({"ok": False, "error": f"Database {label} not found"}), 404

    # Safety backup before deletion
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safety_dir = os.path.join(os.path.dirname(db_path) or ".", "backups")
    os.makedirs(safety_dir, exist_ok=True)
    safety_name = f"{label}_pre_delete_{ts}.db"
    safety_path = os.path.join(safety_dir, safety_name)
    try:
        shutil.copy2(db_path, safety_path)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Safety backup failed: {e}"}), 500

    # Delete and reinitialise
    try:
        os.remove(db_path)
        _init_one_db(db_path)
        return jsonify({"ok": True, "backup": safety_name,
                        "msg": f"{label} database wiped. Safety backup saved as {safety_name}."})
    except Exception as e:
        return jsonify({"ok": False, "error": f"Delete failed: {e}"}), 500


# ─── Update Check ─────────────────────────────────────────────────────────────
_update_cache = None   # {"latest": "0.51", "url": "https://...", "has_update": True/False}

@app.route("/api/update_check")
def update_check():
    """Check GitHub for a newer release. Called once at startup by the frontend."""
    global _update_cache
    force = request.args.get("force", "0") == "1"
    if _update_cache is not None and not force:
        return jsonify(_update_cache)
    try:
        # Try /releases/latest first (skips drafts AND pre-releases).
        # If 404, fall back to /releases list which includes pre-releases.
        resp = requests.get(
            "https://api.github.com/repos/N8SDR1/SDRLoggerPlus/releases/latest",
            timeout=10,
            headers={"Accept": "application/vnd.github.v3+json"}
        )
        if resp.status_code == 404:
            # No full release found — check all releases (includes pre-releases)
            resp = requests.get(
                "https://api.github.com/repos/N8SDR1/SDRLoggerPlus/releases",
                timeout=10,
                headers={"Accept": "application/vnd.github.v3+json"}
            )
            if resp.ok:
                releases = [r for r in resp.json() if not r.get("draft")]
                if releases:
                    resp_data = releases[0]  # most recent non-draft
                else:
                    resp_data = None
            else:
                resp_data = None
        elif resp.ok:
            resp_data = resp.json()
        else:
            resp_data = None

        if resp_data:
            data = resp_data
            tag = data.get("tag_name", "").lstrip("vV").strip()
            url = data.get("html_url", "https://github.com/N8SDR1/SDRLoggerPlus/releases")
            # Compare version numbers (strip " Beta" etc.)
            current = VERSION.split()[0]  # "0.50"
            has_update = False
            try:
                from packaging.version import Version
                has_update = Version(tag) > Version(current)
            except Exception:
                # Fallback: simple string comparison
                has_update = tag != current and tag > current
            _update_cache = {"latest": tag, "url": url, "has_update": has_update, "current": current}
        else:
            _update_cache = {"latest": None, "url": None, "has_update": False, "current": VERSION.split()[0]}
    except Exception as e:
        print(f"Update check failed: {e}")
        _update_cache = {"latest": None, "url": None, "has_update": False, "current": VERSION.split()[0]}
    return jsonify(_update_cache)


# ─── Solar Conditions ─────────────────────────────────────────────────────────
_solar_cache      = None
_solar_cache_time = 0
SOLAR_CACHE_TTL   = 900   # 15 minutes

@app.route("/api/solar")
def get_solar():
    import xml.etree.ElementTree as ET
    import time as _time
    global _solar_cache, _solar_cache_time
    if _solar_cache and (_time.time() - _solar_cache_time) < SOLAR_CACHE_TTL:
        return jsonify(_solar_cache)
    try:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        resp = requests.get("https://www.hamqsl.com/solarxml.php", timeout=10, verify=False)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        # Try <solardata> child first, fall back to root
        sd = root.find(".//solardata") or root

        def gf(tag):
            el = sd.find(tag)
            return el.text.strip() if el is not None and el.text else "?"

        bands = {}
        for b in sd.findall(".//calculatedconditions/band"):
            name = b.get("name","")
            time_of = b.get("time","")
            val = (b.text or "?").strip()
            if name:
                if name not in bands:
                    bands[name] = {}
                bands[name][time_of] = val

        result = {
            "ok":       True,
            "sfi":      gf("solarflux"),
            "sunspots": gf("sunspots"),
            "aindex":   gf("aindex"),
            "kindex":   gf("kindex"),
            "xray":     gf("xray"),
            "solarwind":gf("solarwind"),
            "updated":  gf("updated"),
            "bands":    bands,
        }
        _solar_cache      = result
        _solar_cache_time = _time.time()
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ─── Telnet Spot Helper ────────────────────────────────────────────────────────
def send_telnet_spot(callsign, freq_mhz, mode):
    try:
        freq_khz = int(float(freq_mhz) * 1000) if freq_mhz else 0
        spot = f"DX DE {MY_CALLSIGN}: {freq_khz} {callsign} {mode or ''}\r\n"
        with socket.create_connection((TELNET_SERVER, TELNET_PORT), timeout=5) as s:
            s.sendall(spot.encode())
    except Exception as e:
        print(f"Telnet spot failed: {e}")


# ─── DXCC Entity Lookup (cty.dat) ─────────────────────────────────────────────
# Parses the Big CTY file from country-files.com for callsign → DXCC entity mapping.
_dxcc_prefixes = {}   # prefix → {entity, cq, itu, cont, lat, lon}
_dxcc_exact    = {}   # exact callsign → entity info (= prefix entries)

def _load_cty_dat():
    """Parse cty.dat into prefix and exact-match lookup tables."""
    global _dxcc_prefixes, _dxcc_exact
    cty_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cty.dat")
    if not os.path.exists(cty_path):
        print("DXCC: cty.dat not found — entity lookup disabled")
        return
    prefixes = {}
    exact = {}
    try:
        with open(cty_path, "r", errors="replace") as f:
            lines = f.readlines()
        i = 0
        while i < len(lines):
            line = lines[i].rstrip()
            if not line or line.startswith(" "):
                i += 1
                continue
            # Entity header line
            parts = line.split(":")
            if len(parts) < 8:
                i += 1
                continue
            entity = parts[0].strip()
            cq_zone = parts[1].strip()
            itu_zone = parts[2].strip()
            cont = parts[3].strip()
            lat = parts[4].strip()
            lon = parts[5].strip()
            info = {"entity": entity, "cq": cq_zone, "itu": itu_zone,
                    "cont": cont, "lat": lat, "lon": lon}
            # Read prefix lines until semicolon
            i += 1
            prefix_text = ""
            while i < len(lines):
                pline = lines[i].strip()
                prefix_text += pline
                i += 1
                if pline.endswith(";"):
                    break
            # Parse prefixes
            for token in prefix_text.rstrip(";").split(","):
                token = token.strip()
                if not token:
                    continue
                # Strip override markers like (xx) [xx] /x
                clean = re.sub(r'\([^)]*\)|\[[^\]]*\]', '', token).strip()
                # Remove trailing /x modifiers
                clean = re.split(r'/', clean)[0] if '/' in clean and not clean.startswith('=') else clean
                if clean.startswith("="):
                    # Exact callsign match
                    exact[clean[1:].upper()] = info
                else:
                    prefixes[clean.upper()] = info
        _dxcc_prefixes = prefixes
        _dxcc_exact = exact
        print(f"DXCC: loaded {len(prefixes)} prefixes + {len(exact)} exact calls from cty.dat")
    except Exception as e:
        print(f"DXCC: failed to parse cty.dat: {e}")

def _extract_prefix(callsign):
    """Extract CQ WPX-style prefix from a callsign.
    Returns prefix string (e.g. 'N8', 'VK3', '3DA0') or None."""
    call = callsign.upper().strip()
    if not call:
        return None
    # Split on '/' to handle portable suffixes and stroke calls
    parts = call.split("/")
    if len(parts) == 1:
        base = parts[0]
    elif len(parts) == 2:
        # Strip trailing portable suffixes like /P /M /QRP /MM /AM
        if len(parts[1]) <= 3 and parts[1] in ("P", "M", "QRP", "MM", "AM", "A", "B"):
            base = parts[0]
        else:
            # Stroke call: use the shorter part as the prefix source
            # VK9/N8SDR -> VK9 is prefix; N8SDR/VK9 -> VK9 is prefix
            if len(parts[0]) < len(parts[1]):
                base = parts[0]
            else:
                base = parts[1]
    else:
        # Multiple slashes: strip last part if short suffix, use first part
        if len(parts[-1]) <= 3:
            base = parts[0]
        else:
            base = parts[0]
    # Extract prefix: letters and digits from start, ending after the LAST digit
    # in the leading alphanumeric sequence
    last_digit_pos = -1
    for i, ch in enumerate(base):
        if ch.isdigit():
            last_digit_pos = i
        elif ch.isalpha() and last_digit_pos >= 0:
            # We hit a letter after seeing a digit — prefix ends at last_digit_pos
            break
    if last_digit_pos >= 0:
        return base[:last_digit_pos + 1]
    # No digit found: convention is first letter + "0"
    if base:
        return base[0] + "0"
    return None

def dxcc_lookup(callsign):
    """Look up DXCC entity for a callsign. Returns info dict or None."""
    call = callsign.upper().strip()
    if not call:
        return None
    # Strip portable suffixes (W1ABC/P, W1ABC/QRP, W1ABC/M)
    base = call.split("/")[0] if "/" in call else call
    # Check exact match first
    if base in _dxcc_exact:
        return _dxcc_exact[base]
    if call in _dxcc_exact:
        return _dxcc_exact[call]
    # Longest prefix match
    best = None
    best_len = 0
    for pfx, info in _dxcc_prefixes.items():
        if base.startswith(pfx) and len(pfx) > best_len:
            best = info
            best_len = len(pfx)
    return best

@app.route("/api/dxcc_lookup")
def api_dxcc_lookup():
    call = request.args.get("call", "").strip().upper()
    if not call:
        return jsonify({"ok": False, "error": "No callsign"})
    info = dxcc_lookup(call)
    if info:
        return jsonify({"ok": True, **info})
    return jsonify({"ok": False, "error": "Not found"})


# ─── Worked DXCC / WAZ / WPX Cache ───────────────────────────────────────────
# In-memory cache: _worked_entities = {entity_name: set of (band_upper, mode_upper)}
# _worked_zones = {cq_zone_str: set of (band_upper, mode_upper)}
# _worked_prefixes = {prefix_str: set of (band_upper, mode_upper)}
# Rebuilt from DB on startup; updated incrementally when QSOs are saved/imported.
_worked_entities = {}        # {entity_str: set((band,mode), ...)}
_worked_zones    = {}        # {cq_zone: set((band,mode), ...)}
_worked_prefixes = {}        # {prefix_str: set((band,mode), ...)}
_worked_cache_lock = threading.Lock()

def _rebuild_worked_cache():
    """Scan qso_log and build dicts of worked DXCC entities, CQ zones, and WPX prefixes."""
    ent_cache = {}
    zone_cache = {}
    pfx_cache = {}
    try:
        conn = get_db()
        rows = conn.execute("SELECT callsign, band, mode FROM qso_log").fetchall()
        conn.close()
    except Exception:
        return
    for row in rows:
        call = str(row["callsign"]).strip().upper()
        bm = (str(row["band"]).strip().upper(), str(row["mode"]).strip().upper())
        info = dxcc_lookup(call)
        if info:
            ent_cache.setdefault(info["entity"], set()).add(bm)
            cq = str(info.get("cq", "")).strip()
            if cq:
                zone_cache.setdefault(cq, set()).add(bm)
        pfx = _extract_prefix(call)
        if pfx:
            pfx_cache.setdefault(pfx, set()).add(bm)
    with _worked_cache_lock:
        global _worked_entities, _worked_zones, _worked_prefixes
        _worked_entities = ent_cache
        _worked_zones = zone_cache
        _worked_prefixes = pfx_cache

def _worked_cache_add(callsign, band, mode):
    """Incrementally add a QSO to the worked cache (entities + zones + prefixes)."""
    bm = (str(band).strip().upper(), str(mode).strip().upper())
    info = dxcc_lookup(callsign)
    if info:
        with _worked_cache_lock:
            _worked_entities.setdefault(info["entity"], set()).add(bm)
            cq = str(info.get("cq", "")).strip()
            if cq:
                _worked_zones.setdefault(cq, set()).add(bm)
    pfx = _extract_prefix(callsign)
    if pfx:
        with _worked_cache_lock:
            _worked_prefixes.setdefault(pfx, set()).add(bm)


# ─── Worked Before ────────────────────────────────────────────────────────────
@app.route("/api/worked_before")
def api_worked_before():
    """Check if a DXCC entity has been worked before, optionally on a specific band/mode."""
    call = request.args.get("call", "").strip().upper()
    band = request.args.get("band", "").strip().upper()
    mode = request.args.get("mode", "").strip().upper()
    if not call:
        return jsonify({"ok": False})
    info = dxcc_lookup(call)
    if not info:
        return jsonify({"ok": True, "entity": None, "worked_entity": False, "worked_band_mode": False})
    entity = info["entity"]
    with _worked_cache_lock:
        band_modes = _worked_entities.get(entity, set())
        worked_entity = len(band_modes) > 0
        worked_band_mode = (band, mode) in band_modes if (band and mode) else False
    return jsonify({
        "ok": True, "entity": entity, "cont": info.get("cont", ""),
        "cq": info.get("cq", ""), "itu": info.get("itu", ""),
        "worked_entity": worked_entity,
        "worked_band_mode": worked_band_mode
    })


@app.route("/api/worked_before_batch", methods=["POST"])
def api_worked_before_batch():
    """Batch check worked-before for multiple callsigns (used by spot coloring).
    Input: {"spots": [{"call":"W1ABC","band":"20M","mode":"SSB"}, ...]}
    Output: {"results": {"W1ABC": {"entity":"..","status":"new_entity"|"new_band_mode"|"worked",
             "needs_zone":true|false, "cq":"05"}, ...}}
    """
    data = request.json or {}
    spots = data.get("spots", [])
    results = {}
    with _worked_cache_lock:
        for s in spots:
            call = s.get("call", "").strip().upper()
            if not call or call in results:
                continue
            band = s.get("band", "").strip().upper()
            mode = s.get("mode", "").strip().upper()
            info = dxcc_lookup(call)
            if not info:
                results[call] = {"entity": None, "status": "unknown", "needs_zone": False}
                continue
            entity = info["entity"]
            cq = str(info.get("cq", "")).strip()
            band_modes = _worked_entities.get(entity, set())
            # Check if this CQ zone has been worked at all
            zone_worked = len(_worked_zones.get(cq, set())) > 0 if cq else True
            # For finer-grained splitting (panadapter category coloring):
            # new_band_for_entity → this entity has never been worked on this band (any mode)
            # new_mode_for_entity → this entity has never been worked on this mode (any band)
            worked_bands_for_entity = {b for (b, _m) in band_modes}
            worked_modes_for_entity = {m for (_b, m) in band_modes}
            new_band_for_entity = bool(band) and band not in worked_bands_for_entity
            new_mode_for_entity = bool(mode) and mode not in worked_modes_for_entity
            if not band_modes:
                results[call] = {"entity": entity, "cont": info.get("cont", ""),
                                 "status": "new_entity", "needs_zone": not zone_worked, "cq": cq,
                                 "new_band_for_entity": True, "new_mode_for_entity": True}
            elif band and mode and (band, mode) not in band_modes:
                results[call] = {"entity": entity, "cont": info.get("cont", ""),
                                 "status": "new_band_mode", "needs_zone": not zone_worked, "cq": cq,
                                 "new_band_for_entity": new_band_for_entity,
                                 "new_mode_for_entity": new_mode_for_entity}
            else:
                results[call] = {"entity": entity, "cont": info.get("cont", ""),
                                 "status": "worked", "needs_zone": not zone_worked, "cq": cq,
                                 "new_band_for_entity": False, "new_mode_for_entity": False}
    return jsonify({"results": results})


# ─── Lightning Detection ────────────────────────────────────────────────────
# Three sources: Blitzortung.org (websocket), NOAA/NWS (REST), Ambient Weather (REST)

import math as _math

def _grid_to_latlon(grid):
    """Convert Maidenhead grid square to (lat, lon) tuple."""
    if not grid or len(grid) < 4:
        return None, None
    grid = grid.upper()
    try:
        lon = (ord(grid[0]) - 65) * 20 - 180 + int(grid[2]) * 2
        lat = (ord(grid[1]) - 65) * 10 - 90 + int(grid[3])
        if len(grid) >= 6:
            lon += (ord(grid[4]) - 65) * (2 / 24) + (1 / 24)
            lat += (ord(grid[5]) - 65) * (1 / 24) + (1 / 48)
        else:
            lon += 1
            lat += 0.5
        return lat, lon
    except Exception:
        return None, None

def _haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km between two lat/lon points."""
    R = 6371.0
    to_r = _math.radians
    d_lat = to_r(lat2 - lat1)
    d_lon = to_r(lon2 - lon1)
    a = _math.sin(d_lat / 2) ** 2 + _math.cos(to_r(lat1)) * _math.cos(to_r(lat2)) * _math.sin(d_lon / 2) ** 2
    return R * 2 * _math.atan2(_math.sqrt(a), _math.sqrt(1 - a))

def _bearing_deg(lat1, lon1, lat2, lon2):
    """Initial bearing in degrees from point 1 to point 2."""
    to_r = _math.radians
    d_lon = to_r(lon2 - lon1)
    y = _math.sin(d_lon) * _math.cos(to_r(lat2))
    x = _math.cos(to_r(lat1)) * _math.sin(to_r(lat2)) - _math.sin(to_r(lat1)) * _math.cos(to_r(lat2)) * _math.cos(d_lon)
    return (_math.degrees(_math.atan2(y, x)) + 360) % 360

def _bearing_to_compass(deg):
    """Convert bearing degrees to compass direction string."""
    dirs = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
            "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    return dirs[int((deg + 11.25) / 22.5) % 16]

def _fetch_blitzortung(my_lat, my_lon, range_km):
    """Fetch recent strikes from Blitzortung.org via their public JSON API.
    Returns list of (distance_km, bearing_deg) for strikes within range.
    Uses the getjson.php endpoint which returns flat arrays:
      [lon, lat, timestamp_str, ?, ?, ?, ?]
    Regions: 07=Central Americas, 12=East Americas, 13=West Americas."""
    strikes = []
    # Regions covering the Americas — fetch all three to get full coverage
    regions = [7, 12, 13]
    headers = {
        "Referer": "https://map.blitzortung.org/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SDRLogger+"
    }
    for region in regions:
        try:
            url = f"https://map.blitzortung.org/GEOjson/getjson.php?f=s&n={region:02d}"
            resp = requests.get(url, timeout=8, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                # Response is a flat list of arrays: [[lon, lat, ts, ...], ...]
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, list) and len(item) >= 2:
                            try:
                                s_lon = float(item[0])
                                s_lat = float(item[1])
                            except (ValueError, TypeError):
                                continue
                            dist = _haversine_km(my_lat, my_lon, s_lat, s_lon)
                            if dist <= range_km:
                                brg = _bearing_deg(my_lat, my_lon, s_lat, s_lon)
                                strikes.append((dist, brg))
                    print(f"Lightning: Blitzortung region {region:02d} — {len(data)} strikes fetched")
            else:
                print(f"Lightning: Blitzortung region {region:02d} HTTP {resp.status_code}")
        except Exception as e:
            print(f"Lightning: Blitzortung region {region:02d} error: {e}")
    if strikes:
        closest = min(strikes, key=lambda x: x[0])
        print(f"Lightning: Blitzortung total {len(strikes)} strikes in range, closest {closest[0]:.1f} km @ {closest[1]:.0f}°")
    else:
        print(f"Lightning: Blitzortung — no strikes within {range_km:.0f} km")
    return strikes

def _fetch_noaa_warnings(my_lat, my_lon):
    """Fetch active severe thunderstorm warnings from NOAA/NWS for user's location."""
    warning = ""
    try:
        url = f"https://api.weather.gov/alerts/active?point={my_lat:.4f},{my_lon:.4f}"
        print(f"Lightning: NOAA checking {my_lat:.4f},{my_lon:.4f}")
        resp = requests.get(url, timeout=10, headers={"User-Agent": "SDRLogger+ Ham Radio Logger (contact: n8sdr@arrl.net)"})
        if resp.status_code == 200:
            data = resp.json()
            for feat in data.get("features", []):
                props = feat.get("properties", {})
                event = props.get("event", "")
                if "thunderstorm" in event.lower() or "lightning" in event.lower():
                    warning = event
                    break
    except Exception as e:
        print(f"Lightning: NOAA error: {e}")
    return warning

def _fetch_ambient_weather():
    """Fetch lightning data from Ambient Weather station REST API.
    Returns (distance_km, strikes_per_hour) or (None, 0)."""
    if not LIGHTNING_AMBIENT_API_KEY or not LIGHTNING_AMBIENT_APP_KEY:
        print("Lightning: Ambient Weather — API keys not configured")
        return None, 0
    try:
        url = (f"https://rt.ambientweather.net/v1/devices"
               f"?apiKey={LIGHTNING_AMBIENT_API_KEY}"
               f"&applicationKey={LIGHTNING_AMBIENT_APP_KEY}")
        print("Lightning: Ambient Weather polling...")
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            devices = resp.json()
            if devices and isinstance(devices, list):
                last = devices[0].get("lastData", {})
                dist_mi = last.get("lightning_distance")
                hour_count = last.get("lightning_hour", 0)
                print(f"Lightning: Ambient Weather data — lightning_distance={dist_mi}, lightning_hour={hour_count}")
                if dist_mi is not None and hour_count and hour_count > 0:
                    dist_km = float(dist_mi) * 1.60934
                    return dist_km, int(hour_count)
        elif resp.status_code == 429:
            print("Lightning: Ambient Weather rate limited")
    except Exception as e:
        print(f"Lightning: Ambient Weather error: {e}")
    return None, 0

def lightning_thread():
    """Background thread: poll lightning sources every 60 seconds."""
    import time
    time.sleep(10)  # initial delay to let settings load
    while True:
        try:
            if not LIGHTNING_ENABLED:
                with _lightning_lock:
                    _lightning_status["active"] = False
                time.sleep(30)
                continue
            grid = runtime_settings.get("grid", "")
            my_lat, my_lon = _grid_to_latlon(grid)
            if my_lat is None:
                print(f"Lightning: no valid grid square configured (grid='{grid}') — skipping")
                time.sleep(30)
                continue
            range_km = LIGHTNING_RANGE * 1.60934 if LIGHTNING_UNIT == "mi" else float(LIGHTNING_RANGE)
            print(f"Lightning: polling — grid={grid} lat={my_lat:.2f} lon={my_lon:.2f} range={range_km:.0f}km")
            closest_km = None
            closest_brg = None
            total_strikes = 0
            sources = []
            noaa_warn = ""
            # Source 1: Blitzortung
            if LIGHTNING_BLITZORTUNG:
                strikes = _fetch_blitzortung(my_lat, my_lon, range_km)
                if strikes:
                    sources.append("blitzortung")
                    total_strikes += len(strikes)
                    for dist, brg in strikes:
                        if closest_km is None or dist < closest_km:
                            closest_km = dist
                            closest_brg = brg
            # Source 2: NOAA/NWS
            if LIGHTNING_NOAA:
                noaa_warn = _fetch_noaa_warnings(my_lat, my_lon)
                if noaa_warn:
                    sources.append("noaa")
            # Source 3: Ambient Weather
            if LIGHTNING_AMBIENT:
                amb_dist, amb_count = _fetch_ambient_weather()
                if amb_dist is not None and amb_count > 0:
                    sources.append("ambient")
                    total_strikes += amb_count
                    if closest_km is None or amb_dist < closest_km:
                        closest_km = amb_dist
                        # Ambient Weather doesn't provide bearing
            direction = _bearing_to_compass(closest_brg) if closest_brg is not None else ""
            with _lightning_lock:
                _lightning_status["active"] = (closest_km is not None and closest_km <= range_km) or bool(noaa_warn)
                _lightning_status["closest_km"] = round(closest_km, 1) if closest_km is not None else None
                _lightning_status["closest_mi"] = round(closest_km / 1.60934, 1) if closest_km is not None else None
                _lightning_status["direction"] = direction
                _lightning_status["strikes_1hr"] = total_strikes
                _lightning_status["sources"] = sources
                _lightning_status["noaa_warning"] = noaa_warn
                _lightning_status["last_update"] = datetime.utcnow().strftime("%H:%M:%S")
        except Exception as e:
            print(f"Lightning thread error: {e}")
        time.sleep(90)

@app.route("/api/lightning_status")
def lightning_status():
    """Return current lightning detection status for the banner."""
    with _lightning_lock:
        return jsonify(dict(_lightning_status))


# ─── High-Wind Alerts (v1.08.2-beta) ─────────────────────────────────────────
# Three sources:
#  1. NWS active-alerts API (reuses the lightning NWS endpoint, filters for
#     wind products: High Wind Warning, Wind Advisory, High Wind Watch,
#     Extreme Wind Warning).
#  2. NWS nearest-METAR observation — api.weather.gov/stations/{ICAO}/observations/latest.
#     User picks their nearest METAR station (e.g. KLUK for Cincinnati-Lunken).
#  3. Ambient Weather station — reuses the LIGHTNING_AMBIENT_*_KEY credentials.
#
# Severity tiers:
#  - "elevated" (yellow): sustained >= WIND_THRESH_SUST-10 OR gust >= WIND_THRESH_GUST-10
#  - "high"     (orange): sustained >= WIND_THRESH_SUST OR gust >= WIND_THRESH_GUST
#  - "extreme"  (red):    NWS High Wind Warning OR Extreme Wind Warning OR
#                         sustained >= WIND_THRESH_SUST+15 OR gust >= WIND_THRESH_GUST+15

WIND_PRODUCTS = (
    "high wind warning",
    "high wind watch",
    "wind advisory",
    "extreme wind warning",
)

_wind_last_clear = 0.0   # monotonic timestamp of last "below threshold" observation

def _fetch_nws_wind_alerts(my_lat, my_lon):
    """Return (headline_str, is_extreme) from NWS active-alerts for our point.
    headline_str is '' when no wind-related alert is active."""
    try:
        url = f"https://api.weather.gov/alerts/active?point={my_lat:.4f},{my_lon:.4f}"
        resp = requests.get(url, timeout=10,
            headers={"User-Agent": "SDRLogger+ Ham Radio Logger (contact: n8sdr@arrl.net)"})
        if resp.status_code == 200:
            data = resp.json()
            for feat in data.get("features", []):
                event = (feat.get("properties", {}).get("event", "") or "").lower()
                for prod in WIND_PRODUCTS:
                    if prod in event:
                        is_extreme = ("high wind warning" in event or "extreme wind" in event)
                        return feat["properties"]["event"], is_extreme
    except Exception as e:
        print(f"Wind: NWS alerts error: {e}")
    return "", False


def _fetch_nws_metar(station):
    """Return (sustained_mph, gust_mph, dir_deg) from the latest METAR obs.
    Any field can be None when not reported."""
    if not station or len(station) < 3:
        return None, None, None
    try:
        url = f"https://api.weather.gov/stations/{station}/observations/latest"
        resp = requests.get(url, timeout=10,
            headers={"User-Agent": "SDRLogger+ Ham Radio Logger (contact: n8sdr@arrl.net)"})
        if resp.status_code != 200:
            print(f"Wind: METAR {station} HTTP {resp.status_code}")
            return None, None, None
        props = resp.json().get("properties", {}) or {}
        # NWS reports speeds in km/h (value + unitCode wmoUnit:km_h-1)
        def _mph(field):
            f = props.get(field) or {}
            v = f.get("value")
            if v is None:
                return None
            uc = f.get("unitCode", "")
            if "km_h" in uc or "kmh" in uc:
                return float(v) * 0.621371
            if "m_s" in uc:
                return float(v) * 2.23694
            return float(v)   # assume mph as last resort
        sust = _mph("windSpeed")
        gust = _mph("windGust")
        d    = (props.get("windDirection") or {}).get("value")
        print(f"Wind: METAR {station} — sust={sust} gust={gust} dir={d}")
        return sust, gust, d
    except Exception as e:
        print(f"Wind: METAR {station} error: {e}")
        return None, None, None


def _fetch_ambient_wind():
    """Return (sustained_mph, gust_mph, dir_deg) from Ambient Weather station.
    Reuses the LIGHTNING_AMBIENT_*_KEY credentials (same account/station)."""
    if not LIGHTNING_AMBIENT_API_KEY or not LIGHTNING_AMBIENT_APP_KEY:
        return None, None, None
    try:
        url = (f"https://rt.ambientweather.net/v1/devices"
               f"?apiKey={LIGHTNING_AMBIENT_API_KEY}"
               f"&applicationKey={LIGHTNING_AMBIENT_APP_KEY}")
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            devices = resp.json()
            if devices and isinstance(devices, list):
                last = devices[0].get("lastData", {}) or {}
                sust = last.get("windspeedmph")
                gust = last.get("windgustmph")
                d    = last.get("winddir")
                print(f"Wind: Ambient — sust={sust} gust={gust} dir={d}")
                return (float(sust) if sust is not None else None,
                        float(gust) if gust is not None else None,
                        float(d)    if d    is not None else None)
    except Exception as e:
        print(f"Wind: Ambient error: {e}")
    return None, None, None


def _wind_severity(sust, gust, nws_event, nws_is_extreme):
    """Classify into "" | "elevated" | "high" | "extreme"."""
    if nws_is_extreme:
        return "extreme"
    # Numeric thresholds (use whichever field is present)
    s = sust or 0.0
    g = gust or 0.0
    if s >= WIND_THRESH_SUST + 15 or g >= WIND_THRESH_GUST + 15:
        return "extreme"
    if s >= WIND_THRESH_SUST or g >= WIND_THRESH_GUST:
        return "high"
    if nws_event:   # advisory / watch without extreme flag → "high"
        return "high"
    if s >= max(10, WIND_THRESH_SUST - 10) or g >= max(15, WIND_THRESH_GUST - 10):
        return "elevated"
    return ""


def wind_thread():
    """Background poller (v1.08.2-beta): NWS alerts + METAR + Ambient, every 2 min."""
    import time
    global _wind_last_clear
    time.sleep(15)
    while True:
        try:
            if not WIND_ENABLED:
                with _wind_lock:
                    _wind_status["active"] = False
                    _wind_status["severity"] = ""
                time.sleep(30)
                continue

            grid = runtime_settings.get("grid", "")
            my_lat, my_lon = _grid_to_latlon(grid)
            if my_lat is None and (WIND_NWS_ALERTS or WIND_NWS_METAR):
                print("Wind: no grid configured — NWS sources need lat/lon")

            best_sust = None
            best_gust = None
            best_dir  = None
            sources   = []
            nws_event = ""
            nws_is_extreme = False

            if WIND_NWS_ALERTS and my_lat is not None:
                nws_event, nws_is_extreme = _fetch_nws_wind_alerts(my_lat, my_lon)
                if nws_event:
                    sources.append("nws_alert")

            if WIND_NWS_METAR and WIND_METAR_STATION:
                s, g, d = _fetch_nws_metar(WIND_METAR_STATION)
                if s is not None or g is not None:
                    sources.append(f"metar/{WIND_METAR_STATION}")
                    if s is not None and (best_sust is None or s > best_sust):
                        best_sust, best_dir = s, d
                    if g is not None and (best_gust is None or g > best_gust):
                        best_gust = g

            if WIND_AMBIENT:
                s, g, d = _fetch_ambient_wind()
                if s is not None or g is not None:
                    sources.append("ambient")
                    if s is not None and (best_sust is None or s > best_sust):
                        best_sust, best_dir = s, d
                    if g is not None and (best_gust is None or g > best_gust):
                        best_gust = g

            severity = _wind_severity(best_sust, best_gust, nws_event, nws_is_extreme)

            # Cooldown: once conditions clear, suppress re-alerting for WIND_COOLDOWN_MIN
            now = time.monotonic()
            if severity == "":
                _wind_last_clear = now
            else:
                # If we recently cleared and the new severity is just "elevated", hold off.
                if (now - _wind_last_clear) < (WIND_COOLDOWN_MIN * 60) and severity == "elevated":
                    severity = ""

            direction = _bearing_to_compass(best_dir) if best_dir is not None else ""
            with _wind_lock:
                _wind_status["active"]         = severity != ""
                _wind_status["severity"]       = severity
                _wind_status["sustained_mph"]  = round(best_sust, 1) if best_sust is not None else None
                _wind_status["gust_mph"]       = round(best_gust, 1) if best_gust is not None else None
                _wind_status["direction"]      = direction
                _wind_status["sources"]        = sources
                _wind_status["nws_alert"]      = nws_event
                _wind_status["last_update"]    = datetime.utcnow().strftime("%H:%M:%S")
        except Exception as e:
            print(f"Wind thread error: {e}")
        time.sleep(120)   # 2 min — wind evolves slower than lightning


threading.Thread(target=wind_thread, daemon=True, name="WindThread").start()


@app.route("/api/wind_status")
def wind_status():
    """Return current high-wind alert status for the banner."""
    with _wind_lock:
        return jsonify(dict(_wind_status))


# ─── ADIF File Monitor ────────────────────────────────────────────────────────
# Watches up to 2 external ADIF files (e.g. VarAC, MSHV) for new QSOs.
# Uses byte-offset tracking — only reads bytes appended since last check.
# State persisted in adif_monitor_state.json alongside the database.

ADIF_MONITOR_STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "adif_monitor_state.json")
_adif_monitor_events = collections.deque(maxlen=50)  # recent imports for UI toasts

# Pending ADIF records — monitor detects new QSOs but holds them until user confirms
_adif_monitor_pending = []       # list of {source, qsos, filepath, new_offset}
_adif_monitor_pending_lock = threading.Lock()

def _load_adif_monitor_state():
    try:
        with open(ADIF_MONITOR_STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_adif_monitor_state(state):
    try:
        with open(ADIF_MONITOR_STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        print(f"ADIF Monitor: failed to save state: {e}")

def _parse_adif_records(text):
    """Parse ADIF text into a list of dicts. Handles standard ADIF tag format.
    Strips ADIF header (before <EOH>) and only keeps recognised QSO fields
    to avoid extra fields from apps like MSHV (STATION_CALLSIGN,
    MY_GRIDSQUARE, DISTANCE, QSO_DATE_OFF, TIME_OFF) bloating records."""
    # Strip header if present (everything before <EOH>)
    eoh_match = re.search(r'<EOH>', text, flags=re.IGNORECASE)
    if eoh_match:
        text = text[eoh_match.end():]
    # Fields we actually use — anything else is discarded
    _KNOWN_FIELDS = {
        "CALL", "NAME", "QTH", "GRIDSQUARE", "QSO_DATE", "TIME_ON",
        "BAND", "MODE", "FREQ", "RST_SENT", "RST_RCVD",
        "COMMENT", "NOTES", "CONTEST_ID", "MY_POTA_REF", "POTA_REF",
        "STATE", "COUNTRY", "CNTY",
        "PROP_MODE", "SAT_NAME", "SAT_MODE", "BAND_RX", "FREQ_RX",
    }
    records = []
    parts = re.split(r'<eor>', text, flags=re.IGNORECASE)
    tag_re = re.compile(r'<(\w+):(\d+)(?::[^>]*)?>([^<]*)', re.IGNORECASE)
    for part in parts:
        fields = {}
        for m in tag_re.finditer(part):
            name = m.group(1).upper()
            if name not in _KNOWN_FIELDS:
                continue
            length = int(m.group(2))
            value = m.group(3)[:length]
            fields[name] = value.strip()
        if fields.get("CALL"):
            records.append(fields)
    return records

def _adif_to_qso(rec):
    """Map ADIF field names to our database columns."""
    call = rec.get("CALL", "").upper()
    if not call:
        return None
    # Frequency: ADIF uses MHz
    freq = ""
    if rec.get("FREQ"):
        try: freq = str(float(rec["FREQ"]))
        except: freq = rec["FREQ"]
    # Date: ADIF uses YYYYMMDD
    dt = rec.get("QSO_DATE", "")
    if len(dt) == 8:
        dt = f"{dt[:4]}-{dt[4:6]}-{dt[6:8]}"
    elif not dt:
        dt = date.today().isoformat()
    # Time: ADIF uses HHMMSS or HHMM
    tm = rec.get("TIME_ON", rec.get("TIME_OFF", ""))
    if len(tm) >= 4:
        tm = f"{tm[:2]}:{tm[2:4]}:{tm[4:6]}" if len(tm) >= 6 else f"{tm[:2]}:{tm[2:4]}:00"
    elif not tm:
        tm = datetime.utcnow().strftime("%H:%M:%S")
    # State: try STATE field, then extract from CNTY (format "OH,Butler")
    state = rec.get("STATE", "")
    if not state:
        cnty = rec.get("CNTY", "")
        if cnty and "," in cnty:
            state = cnty.split(",")[0].strip()
    return {
        "callsign": call,
        "name": rec.get("NAME", ""),
        "qth": rec.get("QTH", rec.get("GRIDSQUARE", "")),
        "date_worked": dt,
        "time_worked": tm,
        "band": rec.get("BAND", ""),
        "mode": rec.get("MODE", ""),
        "freq_mhz": freq,
        "my_rst_sent": rec.get("RST_SENT", "59"),
        "their_rst_rcvd": rec.get("RST_RCVD", "59"),
        "remarks": rec.get("COMMENT", rec.get("NOTES", "")),
        "contest_name": rec.get("CONTEST_ID", ""),
        "pota_ref": rec.get("MY_POTA_REF", ""),
        "pota_p2p": rec.get("POTA_REF", ""),
        "state": state,
        "country": rec.get("COUNTRY", ""),
        "gridsquare": rec.get("GRIDSQUARE", ""),
        "prop_mode": rec.get("PROP_MODE", ""),
        "sat_name": rec.get("SAT_NAME", ""),
    }

def _adif_monitor_insert(qso):
    """Insert a QSO from ADIF monitor into the active database."""
    try:
        conn = get_db()
        conn.execute("""
            INSERT INTO qso_log
                (callsign, name, qth, date_worked, time_worked, band, mode,
                 freq_mhz, my_rst_sent, their_rst_rcvd, remarks, contest_name,
                 pota_ref, pota_p2p, state, country, gridsquare,
                 prop_mode, sat_name)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            qso["callsign"], qso["name"], qso["qth"],
            qso["date_worked"], qso["time_worked"],
            qso["band"], qso["mode"],
            float(qso["freq_mhz"]) if qso["freq_mhz"] else None,
            qso["my_rst_sent"], qso["their_rst_rcvd"],
            qso["remarks"], qso["contest_name"],
            qso["pota_ref"], qso["pota_p2p"],
            qso.get("state", ""), qso.get("country", ""), qso.get("gridsquare", ""),
            qso.get("prop_mode", ""), qso.get("sat_name", ""),
        ))
        conn.commit()
        conn.close()
        # Update worked DXCC cache
        _worked_cache_add(qso["callsign"], qso["band"], qso["mode"])
        return True
    except Exception as e:
        print(f"ADIF Monitor: insert failed: {e}")
        return False

def adif_monitor_thread():
    """Background thread: poll configured ADIF files every 5 seconds.
    Detected QSOs are queued as pending — user must confirm before import."""
    import time
    while True:
        try:
            # Skip scan if there are already pending records awaiting confirmation
            with _adif_monitor_pending_lock:
                has_pending = len(_adif_monitor_pending) > 0
            if has_pending:
                time.sleep(5)
                continue
            s = runtime_settings
            slots = []
            for i in [1, 2]:
                path = s.get(f"adif_mon_path_{i}", "").strip()
                enabled = s.get(f"adif_mon_enabled_{i}", False)
                label = s.get(f"adif_mon_label_{i}", f"Slot {i}")
                if path and enabled:
                    slots.append((i, path, label))
            if slots:
                state = _load_adif_monitor_state()
                for idx, filepath, label in slots:
                    key = filepath.replace("\\", "/")
                    try:
                        fsize = os.path.getsize(filepath)
                    except OSError:
                        continue
                    offset = state.get(key, 0)
                    if fsize < offset:
                        offset = 0
                    if fsize <= offset:
                        continue
                    try:
                        with open(filepath, "r", errors="replace") as f:
                            f.seek(offset)
                            new_data = f.read()
                            new_offset = f.tell()
                    except Exception as e:
                        print(f"ADIF Monitor [{label}]: read error: {e}")
                        continue
                    records = _parse_adif_records(new_data)
                    qsos = []
                    for rec in records:
                        qso = _adif_to_qso(rec)
                        if qso:
                            qsos.append(qso)
                    if qsos:
                        with _adif_monitor_pending_lock:
                            _adif_monitor_pending.append({
                                "source": label,
                                "qsos": qsos,
                                "filepath": key,
                                "new_offset": new_offset,
                            })
                        print(f"ADIF Monitor [{label}]: {len(qsos)} QSO(s) pending confirmation")
        except Exception as e:
            print(f"ADIF Monitor error: {e}")
        time.sleep(5)

@app.route("/api/adif_monitor_events")
def adif_monitor_events():
    """Return recent ADIF monitor import events for UI toast notifications."""
    events = list(_adif_monitor_events)
    _adif_monitor_events.clear()
    return jsonify(events)

@app.route("/api/adif_monitor_pending")
def adif_monitor_pending():
    """Return pending ADIF records awaiting user confirmation."""
    with _adif_monitor_pending_lock:
        pending = []
        for batch in _adif_monitor_pending:
            pending.append({
                "source": batch["source"],
                "count": len(batch["qsos"]),
                "calls": [q["callsign"] for q in batch["qsos"]],
            })
    return jsonify(pending)

@app.route("/api/adif_monitor_confirm", methods=["POST"])
def adif_monitor_confirm():
    """Import all pending ADIF records and advance file offsets."""
    with _adif_monitor_pending_lock:
        batches = list(_adif_monitor_pending)
        _adif_monitor_pending.clear()
    imported = 0
    state = _load_adif_monitor_state()
    for batch in batches:
        for qso in batch["qsos"]:
            if _adif_monitor_insert(qso):
                evt = {"callsign": qso["callsign"], "mode": qso["mode"],
                       "freq": qso["freq_mhz"], "source": batch["source"],
                       "time": datetime.utcnow().strftime("%H:%M:%S")}
                _adif_monitor_events.append(evt)
                imported += 1
        state[batch["filepath"]] = batch["new_offset"]
    _save_adif_monitor_state(state)
    return jsonify({"ok": True, "imported": imported})

@app.route("/api/adif_monitor_dismiss", methods=["POST"])
def adif_monitor_dismiss():
    """Discard pending ADIF records but still advance file offsets (so they aren't re-detected)."""
    with _adif_monitor_pending_lock:
        batches = list(_adif_monitor_pending)
        _adif_monitor_pending.clear()
    state = _load_adif_monitor_state()
    for batch in batches:
        state[batch["filepath"]] = batch["new_offset"]
    _save_adif_monitor_state(state)
    return jsonify({"ok": True, "dismissed": sum(len(b["qsos"]) for b in batches)})


# ─── Awards & Statistics ──────────────────────────────────────────────────────

# US state abbreviations for WAS tracking
_US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
}
_US_STATE_NAMES = {
    "ALABAMA":"AL","ALASKA":"AK","ARIZONA":"AZ","ARKANSAS":"AR",
    "CALIFORNIA":"CA","COLORADO":"CO","CONNECTICUT":"CT","DELAWARE":"DE",
    "FLORIDA":"FL","GEORGIA":"GA","HAWAII":"HI","IDAHO":"ID",
    "ILLINOIS":"IL","INDIANA":"IN","IOWA":"IA","KANSAS":"KS",
    "KENTUCKY":"KY","LOUISIANA":"LA","MAINE":"ME","MARYLAND":"MD",
    "MASSACHUSETTS":"MA","MICHIGAN":"MI","MINNESOTA":"MN","MISSISSIPPI":"MS",
    "MISSOURI":"MO","MONTANA":"MT","NEBRASKA":"NE","NEVADA":"NV",
    "NEW HAMPSHIRE":"NH","NEW JERSEY":"NJ","NEW MEXICO":"NM","NEW YORK":"NY",
    "NORTH CAROLINA":"NC","NORTH DAKOTA":"ND","OHIO":"OH","OKLAHOMA":"OK",
    "OREGON":"OR","PENNSYLVANIA":"PA","RHODE ISLAND":"RI","SOUTH CAROLINA":"SC",
    "SOUTH DAKOTA":"SD","TENNESSEE":"TN","TEXAS":"TX","UTAH":"UT",
    "VERMONT":"VT","VIRGINIA":"VA","WASHINGTON":"WA","WEST VIRGINIA":"WV",
    "WISCONSIN":"WI","WYOMING":"WY"
}

def _extract_us_state(qth_str):
    """Try to extract a US state abbreviation from a QTH string."""
    if not qth_str:
        return None
    qth = qth_str.strip().upper()
    # Direct 2-letter abbreviation match (e.g., "OH", "Columbus, OH", "OH, USA")
    for token in re.split(r'[,\s/]+', qth):
        token = token.strip()
        if token in _US_STATES:
            return token
    # Full state name match (e.g., "Ohio", "New York")
    for name, abbr in _US_STATE_NAMES.items():
        if name in qth:
            return abbr
    return None


@app.route("/awards")
def awards_page():
    return render_template("awards.html", version=VERSION)


@app.route("/api/awards/dxcc")
def api_awards_dxcc():
    """Return DXCC award progress — entities worked, with band/mode breakdown."""
    band_filter = request.args.get("band", "").strip().upper()
    mode_filter = request.args.get("mode", "").strip().upper()
    with _worked_cache_lock:
        entities = {}
        for entity, bm_set in _worked_entities.items():
            bands_worked = {}
            for (b, m) in bm_set:
                if band_filter and b != band_filter:
                    continue
                if mode_filter and m != mode_filter:
                    continue
                bands_worked.setdefault(b, []).append(m)
            if bands_worked:
                entities[entity] = {
                    "bands": bands_worked,
                    "count": len(bands_worked)
                }
    # Get continent for each entity from prefix table
    entity_info = {}
    seen = set()
    for pfx, info in _dxcc_prefixes.items():
        ent = info["entity"]
        if ent not in seen and ent in entities:
            seen.add(ent)
            entity_info[ent] = {"cont": info.get("cont", ""), "cq": info.get("cq", "")}
    return jsonify({
        "ok": True,
        "total_worked": len(entities),
        "entities": entities,
        "entity_info": entity_info
    })


@app.route("/api/awards/was")
def api_awards_was():
    """Return WAS (Worked All States) progress — states worked, with band/mode breakdown."""
    band_filter = request.args.get("band", "").strip().upper()
    mode_filter = request.args.get("mode", "").strip().upper()
    try:
        conn = get_db()
        rows = conn.execute("SELECT callsign, qth, band, mode, state, country FROM qso_log").fetchall()
        conn.close()
    except Exception as e:
        print(f"Awards WAS: DB error: {e}")
        return jsonify({"ok": False, "error": "Database error"})

    states = {}   # {state_abbr: {"bands": {band: [modes]}, "calls": set()}}
    for row in rows:
        call = str(row["callsign"]).strip().upper()
        # Try the state column first (populated from ADIF STATE field)
        state = None
        raw_state = _safe_col(row, "state").strip().upper()
        if raw_state and raw_state in _US_STATES:
            state = raw_state
        elif raw_state and raw_state in _US_STATE_NAMES:
            state = _US_STATE_NAMES[raw_state]
        # Fallback: extract from QTH field
        if not state:
            state = _extract_us_state(_safe_col(row, "qth"))
        # Last resort: only for US callsigns, try to find state from QTH
        if not state:
            info = dxcc_lookup(call)
            if not info or info["entity"] not in ("United States", "Alaska", "Hawaii"):
                continue
            # Alaska and Hawaii are DXCC entities — auto-map them
            if info["entity"] == "Alaska":
                state = "AK"
            elif info["entity"] == "Hawaii":
                state = "HI"
            else:
                continue  # US callsign but no state info available
        else:
            # Verify it's a US callsign if state came from QTH (could be wrong)
            info = dxcc_lookup(call)
            if not info or info["entity"] not in ("United States", "Alaska", "Hawaii"):
                continue

        b = str(row["band"]).strip().upper()
        m = str(row["mode"]).strip().upper()
        if band_filter and b != band_filter:
            continue
        if mode_filter and m != mode_filter:
            continue
        if state not in states:
            states[state] = {"bands": {}, "calls": set()}
        states[state]["bands"].setdefault(b, []).append(m)
        states[state]["calls"].add(call)

    # Convert sets to lists for JSON
    out = {}
    for st, data in states.items():
        out[st] = {"bands": data["bands"], "calls": list(data["calls"])[:5]}

    print(f"Awards WAS: {len(out)} states found from {len(rows)} QSOs")
    return jsonify({
        "ok": True,
        "total_worked": len(out),
        "total_needed": 50,
        "states": out
    })


@app.route("/api/awards/waz")
def api_awards_waz():
    """Return WAZ (Worked All Zones) progress — CQ zones worked, with band/mode breakdown."""
    band_filter = request.args.get("band", "").strip().upper()
    mode_filter = request.args.get("mode", "").strip().upper()
    try:
        conn = get_db()
        rows = conn.execute("SELECT callsign, band, mode FROM qso_log").fetchall()
        conn.close()
    except Exception:
        return jsonify({"ok": False, "error": "Database error"})

    zones = {}  # {zone_num: {"bands": {band: [modes]}, "entities": set()}}
    for row in rows:
        call = str(row["callsign"]).strip().upper()
        info = dxcc_lookup(call)
        if not info or not info.get("cq"):
            continue
        cq = str(info["cq"]).strip()
        b = str(row["band"]).strip().upper()
        m = str(row["mode"]).strip().upper()
        if band_filter and b != band_filter:
            continue
        if mode_filter and m != mode_filter:
            continue
        if cq not in zones:
            zones[cq] = {"bands": {}, "entities": set()}
        zones[cq]["bands"].setdefault(b, []).append(m)
        zones[cq]["entities"].add(info["entity"])

    out = {}
    for z, data in zones.items():
        out[z] = {"bands": data["bands"], "entities": list(data["entities"])[:10]}

    return jsonify({
        "ok": True,
        "total_worked": len(out),
        "total_needed": 40,
        "zones": out
    })


@app.route("/api/awards/wpx")
def api_awards_wpx():
    """Return WPX (Worked All Prefixes) progress — prefixes worked, with band/mode breakdown."""
    band_filter = request.args.get("band", "").strip().upper()
    mode_filter = request.args.get("mode", "").strip().upper()
    try:
        conn = get_db()
        rows = conn.execute("SELECT callsign, band, mode FROM qso_log").fetchall()
        conn.close()
    except Exception:
        return jsonify({"ok": False, "error": "Database error"})

    prefixes = {}  # {prefix: {"bands": {band: [modes]}, "entities": set(), "calls": set()}}
    for row in rows:
        call = str(row["callsign"]).strip().upper()
        pfx = _extract_prefix(call)
        if not pfx:
            continue
        b = str(row["band"]).strip().upper()
        m = str(row["mode"]).strip().upper()
        if band_filter and b != band_filter:
            continue
        if mode_filter and m != mode_filter:
            continue
        if pfx not in prefixes:
            prefixes[pfx] = {"bands": {}, "entities": set(), "calls": set()}
        prefixes[pfx]["bands"].setdefault(b, []).append(m)
        prefixes[pfx]["calls"].add(call)
        info = dxcc_lookup(call)
        if info:
            prefixes[pfx]["entities"].add(info["entity"])

    out = {}
    for p, data in prefixes.items():
        out[p] = {
            "bands": data["bands"],
            "entities": list(data["entities"])[:5],
            "calls": list(data["calls"])[:5],
            "band_count": len(data["bands"])
        }

    return jsonify({
        "ok": True,
        "total_worked": len(out),
        "prefixes": out
    })


# ─── WAC — Worked All Continents (v1.09) ──────────────────────────────────────
# Six standard continents (ARRL/IARU WAC). Antarctica recognized as a 7th
# "bonus" endorsement — rendered separately so it doesn't gate the base award.
_WAC_CONTS  = ["NA", "SA", "EU", "AS", "AF", "OC"]
_WAC_EXTRAS = ["AN"]

_WAC_NAMES = {
    "NA": "North America", "SA": "South America", "EU": "Europe",
    "AS": "Asia",          "AF": "Africa",        "OC": "Oceania",
    "AN": "Antarctica",
}

@app.route("/api/awards/wac")
def api_awards_wac():
    """WAC: continents worked. 6 standard + Antarctica as bonus endorsement."""
    band_filter = request.args.get("band", "").strip().upper()
    mode_filter = request.args.get("mode", "").strip().upper()
    try:
        conn = get_db()
        rows = conn.execute("SELECT callsign, band, mode FROM qso_log").fetchall()
        conn.close()
    except Exception:
        return jsonify({"ok": False, "error": "Database error"})

    conts = {}  # {cont_code: {"bands": {band: [modes]}, "entities": set()}}
    for row in rows:
        call = str(row["callsign"] or "").strip().upper()
        if not call:
            continue
        info = dxcc_lookup(call)
        if not info or not info.get("cont"):
            continue
        c = str(info["cont"]).strip().upper()
        b = str(row["band"] or "").strip().upper()
        m = str(row["mode"] or "").strip().upper()
        if band_filter and b != band_filter:
            continue
        if mode_filter and m != mode_filter:
            continue
        if c not in conts:
            conts[c] = {"bands": {}, "entities": set()}
        conts[c]["bands"].setdefault(b, []).append(m)
        conts[c]["entities"].add(info["entity"])

    out = {}
    for c, data in conts.items():
        out[c] = {
            "bands": data["bands"],
            "entities": sorted(data["entities"])[:8],
            "name": _WAC_NAMES.get(c, c),
        }
    base_worked  = sum(1 for c in _WAC_CONTS if c in out)
    extra_worked = sum(1 for c in _WAC_EXTRAS if c in out)
    return jsonify({
        "ok": True,
        "base_cont_order":  _WAC_CONTS,
        "extra_cont_order": _WAC_EXTRAS,
        "names": _WAC_NAMES,
        "continents": out,
        "base_worked":  base_worked,
        "extra_worked": extra_worked,
        "achieved":     base_worked >= 6,
    })


# ─── VUCC / 5BWAS / 5BDXCC (v1.09) ───────────────────────────────────────────
# VHF/UHF Century Club: unique 4-character grid squares per band, VHF and up.
# ARRL thresholds vary per band (6m=100, 2m=100, 222=50, 432=50, 902=25, 1296=25,
# 2.3GHz+=10). We report per-band counts; the UI shows the threshold + bar.
_VUCC_BANDS = ["6M", "2M", "1.25M", "70CM", "33CM", "23CM", "13CM", "9CM", "6CM", "3CM"]
_VUCC_THRESHOLDS = {
    "6M": 100, "2M": 100, "1.25M": 50, "70CM": 50,
    "33CM": 25, "23CM": 25, "13CM": 10, "9CM": 10, "6CM": 10, "3CM": 10,
}

def _grid4(g):
    """Normalize a Maidenhead grid to its 4-char field+square (e.g. 'EN82bm' -> 'EN82')."""
    if not g:
        return ""
    g = str(g).strip().upper()
    if len(g) < 4:
        return ""
    a, b, c, d = g[0], g[1], g[2], g[3]
    if not (a.isalpha() and b.isalpha() and c.isdigit() and d.isdigit()):
        return ""
    return a + b + c + d


@app.route("/api/awards/vucc")
def api_awards_vucc():
    """VUCC: unique 4-char grids per band, VHF/UHF only."""
    mode_filter = request.args.get("mode", "").strip().upper()
    try:
        conn = get_db()
        rows = conn.execute("SELECT band, mode, gridsquare FROM qso_log").fetchall()
        conn.close()
    except Exception:
        return jsonify({"ok": False, "error": "Database error"})

    bands = {b: {"grids": set(), "threshold": _VUCC_THRESHOLDS[b]} for b in _VUCC_BANDS}
    for row in rows:
        b = str(row["band"] or "").strip().upper()
        if b not in bands:
            continue
        m = str(row["mode"] or "").strip().upper()
        if mode_filter and m != mode_filter:
            continue
        g = _grid4(_safe_col(row, "gridsquare"))
        if not g:
            continue
        bands[b]["grids"].add(g)

    out = {}
    for b, data in bands.items():
        gl = sorted(data["grids"])
        out[b] = {
            "count": len(gl),
            "threshold": data["threshold"],
            "achieved": len(gl) >= data["threshold"],
            "grids": gl,
        }
    return jsonify({"ok": True, "bands": out, "band_order": _VUCC_BANDS})


# 5-Band WAS — all 50 states on EACH of 80/40/20/15/10
_5B_BANDS = ["80M", "40M", "20M", "15M", "10M"]

@app.route("/api/awards/5bwas")
def api_awards_5bwas():
    """5-Band WAS: per-band count of US states worked on 80/40/20/15/10."""
    mode_filter = request.args.get("mode", "").strip().upper()
    try:
        conn = get_db()
        rows = conn.execute("SELECT callsign, qth, band, mode, state FROM qso_log").fetchall()
        conn.close()
    except Exception:
        return jsonify({"ok": False, "error": "Database error"})

    bands = {b: set() for b in _5B_BANDS}
    for row in rows:
        b = str(row["band"] or "").strip().upper()
        if b not in bands:
            continue
        m = str(row["mode"] or "").strip().upper()
        if mode_filter and m != mode_filter:
            continue
        call = str(row["callsign"] or "").strip().upper()
        # Reuse WAS state-resolution logic
        state = None
        raw_state = _safe_col(row, "state").strip().upper()
        if raw_state and raw_state in _US_STATES:
            state = raw_state
        elif raw_state and raw_state in _US_STATE_NAMES:
            state = _US_STATE_NAMES[raw_state]
        if not state:
            state = _extract_us_state(_safe_col(row, "qth"))
        info = dxcc_lookup(call) if call else None
        if state:
            if not info or info["entity"] not in ("United States", "Alaska", "Hawaii"):
                continue
        else:
            if not info:
                continue
            if info["entity"] == "Alaska":
                state = "AK"
            elif info["entity"] == "Hawaii":
                state = "HI"
            else:
                continue
        bands[b].add(state)

    out = {}
    all_states = set()
    for b in _5B_BANDS:
        sl = sorted(bands[b])
        all_states.update(sl)
        out[b] = {"count": len(sl), "states": sl, "achieved": len(sl) >= 50}
    achieved_5b = all(out[b]["achieved"] for b in _5B_BANDS)
    return jsonify({
        "ok": True,
        "bands": out,
        "band_order": _5B_BANDS,
        "achieved": achieved_5b,
        "union_count": len(all_states),
    })


@app.route("/api/awards/5bdxcc")
def api_awards_5bdxcc():
    """5-Band DXCC: per-band count of DXCC entities worked on 80/40/20/15/10."""
    mode_filter = request.args.get("mode", "").strip().upper()
    try:
        conn = get_db()
        rows = conn.execute("SELECT callsign, band, mode FROM qso_log").fetchall()
        conn.close()
    except Exception:
        return jsonify({"ok": False, "error": "Database error"})

    bands = {b: set() for b in _5B_BANDS}
    for row in rows:
        b = str(row["band"] or "").strip().upper()
        if b not in bands:
            continue
        m = str(row["mode"] or "").strip().upper()
        if mode_filter and m != mode_filter:
            continue
        call = str(row["callsign"] or "").strip().upper()
        if not call:
            continue
        info = dxcc_lookup(call)
        if not info:
            continue
        bands[b].add(info["entity"])

    out = {}
    union = set()
    for b in _5B_BANDS:
        el = sorted(bands[b])
        union.update(el)
        out[b] = {"count": len(el), "entities": el, "achieved": len(el) >= 100}
    achieved_5b = all(out[b]["achieved"] for b in _5B_BANDS)
    return jsonify({
        "ok": True,
        "bands": out,
        "band_order": _5B_BANDS,
        "achieved": achieved_5b,
        "union_count": len(union),
    })


# ─── Statistics Dashboard (v1.09) ────────────────────────────────────────────
@app.route("/stats")
def stats_page():
    return render_template("stats.html", version=VERSION)


@app.route("/api/stats")
def api_stats():
    """Aggregate QSO statistics for the dashboard.
    Query params:
      source = general | pota | combined  (default: combined)
      range  = all | year | 12mo | custom (default: all)
      from, to = ISO dates YYYY-MM-DD when range=custom
      mode   = optional mode filter (e.g. CW, SSB, FT8)
    """
    source = (request.args.get("source") or "combined").strip().lower()
    rng    = (request.args.get("range")  or "all").strip().lower()
    mode_f = (request.args.get("mode")   or "").strip().upper()
    d_from = (request.args.get("from")   or "").strip()
    d_to   = (request.args.get("to")     or "").strip()

    # Resolve date window
    today = datetime.utcnow().date()
    start_date = end_date = None
    if rng == "year":
        start_date = today.replace(month=1, day=1)
        end_date   = today
    elif rng == "12mo":
        end_date = today
        # roughly 365 days back
        start_date = today - timedelta(days=365)
    elif rng == "custom":
        try:
            if d_from: start_date = datetime.strptime(d_from, "%Y-%m-%d").date()
        except Exception: start_date = None
        try:
            if d_to:   end_date   = datetime.strptime(d_to,   "%Y-%m-%d").date()
        except Exception: end_date = None

    # Pick DBs
    db_paths = []
    if source == "general": db_paths = [DATABASE]
    elif source == "pota":  db_paths = [POTA_DATABASE]
    else:                   db_paths = [DATABASE, POTA_DATABASE]

    rows = []
    for p in db_paths:
        try:
            conn = get_db(p)
            rs = conn.execute(
                "SELECT date_worked, time_worked, band, mode, callsign FROM qso_log"
            ).fetchall()
            conn.close()
            for r in rs:
                rows.append(dict(r))
        except Exception:
            continue

    # Aggregations
    by_year   = {}   # "2024" -> count
    by_band   = {}
    by_mode   = {}
    by_hour   = [0]*24
    by_entity = {}
    cumulative_per_day = {}  # YYYY-MM-DD -> count

    total = 0
    for r in rows:
        d_raw = (r.get("date_worked") or "").strip()
        if not d_raw:
            continue
        # date_worked is typically YYYY-MM-DD
        try:
            dt = datetime.strptime(d_raw[:10], "%Y-%m-%d").date()
        except Exception:
            continue
        if start_date and dt < start_date: continue
        if end_date   and dt > end_date:   continue

        m = (r.get("mode") or "").strip().upper()
        if mode_f and m != mode_f: continue

        b = (r.get("band") or "").strip().upper()
        call = (r.get("callsign") or "").strip().upper()
        t = (r.get("time_worked") or "").strip()
        # Hour
        try:
            hr = int(t[:2]) if len(t) >= 2 else None
        except Exception:
            hr = None

        total += 1
        y = str(dt.year)
        by_year[y] = by_year.get(y, 0) + 1
        if b: by_band[b] = by_band.get(b, 0) + 1
        if m: by_mode[m] = by_mode.get(m, 0) + 1
        if hr is not None and 0 <= hr < 24: by_hour[hr] += 1

        if call:
            info = dxcc_lookup(call)
            if info and info.get("entity"):
                ent = info["entity"]
                by_entity[ent] = by_entity.get(ent, 0) + 1

        ds = dt.isoformat()
        cumulative_per_day[ds] = cumulative_per_day.get(ds, 0) + 1

    # Order years ascending
    year_keys = sorted(by_year.keys())
    years_out = [{"year": y, "count": by_year[y]} for y in year_keys]

    # Bands sorted by canonical wavelength order if known, else by count
    band_order = ["160M","80M","60M","40M","30M","20M","17M","15M","12M","10M","6M","2M","1.25M","70CM","33CM","23CM","13CM"]
    bands_out = []
    for b in band_order:
        if b in by_band: bands_out.append({"band": b, "count": by_band.pop(b)})
    for b, c in sorted(by_band.items(), key=lambda x: -x[1]):
        bands_out.append({"band": b, "count": c})

    modes_out = [{"mode": m, "count": c} for m, c in sorted(by_mode.items(), key=lambda x: -x[1])]
    top_entities = [{"entity": e, "count": c} for e, c in sorted(by_entity.items(), key=lambda x: -x[1])[:10]]

    # Cumulative timeline (sorted by date)
    cum_dates = sorted(cumulative_per_day.keys())
    running = 0
    cumulative_out = []
    for d in cum_dates:
        running += cumulative_per_day[d]
        cumulative_out.append({"date": d, "total": running})

    return jsonify({
        "ok": True,
        "source": source,
        "range": rng,
        "mode": mode_f,
        "from": start_date.isoformat() if start_date else None,
        "to":   end_date.isoformat()   if end_date   else None,
        "total": total,
        "years": years_out,
        "bands": bands_out,
        "modes": modes_out,
        "hours": by_hour,
        "top_entities": top_entities,
        "cumulative": cumulative_out,
    })


# ─── Feeds & Alerts (v1.09): Contests + DXpeditions ──────────────────────────
# WA7BNM Contest Calendar (iCal) + NG3K DXpedition announcements (RSS).
# Server fetches + caches for 30 min so we don't hammer their hobby sites.
_FEED_CACHE_TTL = 30 * 60  # seconds
_feed_cache = {
    "contests":     {"data": None, "fetched_at": 0, "error": None},
    "dxpeditions":  {"data": None, "fetched_at": 0, "error": None},
}
_feed_cache_lock = threading.Lock()

_WA7BNM_ICAL_URL = "https://www.contestcalendar.com/weeklycontcustom.php"
_NG3K_RSS_URL    = "http://www.ng3k.com/adxo.xml"

# Conservative ITU-style amateur callsign regex (with optional /suffix tokens).
# Avoids common false positives like QSL, LoTW, IOTA, OQRS, etc. via blocklist.
_CALL_RE = re.compile(r'\b[A-Z0-9]{1,3}\d[A-Z]{1,4}(?:/[A-Z0-9]{1,4})*\b')
_CALL_BLOCKLIST = {
    "QSL","LOTW","IOTA","OQRS","SOTA","WWFF","POTA","SSB","CW","FT8","FT4","RTTY",
    "JT65","JT9","PSK","DIGI","UTC","ADIF","CQDX","CQWW","ARRL","DXCC","WAS","WAZ",
    "WPX","HF","VHF","UHF","HAM","RIT","XIT","TX","RX","QSO","DX","PWR","ANT","NA",
    "SA","EU","AS","AF","OC","AN","USA","UK","SP","JA","ZL","VK","HQ","ID","OK",
    "EME","QRV","QRO","QRP","QRT","QTH","QSY","STN","STNS","OPS","OP","HRS","MIN",
    "QSOS","UTC","GMT","LOC","LOG","NIL",
}

# Band designators (e.g. 10M, 12M, 160M, 70CM, 23CM, 6M, 2M)
_BAND_RE = re.compile(r'^\d{1,3}(M|CM|MM)$')
# Power designators (e.g. 50W, 100W, 400W, 1KW, 5W)
_POWER_RE = re.compile(r'^\d{1,4}(W|KW|MW)$')
# Maidenhead grid squares (e.g. EN82, FN20XR, QL64XG) — 2 letters + 2 digits + optional 2 letters
_GRID_RE = re.compile(r'^[A-R]{2}\d{2}([A-X]{2})?$')

def _looks_like_call(tok):
    if not tok or tok in _CALL_BLOCKLIST:
        return False
    # Need at least one digit and total length 3-12
    if not (3 <= len(tok) <= 12):
        return False
    if not any(c.isdigit() for c in tok):
        return False
    # Reject pure-numeric tokens
    if tok.isdigit():
        return False
    # Reject band / power / grid-square false positives
    if _BAND_RE.match(tok) or _POWER_RE.match(tok) or _GRID_RE.match(tok):
        return False
    return True

def _extract_calls(text):
    """Return ordered, de-duped list of likely amateur callsigns from text."""
    if not text:
        return []
    seen = set()
    out = []
    for m in _CALL_RE.findall(text.upper()):
        if _looks_like_call(m) and m not in seen:
            seen.add(m)
            out.append(m)
    return out

def _parse_ical(text):
    """Minimal VEVENT extractor — returns list of {summary, dtstart, dtend, url, description}.
    Handles line-folding (lines starting with space/tab continue previous line)."""
    if not text:
        return []
    # Unfold continuation lines
    lines = []
    for raw in text.splitlines():
        if raw.startswith((" ", "\t")) and lines:
            lines[-1] += raw[1:]
        else:
            lines.append(raw)
    events = []
    cur = None
    for ln in lines:
        if ln.startswith("BEGIN:VEVENT"):
            cur = {}
        elif ln.startswith("END:VEVENT") and cur is not None:
            events.append(cur)
            cur = None
        elif cur is not None:
            if ":" not in ln:
                continue
            key, _, val = ln.partition(":")
            key = key.split(";", 1)[0].upper()
            if key == "SUMMARY":     cur["summary"] = val.strip()
            elif key == "DTSTART":   cur["dtstart"] = val.strip()
            elif key == "DTEND":     cur["dtend"]   = val.strip()
            elif key == "URL":       cur["url"]     = val.strip()
            elif key == "DESCRIPTION": cur["description"] = val.strip()
    return events

def _ical_dt_to_iso(s):
    """Convert iCal DTSTART/DTEND like 20260416T000000Z to ISO 8601 UTC."""
    if not s or len(s) < 8:
        return None
    try:
        if "T" in s:
            d = datetime.strptime(s.replace("Z",""), "%Y%m%dT%H%M%S")
        else:
            d = datetime.strptime(s, "%Y%m%d")
        return d.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None

def _fetch_contests():
    """Fetch + parse WA7BNM iCal. Returns list of contest dicts."""
    try:
        r = requests.get(_WA7BNM_ICAL_URL, timeout=12,
                         headers={"User-Agent": "SDRLogger+/1.09 (ham logger)"})
        r.raise_for_status()
        events = _parse_ical(r.text)
        out = []
        for e in events:
            out.append({
                "title": e.get("summary", "").strip(),
                "start": _ical_dt_to_iso(e.get("dtstart", "")),
                "end":   _ical_dt_to_iso(e.get("dtend", "")),
                "url":   e.get("url", "").strip() or "https://www.contestcalendar.com/weeklycont.php",
            })
        # Sort by start date
        out.sort(key=lambda x: x["start"] or "")
        return out
    except Exception as e:
        raise RuntimeError(f"WA7BNM fetch failed: {e}")

def _parse_rss(text):
    """Minimal RSS 2.0 item extractor. Returns list of {title, description, link}."""
    if not text:
        return []
    try:
        import xml.etree.ElementTree as ET
        # Strip BOM if present
        if text.startswith("\ufeff"):
            text = text[1:]
        root = ET.fromstring(text)
        items = []
        for it in root.iter("item"):
            d = {"title": "", "description": "", "link": ""}
            for child in it:
                tag = child.tag.split("}", 1)[-1].lower()
                if tag in d and (child.text or "").strip():
                    d[tag] = child.text.strip()
            items.append(d)
        return items
    except Exception:
        return []

# Match NG3K title: "Country: Date Range -- CALL -- QSL via: ..."
_NG3K_TITLE_RE = re.compile(r'^(.*?):\s*(.+?)\s*--\s*([A-Z0-9/]+)\s*--\s*(.*)$', re.IGNORECASE)

def _fetch_dxpeditions():
    """Fetch + parse NG3K RSS. Returns list of DXpedition dicts with extracted callsigns."""
    try:
        r = requests.get(_NG3K_RSS_URL, timeout=12,
                         headers={"User-Agent": "SDRLogger+/1.09 (ham logger)"})
        r.raise_for_status()
        items = _parse_rss(r.text)
        out = []
        for it in items:
            title = it.get("title", "")
            desc  = it.get("description", "")
            link  = it.get("link", "") or "http://www.ng3k.com/Misc/adxo.html"
            country = ""
            daterange = ""
            primary_call = ""
            qsl = ""
            m = _NG3K_TITLE_RE.match(title)
            if m:
                country     = m.group(1).strip()
                daterange   = m.group(2).strip()
                primary_call = m.group(3).strip().upper()
                qsl         = m.group(4).strip()
                if qsl.lower().startswith("qsl via:"):
                    qsl = qsl[8:].strip()
            # Callsigns: ONLY trust the CALL column from the NG3K title
            # (group 3 of _NG3K_TITLE_RE). The freeform description routinely
            # contains QSL-via calls, OPDX reporter calls, operator calls, and
            # other tokens that are NOT the DXpedition's operating call(s) —
            # surfacing them as clickable Hot-List chips was wrong.
            # Some titles list multiple ops with "/" or "+" separators; split
            # on those so each genuine call becomes its own chip while still
            # preserving portable-call slashes like "JG8NQJ/JD1".
            calls = []
            if primary_call:
                # Split on '+' (multi-op separator on NG3K) but keep '/' intact
                # for portable calls. Then validate each token.
                for tok in primary_call.split('+'):
                    tok = tok.strip().upper()
                    if not tok:
                        continue
                    # Strip any trailing punctuation
                    tok = tok.rstrip(',;.')
                    # A portable call like "JG8NQJ/JD1" is one chip; validate
                    # the base (pre-slash) part as a real call.
                    base = tok.split('/')[0]
                    if _looks_like_call(base) and tok not in calls:
                        calls.append(tok)
            out.append({
                "title":     title,
                "country":   country,
                "daterange": daterange,
                "qsl":       qsl,
                "calls":     calls,
                "description": desc,
                "link":      link,
            })
        return out
    except Exception as e:
        raise RuntimeError(f"NG3K fetch failed: {e}")

def _get_feed(kind, fetcher, force=False):
    """Cached feed access. kind = 'contests' | 'dxpeditions'."""
    import time as _t
    now = _t.time()
    with _feed_cache_lock:
        c = _feed_cache[kind]
        if not force and c["data"] is not None and (now - c["fetched_at"]) < _FEED_CACHE_TTL:
            return c["data"], c["fetched_at"], None
    # Fetch outside lock (network call)
    try:
        data = fetcher()
        with _feed_cache_lock:
            _feed_cache[kind] = {"data": data, "fetched_at": now, "error": None}
        return data, now, None
    except Exception as e:
        # Return stale cache if any, with error flag
        with _feed_cache_lock:
            c = _feed_cache[kind]
            c["error"] = str(e)
            return c["data"], c["fetched_at"], str(e)


@app.route("/feeds")
def feeds_page():
    return render_template("feeds.html", version=VERSION)


@app.route("/api/feeds/contests")
def api_feeds_contests():
    force = request.args.get("force", "0") == "1"
    data, fetched_at, err = _get_feed("contests", _fetch_contests, force=force)
    return jsonify({
        "ok": data is not None,
        "events": data or [],
        "fetched_at": datetime.utcfromtimestamp(fetched_at).isoformat() + "Z" if fetched_at else None,
        "cache_age_sec": int(time.time() - fetched_at) if fetched_at else None,
        "error": err,
        "source": "WA7BNM Contest Calendar",
        "source_url": "https://www.contestcalendar.com/",
    })


@app.route("/api/feeds/dxpeditions")
def api_feeds_dxpeditions():
    force = request.args.get("force", "0") == "1"
    data, fetched_at, err = _get_feed("dxpeditions", _fetch_dxpeditions, force=force)
    return jsonify({
        "ok": data is not None,
        "items": data or [],
        "fetched_at": datetime.utcfromtimestamp(fetched_at).isoformat() + "Z" if fetched_at else None,
        "error": err,
        "source": "NG3K Announced DX Operations",
        "source_url": "http://www.ng3k.com/Misc/adxo.html",
    })


# ─── S.A.T. UDP Listener ─────────────────────────────────────────────────────
def sat_udp_listener():
    """Background thread: listens for S.A.T. controller UDP broadcasts on port 9932.
    Parses comma-separated SAT protocol messages and updates _sat_state."""
    import time as _t
    while True:
        # Only listen when SAT integration is enabled AND user is in SAT mode.
        # When the user switches away from SAT mode, the listener releases the
        # socket so port 9932 is free for other tools.
        if not (SAT_UDP_ENABLED and ACTIVE_MODE == "sat"):
            _t.sleep(2)
            continue
        s = None
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("0.0.0.0", SAT_UDP_PORT))
            s.settimeout(2.0)
            _log(f"SAT: UDP listener active on port {SAT_UDP_PORT} (SAT mode)")
            while SAT_UDP_ENABLED and ACTIVE_MODE == "sat":
                try:
                    data, addr = s.recvfrom(4096)
                    if not data:
                        continue
                    text = data.decode('utf-8', errors='replace').strip()
                    if not text.startswith("SAT,"):
                        continue
                    _log(f"SAT: UDP from {addr[0]}: {text}")
                    _parse_sat_message(text)
                except socket.timeout:
                    continue
        except Exception as e:
            _log(f"SAT: UDP error (port {SAT_UDP_PORT}): {e}")
            _t.sleep(5)
        finally:
            if s:
                try: s.close()
                except: pass


def _parse_sat_message(text):
    """Parse a single SAT protocol message and update state machine."""
    parts = text.split(",")
    if len(parts) < 2:
        return
    cmd = parts[1].strip().upper()
    now = datetime.utcnow()

    with _sat_state_lock:
        _sat_state["last_heard"] = now.isoformat()

        if cmd == "BOOT" and len(parts) >= 4:
            _sat_state["serial"] = parts[2].strip()
            _sat_state["firmware"] = parts[3].strip()
            _sat_state["status"] = "idle"
            _sat_events.append({"time": now.isoformat(), "event": "boot",
                               "detail": f"S.A.T. v{parts[3].strip()} (SN: {parts[2].strip()})"})

        elif cmd == "START TRACK" and len(parts) >= 4:
            _sat_state["status"] = "tracking"
            _sat_state["satellite"] = parts[2].strip()
            _sat_state["catno"] = parts[3].strip()
            _sat_state["pass_qsos"] = []
            # Clear transponder info for new pass
            _sat_state["transponder"] = ""
            _sat_state["uplink_freq"] = ""
            _sat_state["downlink_freq"] = ""
            _sat_state["uplink_mode"] = ""
            _sat_state["downlink_mode"] = ""
            _sat_state["aos_az"] = ""
            _sat_state["los_az"] = ""
            _sat_state["aos_time"] = None
            _sat_events.append({"time": now.isoformat(), "event": "track",
                               "detail": f"Tracking {parts[2].strip()} ({parts[3].strip()})"})

        elif cmd == "AOS" and len(parts) >= 3:
            _sat_state["status"] = "aos"
            _sat_state["aos_az"] = parts[2].strip()
            _sat_state["aos_time"] = now.isoformat()
            _sat_events.append({"time": now.isoformat(), "event": "aos",
                               "detail": f"AOS at {parts[2].strip()}\u00b0"})

        elif cmd == "LOS" and len(parts) >= 3:
            _sat_state["status"] = "los"
            _sat_state["los_az"] = parts[2].strip()
            _sat_state["aos_time"] = None
            _sat_events.append({"time": now.isoformat(), "event": "los",
                               "detail": f"LOS at {parts[2].strip()}\u00b0"})

        elif cmd == "TRANSPONDER" and len(parts) >= 7:
            _sat_state["transponder"] = parts[2].strip()
            _sat_state["uplink_freq"] = parts[3].strip()
            _sat_state["uplink_mode"] = parts[4].strip()
            _sat_state["downlink_freq"] = parts[5].strip()
            _sat_state["downlink_mode"] = parts[6].strip()
            _sat_events.append({"time": now.isoformat(), "event": "transponder",
                               "detail": f"{parts[2].strip()}: \u2191{parts[3].strip()} \u2193{parts[5].strip()}"})

        elif cmd == "QSO" and len(parts) >= 12:
            # SAT,QSO,SATNAME,CALL,GRID,MODE,COMMENT,RSTSENT,RSTRECV,UPFREQ,DOWNFREQ,NAME
            qso_info = {
                "sat_name": parts[2].strip(),
                "callsign": parts[3].strip().upper(),
                "grid": parts[4].strip(),
                "mode": parts[5].strip().upper(),
                "comment": parts[6].strip(),
                "rst_sent": parts[7].strip(),
                "rst_recv": parts[8].strip(),
                "uplink_freq": parts[9].strip(),
                "downlink_freq": parts[10].strip(),
                "name": parts[11].strip() if len(parts) > 11 else "",
                "time": now.isoformat(),
            }
            _sat_state["pass_qsos"].append(qso_info)
            _sat_events.append({"time": now.isoformat(), "event": "qso",
                               "detail": f"QSO: {qso_info['callsign']} on {qso_info['sat_name']}"})
            # Auto-log to database
            _sat_auto_log_qso(qso_info)

        elif cmd == "STOP":
            _sat_state["status"] = "idle"
            _sat_state["aos_time"] = None
            _sat_events.append({"time": now.isoformat(), "event": "stop",
                               "detail": "Tracking stopped"})


def _sat_auto_log_qso(qso_info):
    """Auto-save a SAT QSO to the general database when received via UDP."""
    try:
        # Convert frequency from Hz to MHz
        up_mhz = ""
        dn_mhz = ""
        try:
            if qso_info.get("uplink_freq"):
                up_mhz = f"{float(qso_info['uplink_freq']) / 1e6:.6f}".rstrip('0').rstrip('.')
        except (ValueError, TypeError):
            up_mhz = qso_info.get("uplink_freq", "")
        try:
            if qso_info.get("downlink_freq"):
                dn_mhz = f"{float(qso_info['downlink_freq']) / 1e6:.6f}".rstrip('0').rstrip('.')
        except (ValueError, TypeError):
            dn_mhz = qso_info.get("downlink_freq", "")

        # Determine band from downlink frequency
        freq_for_band = None
        try:
            freq_for_band = float(dn_mhz) if dn_mhz else None
        except (ValueError, TypeError):
            pass
        band = freq_to_band(freq_for_band) if freq_for_band else ""

        now = datetime.utcnow()
        conn = get_db(DATABASE)  # Always use general DB for SAT QSOs
        conn.execute("""
            INSERT INTO qso_log
                (callsign, name, qth, date_worked, time_worked, band, mode,
                 freq_mhz, my_rst_sent, their_rst_rcvd, remarks, contest_name,
                 pota_ref, pota_p2p, state, country, gridsquare,
                 prop_mode, sat_name, sat_catno, transponder_name,
                 uplink_freq, downlink_freq, uplink_mode, downlink_mode,
                 aos_az, los_az, my_grid, my_lat, my_lon)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            qso_info.get("callsign", ""),
            qso_info.get("name", ""),
            qso_info.get("grid", ""),       # grid goes to qth
            now.strftime("%Y-%m-%d"),
            now.strftime("%H:%M:%S"),
            band,
            qso_info.get("mode", ""),
            float(dn_mhz) if dn_mhz else None,  # freq_mhz = downlink
            qso_info.get("rst_sent", "59"),
            qso_info.get("rst_recv", "59"),
            qso_info.get("comment", ""),
            "",                             # contest_name
            "", "",                         # pota_ref, pota_p2p
            "", "", qso_info.get("grid", ""),  # state, country, gridsquare
            "SAT",                          # prop_mode
            qso_info.get("sat_name", ""),
            _sat_state.get("catno", ""),
            _sat_state.get("transponder", ""),
            up_mhz,
            dn_mhz,
            _sat_state.get("uplink_mode", ""),
            _sat_state.get("downlink_mode", ""),
            _sat_state.get("aos_az", ""),
            _sat_state.get("los_az", ""),
            "",                             # my_grid (from settings later)
            "",                             # my_lat
            "",                             # my_lon
        ))
        conn.commit()
        conn.close()
        _log(f"SAT: QSO auto-logged: {qso_info.get('callsign')} on {qso_info.get('sat_name')}")
        global _sat_qso_counter
        _sat_qso_counter += 1
        # Update worked cache
        _worked_cache_add(qso_info.get("callsign", ""), band, qso_info.get("mode", ""))
    except Exception as e:
        _log(f"SAT: Auto-log error: {e}")


# ─── SAT ADIF-over-UDP Listener (QSO LOG TYPE) ──────────────────────────────
def sat_adif_listener():
    """Listen for ADIF records sent from S.A.T. via its QSO LOG TYPE feature.
    The S.A.T. sends ADIF-formatted QSO records over UDP to a configurable
    IP:PORT when a QSO is logged on the device (ACLog, N1MM, Log4OM, etc.)."""
    import socket, time
    while True:
        # Only listen when SAT integration is enabled AND user is in SAT mode.
        # When the user switches away from SAT mode, the listener releases
        # port 1100 so it's free for other applications.
        if not (SAT_UDP_ENABLED and ACTIVE_MODE == "sat"):
            time.sleep(2)
            continue
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.settimeout(3)
            s.bind(("0.0.0.0", SAT_ADIF_PORT))
            _log(f"SAT-ADIF: Listener active on port {SAT_ADIF_PORT} (SAT mode)")
            while SAT_UDP_ENABLED and ACTIVE_MODE == "sat":
                try:
                    data, addr = s.recvfrom(8192)
                except socket.timeout:
                    continue
                try:
                    text = data.decode("utf-8", errors="replace").strip()
                    if not text:
                        continue
                    _log(f"SAT-ADIF: Received {len(text)} bytes from {addr[0]}:{addr[1]}")
                    _log(f"SAT-ADIF: Raw → {text[:300]}")
                    _sat_process_adif_record(text)
                except Exception as pe:
                    _log(f"SAT-ADIF: Parse error: {pe}")
            s.close()
        except Exception as e:
            _log(f"SAT-ADIF: Error (port {SAT_ADIF_PORT}): {e}")
            time.sleep(5)


def _sat_process_adif_record(text):
    """Parse an ADIF record received from the S.A.T. and insert as SAT QSO."""
    # Use parse_adif_string for single-record parsing (more forgiving)
    fields = parse_adif_string(text)
    if not fields.get("call"):
        _log(f"SAT-ADIF: No CALL field found, skipping")
        return

    call = fields.get("call", "").upper()
    name = fields.get("name", "")
    grid = fields.get("gridsquare", fields.get("grid", ""))
    state = fields.get("state", "")
    country = fields.get("country", "")
    mode = fields.get("mode", "")
    rst_sent = fields.get("rst_sent", "59")
    rst_recv = fields.get("rst_rcvd", fields.get("rst_recv", "59"))
    comment = fields.get("comment", fields.get("notes", ""))
    sat_name = fields.get("sat_name", "")
    prop_mode = fields.get("prop_mode", "SAT")

    # Frequencies — ADIF uses MHz
    freq = fields.get("freq", "")
    freq_rx = fields.get("freq_rx", "")
    # If we have freq and freq_rx, assume freq=uplink, freq_rx=downlink
    # Some loggers use freq=downlink — the S.A.T. sends both
    up_mhz = freq if freq_rx else ""
    dn_mhz = freq_rx if freq_rx else freq

    # Band from downlink freq
    band_str = fields.get("band", "")
    if not band_str and dn_mhz:
        try:
            band_str = freq_to_band(float(dn_mhz))
        except (ValueError, TypeError):
            pass

    # Date and time
    dt = fields.get("qso_date", "")
    if len(dt) == 8:
        dt = f"{dt[:4]}-{dt[4:6]}-{dt[6:8]}"
    elif not dt:
        dt = date.today().isoformat()
    tm = fields.get("time_on", "")
    if len(tm) >= 6:
        tm = f"{tm[:2]}:{tm[2:4]}:{tm[4:6]}"
    elif len(tm) >= 4:
        tm = f"{tm[:2]}:{tm[2:4]}:00"
    elif not tm:
        tm = datetime.utcnow().strftime("%H:%M:%S")

    # Enrich with current SAT state if available
    with _sat_state_lock:
        if not sat_name and _sat_state.get("satellite"):
            sat_name = _sat_state["satellite"]
        catno = _sat_state.get("catno", "")
        transponder = _sat_state.get("transponder", "")
        up_mode = fields.get("sat_mode", _sat_state.get("uplink_mode", ""))
        dn_mode = _sat_state.get("downlink_mode", mode)
        aos_az = _sat_state.get("aos_az", "")
        los_az = _sat_state.get("los_az", "")

    # Fast in-memory dedup check (no DB query) — same callsign + date +
    # sat_name within ±5 minutes is treated as a duplicate. Prevents the
    # same QSO being inserted twice when both the UDP push and the /adif
    # background poll deliver it (the S.A.T. typically reports them ~60s
    # apart due to firmware QRZ-lookup / CSN-report delay).
    if _sat_dedup_check_and_record(call, dt, sat_name, tm):
        _log(f"SAT-ADIF: Duplicate skipped → {call} on {sat_name} at {tm} (already logged within 5min window)")
        return

    try:
        conn = get_db(DATABASE)
        conn.execute("""
            INSERT INTO qso_log
                (callsign, name, qth, date_worked, time_worked, band, mode,
                 freq_mhz, my_rst_sent, their_rst_rcvd, remarks, contest_name,
                 pota_ref, pota_p2p, state, country, gridsquare,
                 prop_mode, sat_name, sat_catno, transponder_name,
                 uplink_freq, downlink_freq, uplink_mode, downlink_mode,
                 aos_az, los_az, my_grid, my_lat, my_lon)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            call, name, grid, dt, tm,
            band_str, mode,
            float(dn_mhz) if dn_mhz else None,
            rst_sent, rst_recv,
            comment, "",                        # contest_name
            "", "",                             # pota_ref, pota_p2p
            state, country, grid,
            prop_mode if prop_mode else "SAT",
            sat_name, catno, transponder,
            up_mhz, dn_mhz,
            up_mode, dn_mode,
            aos_az, los_az,
            "", "", "",                         # my_grid, my_lat, my_lon
        ))
        conn.commit()
        conn.close()
        _log(f"SAT-ADIF: QSO logged → {call} on {sat_name} ({mode})")
        global _sat_qso_counter
        _sat_qso_counter += 1
        _worked_cache_add(call, band_str, mode)
        # Add to pass QSO list for the SAT status panel
        with _sat_state_lock:
            _sat_state["pass_qsos"].append({
                "callsign": call, "sat_name": sat_name,
                "mode": mode, "grid": grid,
                "rst_sent": rst_sent, "rst_recv": rst_recv,
                "time": tm, "source": "adif"
            })
    except Exception as e:
        _log(f"SAT-ADIF: Insert error: {e}")


# ─── SAT Status API ──────────────────────────────────────────────────────────
_sat_track_cache = {"data": None, "ts": 0}  # cache /track responses briefly


def _sat_subpoint_from_look(lat_deg, lon_deg, az_deg, el_deg, rng_km):
    """Compute sub-satellite geodetic (lat, lon, alt_km) from station look-angle + range.

    Uses a spherical Earth (R=6371 km) — accurate to <0.3% for LEO altitudes,
    which is plenty for a panel map display.

    Math:
      1. Station → ECEF unit vector S.
      2. Local ENU unit vectors at station.
      3. Look vector in ENU = (cos E sin A, cos E cos A, sin E).
      4. Satellite ECEF = station_ECEF + rng * (ENU→ECEF * look_ENU).
      5. Convert sat ECEF → geodetic lat/lon/alt.
    """
    try:
        from math import sin, cos, asin, atan2, sqrt, radians, degrees
        R = 6371.0  # Earth mean radius, km
        if lat_deg in (None, "") or lon_deg in (None, "") or rng_km in (None, 0):
            return None
        lat_deg = float(lat_deg); lon_deg = float(lon_deg)
        az = radians(float(az_deg or 0))
        el = radians(float(el_deg or 0))
        rng = float(rng_km)
        lat = radians(lat_deg); lon = radians(lon_deg)

        # Station ECEF (spherical)
        sx = R * cos(lat) * cos(lon)
        sy = R * cos(lat) * sin(lon)
        sz = R * sin(lat)

        # ENU components of look vector (azimuth clockwise from north)
        e = cos(el) * sin(az)
        n = cos(el) * cos(az)
        u = sin(el)

        # ENU → ECEF rotation
        ex = -sin(lon)*e - sin(lat)*cos(lon)*n + cos(lat)*cos(lon)*u
        ey =  cos(lon)*e - sin(lat)*sin(lon)*n + cos(lat)*sin(lon)*u
        ez =  cos(lat)*n + sin(lat)*u

        # Satellite ECEF
        satx = sx + rng * ex
        saty = sy + rng * ey
        satz = sz + rng * ez

        r_sat = sqrt(satx*satx + saty*saty + satz*satz)
        sat_lat = degrees(asin(satz / r_sat))
        sat_lon = degrees(atan2(saty, satx))
        sat_alt = r_sat - R
        return {"lat": round(sat_lat, 4), "lon": round(sat_lon, 4), "alt_km": round(sat_alt, 1)}
    except Exception:
        return None

def _sat_poll_track():
    """Poll the S.A.T. /track endpoint for live tracking data."""
    import time as _t
    now = _t.time()
    # Cache for 1.5s to avoid hammering the S.A.T.
    if _sat_track_cache["data"] and (now - _sat_track_cache["ts"]) < 1.5:
        return _sat_track_cache["data"]
    sat_ip = SAT_CONTROLLER_IP or "192.168.200.194"
    try:
        resp = requests.get(f"http://{sat_ip}/track", timeout=2)
        if resp.ok:
            data = resp.json()
            _sat_track_cache["data"] = data
            _sat_track_cache["ts"] = now
            # Update _sat_state from live data
            with _sat_state_lock:
                _sat_state["last_heard"] = datetime.utcnow().isoformat()
                if data.get("satname"):
                    _sat_state["satellite"] = data["satname"].strip()
                    _sat_state["catno"] = str(data.get("catno", ""))
                if data.get("aosAZ"):
                    _sat_state["aos_az"] = f"{data['aosAZ']:.1f}"
                if data.get("losAZ"):
                    _sat_state["los_az"] = f"{data['losAZ']:.1f}"
                # Determine pass status from ttaos/ttlos
                ttaos = data.get("ttaos", -1)
                ttlos = data.get("ttlos", -1)
                if data.get("satname") and ttaos == 0 and ttlos > 0:
                    _sat_state["status"] = "aos"
                    if not _sat_state.get("aos_time"):
                        _sat_state["aos_time"] = datetime.utcnow().isoformat()
                elif data.get("satname") and ttaos > 0:
                    _sat_state["status"] = "tracking"
                    _sat_state["aos_time"] = None
                elif data.get("satname"):
                    _sat_state["status"] = "tracking"
                else:
                    _sat_state["status"] = "idle"
                # Update firmware from /status data if present
                if data.get("fw"):
                    _sat_state["firmware"] = str(data["fw"])
            return data
    except Exception as e:
        _log(f"SAT: /track poll error: {e}")
    return None


@app.route("/api/sat/status")
def api_sat_status():
    """Return current S.A.T. state for the satellite status panel,
    enriched with live /track data from the S.A.T. controller."""
    # Poll live data from S.A.T.
    track = None
    # Only poll the S.A.T.'s /track endpoint when the user is in SAT mode —
    # avoids hitting the device unnecessarily when it's not being used.
    if SAT_UDP_ENABLED and SAT_CONTROLLER_IP and ACTIVE_MODE == "sat":
        track = _sat_poll_track()

    with _sat_state_lock:
        state = dict(_sat_state)
        state["pass_qsos"] = list(_sat_state["pass_qsos"])

    # Calculate elapsed time since AOS
    elapsed = ""
    if state.get("aos_time") and state["status"] == "aos":
        try:
            aos_dt = datetime.fromisoformat(state["aos_time"])
            delta = datetime.utcnow() - aos_dt
            mins = int(delta.total_seconds() // 60)
            secs = int(delta.total_seconds() % 60)
            elapsed = f"{mins:02d}:{secs:02d}"
        except Exception:
            pass
    state["elapsed"] = elapsed
    state["enabled"] = SAT_UDP_ENABLED
    state["port"] = SAT_UDP_PORT
    state["adif_port"] = SAT_ADIF_PORT
    state["qso_counter"] = _sat_qso_counter

    # Enrich with live /track data
    if track:
        state["live"] = True
        state["sat_az"] = round(track.get("satAZ", 0), 1)
        state["sat_el"] = round(track.get("satEL", 0), 1)
        state["max_el"] = round(track.get("maxEL", 0), 1)
        state["range_km"] = round(track.get("rng", 0), 1)
        state["ttlos"] = track.get("ttlos", -1)
        state["ttaos"] = track.get("ttaos", -1)
        state["los_az"] = f"{track['losAZ']:.1f}" if track.get("losAZ") else ""
        state["aos_az"] = f"{track['aosAZ']:.1f}" if track.get("aosAZ") else ""
        # GPS info — CSN S.A.T. firmware reports gpslat/gpslon in RADIANS (same
        # convention as satLat/satLon). Apply the same |val| ≤ π heuristic so
        # the my-position marker on the sat map lands at the right place.
        # Without this, an Ohio station (~41°N, -83°W → 0.71 rad, -1.45 rad)
        # ends up plotted near the equator off west Africa.
        state["my_grid"] = track.get("gpsgr", track.get("grid", ""))
        from math import degrees as _deg_my
        _raw_my_lat = track.get("gpslat", track.get("lat", ""))
        _raw_my_lon = track.get("gpslon", track.get("lon", ""))
        try:
            if _raw_my_lat != "" and _raw_my_lon != "":
                _ml = float(_raw_my_lat); _mn = float(_raw_my_lon)
                if abs(_ml) <= 3.2 and abs(_mn) <= 3.2:
                    state["my_lat"] = round(_deg_my(_ml), 5)
                    state["my_lon"] = round(_deg_my(_mn), 5)
                else:
                    state["my_lat"] = round(_ml, 5)
                    state["my_lon"] = round(_mn, 5)
            else:
                state["my_lat"] = _raw_my_lat
                state["my_lon"] = _raw_my_lon
        except Exception:
            state["my_lat"] = _raw_my_lat
            state["my_lon"] = _raw_my_lon
        state["gps_lock"] = bool(track.get("gpslock", 0))
        state["gps_sats"] = track.get("gpssats", 0)
        # Sub-satellite point for the map panel. The CSN S.A.T. firmware exposes
        # satLat / satLon as RADIANS, satAlt in km. Prefer those.
        # Fallback: compute from station + look-angle + range.
        #
        # NOTE on satFootprint: CSN firmware emits this field (inheriting the
        # predict/Gpredict convention where it's the DIAMETER in km, = 12756.33 *
        # acos(R/(R+h)) at 0° horizon). We still surface it to the frontend for
        # diagnostics, but the map code intentionally ignores it and computes
        # the footprint RADIUS directly from satAlt so there's a single source
        # of truth.
        from math import degrees as _deg
        raw_lat = track.get("satLat", track.get("satlat"))
        raw_lon = track.get("satLon", track.get("satlon"))
        sat_alt = track.get("satAlt", track.get("satalt"))
        sat_foot = track.get("satFootprint")
        sat_lat = sat_lon = None
        if raw_lat is not None and raw_lon is not None:
            try:
                # Heuristic: values in radians are bounded by |π|. If |val| ≤ π we
                # treat as radians (the CSN firmware's convention).
                rlat = float(raw_lat); rlon = float(raw_lon)
                if abs(rlat) <= 3.2 and abs(rlon) <= 3.2:
                    sat_lat = round(_deg(rlat), 4)
                    sat_lon = round(_deg(rlon), 4)
                else:
                    sat_lat = round(rlat, 4)
                    sat_lon = round(rlon, 4)
            except Exception:
                pass
        if sat_lat is None or sat_lon is None:
            sub = _sat_subpoint_from_look(
                state.get("my_lat"), state.get("my_lon"),
                state.get("sat_az"), state.get("sat_el"), state.get("range_km"),
            )
            if sub:
                sat_lat, sat_lon = sub["lat"], sub["lon"]
                if sat_alt is None: sat_alt = sub["alt_km"]
        if sat_lat is not None and sat_lon is not None:
            state["sat_lat"] = sat_lat
            state["sat_lon"] = sat_lon
            state["sat_alt_km"] = round(float(sat_alt), 1) if sat_alt is not None else None
            if sat_foot is not None:
                try: state["sat_footprint_km"] = round(float(sat_foot), 1)
                except Exception: pass
        # Active transponders with Doppler
        freqs = track.get("freq", [])
        state["transponders"] = []
        for f in freqs:
            up_hz = f.get("upFreq", 0)
            dn_hz = f.get("downFreq", 0)
            dop_up = f.get("dop_up", 0)
            dop_dn = f.get("dop_down", 0)
            state["transponders"].append({
                "name": (f.get("descr") or "").strip(),
                "up_mode": (f.get("upMode") or "").strip(),
                "dn_mode": (f.get("downMode") or "").strip(),
                "up_freq": up_hz,
                "dn_freq": dn_hz,
                "dop_up": dop_up,
                "dop_dn": dop_dn,
                "up_corrected": up_hz + dop_up if up_hz else 0,
                "dn_corrected": dn_hz + dop_dn if dn_hz else 0,
            })
        # Also set the "active" transponder info in main state for form auto-fill
        # Use the first transponder with both up and down freqs, or the one with Doppler
        active = None
        for t in state["transponders"]:
            if t["up_freq"] and t["dn_freq"] and (t["dop_up"] or t["dop_dn"]):
                active = t
                break
        if not active and state["transponders"]:
            # Fallback: first with both freqs
            for t in state["transponders"]:
                if t["up_freq"] and t["dn_freq"]:
                    active = t
                    break
        if not active and state["transponders"]:
            active = state["transponders"][0]
        if active:
            state["transponder"] = active["name"]
            state["uplink_freq"] = str(active["up_corrected"] or active["up_freq"])
            state["downlink_freq"] = str(active["dn_corrected"] or active["dn_freq"])
            state["uplink_mode"] = active["up_mode"]
            state["downlink_mode"] = active["dn_mode"]
    else:
        state["live"] = False

    # Recent events
    state["events"] = list(_sat_events)[-10:]
    return jsonify(state)


# ─── SAT TLE Feed (Celestrak amateur.txt, cached) ───────────────────────────
# The ground-track overlay on the sat map uses SGP4 propagation (client-side
# via satellite.js). TLEs come from Celestrak's amateur.txt. We cache the whole
# feed in-memory with a 6-hour TTL so we don't hammer their server when the
# user switches satellites.
_TLE_CACHE = {"fetched_at": 0.0, "lines": [], "by_name": {}}
_TLE_TTL_SECONDS = 6 * 3600
_TLE_URL = "https://celestrak.org/NORAD/elements/gp.php?GROUP=amateur&FORMAT=tle"

def _tle_normalize_name(name):
    """Match sat names loosely: uppercase, strip punctuation/whitespace.
    Celestrak uses 'ISS (ZARYA)', S.A.T. says 'ISS'. Also 'RS-44' vs 'RS44' etc."""
    if not name: return ""
    s = str(name).upper()
    # strip parenthesised aliases
    s = _re.sub(r"\(.*?\)", "", s)
    s = _re.sub(r"[^A-Z0-9]", "", s)
    return s.strip()

def _tle_refresh(force=False):
    """Fetch amateur.txt if cache is stale. Populates _TLE_CACHE['by_name']."""
    now = _time.time()
    if not force and (now - _TLE_CACHE["fetched_at"] < _TLE_TTL_SECONDS) and _TLE_CACHE["by_name"]:
        return True
    try:
        resp = requests.get(_TLE_URL, timeout=10)
        if resp.status_code != 200 or not resp.text:
            return False
        lines = [ln.rstrip("\r\n") for ln in resp.text.splitlines() if ln.strip()]
        by_name = {}
        i = 0
        while i + 2 < len(lines):
            name = lines[i].strip()
            l1 = lines[i+1]
            l2 = lines[i+2]
            if l1.startswith("1 ") and l2.startswith("2 "):
                by_name[_tle_normalize_name(name)] = {"name": name, "line1": l1, "line2": l2}
                i += 3
            else:
                i += 1
        if by_name:
            _TLE_CACHE["fetched_at"] = now
            _TLE_CACHE["lines"] = lines
            _TLE_CACHE["by_name"] = by_name
            return True
    except Exception as e:
        print(f"[TLE] fetch error: {e}")
    return False

@app.route("/api/sat/tle")
def api_sat_tle():
    """Return TLE lines for the requested satellite name (query param: sat).
    Cached in-memory for ~6h. Returns {ok, name, line1, line2} on success,
    {ok:false, error} on miss."""
    sat_name = (request.args.get("sat") or "").strip()
    if not sat_name:
        return jsonify({"ok": False, "error": "missing sat parameter"}), 400
    if not _tle_refresh():
        # Still try to use any cached data if the fetch failed
        if not _TLE_CACHE["by_name"]:
            return jsonify({"ok": False, "error": "tle feed unavailable"}), 502
    key = _tle_normalize_name(sat_name)
    rec = _TLE_CACHE["by_name"].get(key)
    # Fallback: contains-match (for names like "HADES-SA" vs "HADES SA (SO-121)")
    if not rec:
        for k, v in _TLE_CACHE["by_name"].items():
            if key and (key in k or k in key):
                rec = v
                break
    if not rec:
        return jsonify({"ok": False, "error": f"'{sat_name}' not in amateur.txt"}), 404
    return jsonify({
        "ok": True,
        "name": rec["name"],
        "line1": rec["line1"],
        "line2": rec["line2"],
        "cache_age_s": int(_time.time() - _TLE_CACHE["fetched_at"]),
    })


# ─── SAT QSO Counter (fast polling endpoint for new-QSO detection) ──────────
@app.route("/api/sat/qso_count")
def api_sat_qso_count():
    """Lightweight endpoint that just returns the SAT QSO counter.
    The frontend polls this faster than the full /api/sat/status to detect
    incoming QSOs with minimal latency, without making an HTTP call to the S.A.T."""
    return jsonify({"qso_counter": _sat_qso_counter})


# ─── SAT Log Fetch (pull ADIF from S.A.T. device) ───────────────────────────
def _time_to_minutes(time_str):
    """Convert HH:MM:SS or HH:MM string to minutes since midnight (int).
    Returns None on parse failure."""
    try:
        parts = time_str.strip().split(":")
        if len(parts) >= 2:
            return int(parts[0]) * 60 + int(parts[1])
    except (ValueError, AttributeError):
        pass
    return None


def _sat_is_duplicate(call, date, sat_name, time_str, existing_index, window_min=5):
    """Check if a SAT QSO is a duplicate by (callsign + date + sat_name) within
    a ±window_min minute time window. The S.A.T. can deliver the same QSO via
    multiple paths (UDP push at button-press time, /adif poll ~60s later after
    QRZ lookup / CSN submission), so a wider time window catches these reliably
    while still allowing legitimate later-pass contacts (typically minutes apart)."""
    key = (call.upper().strip(), date.strip(), (sat_name or "").upper().strip())
    if key not in existing_index:
        return False
    qso_min = _time_to_minutes(time_str)
    if qso_min is None:
        # Can't parse time — fall back to exact callsign+date+sat match
        return True
    for existing_min in existing_index[key]:
        if abs(existing_min - qso_min) <= window_min:
            return True
    return False


# In-memory dedup index — seeded once from the DB (last 7 days only),
# then updated incrementally as new SAT QSOs arrive. Avoids a full-table
# scan on every QSO arrival, which was causing significant lag (~5-7s)
# on databases with thousands of QSOs.
_sat_dedup_index  = {}                # (call, date, sat) -> [minutes_since_midnight, ...]
_sat_dedup_lock   = threading.Lock()
_sat_dedup_seeded = False

def _seed_sat_dedup_index():
    """One-time seed of the in-memory SAT dedup index from the DB.
    Only loads QSOs from the last 7 days — the dedup window is only 5
    minutes, so older QSOs can't conflict with new arrivals."""
    global _sat_dedup_seeded
    with _sat_dedup_lock:
        if _sat_dedup_seeded:
            return
        _sat_dedup_index.clear()
        try:
            from datetime import timedelta
            cutoff = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
            conn = get_db(DATABASE)
            rows = conn.execute(
                "SELECT callsign, date_worked, time_worked, sat_name "
                "FROM qso_log WHERE prop_mode='SAT' AND date_worked >= ?",
                (cutoff,)
            ).fetchall()
            conn.close()
            for row in rows:
                call = (row[0] or "").upper().strip()
                dt   = (row[1] or "").strip()
                tm   = (row[2] or "").strip()
                sat  = (row[3] or "").upper().strip()
                mins = _time_to_minutes(tm)
                if mins is None:
                    continue
                _sat_dedup_index.setdefault((call, dt, sat), []).append(mins)
            _sat_dedup_seeded = True
            _log(f"SAT-DEDUP: Index seeded with {len(_sat_dedup_index)} keys (last 7 days)")
        except Exception as e:
            _log(f"SAT-DEDUP: Seed error: {e}")
            _sat_dedup_seeded = True  # don't keep retrying


def _sat_dedup_check_and_record(call, date_str, sat_name, time_str, window_min=5):
    """Atomic check-and-record using the in-memory dedup index. Returns
    True if the QSO is a duplicate (caller should skip insert). Returns
    False if it's new — and the record is added to the index so subsequent
    checks (within the next few seconds) will catch it."""
    if not _sat_dedup_seeded:
        _seed_sat_dedup_index()
    qso_min = _time_to_minutes(time_str)
    if qso_min is None:
        return False  # can't reason about it — let it through
    key = ((call or "").upper().strip(),
           (date_str or "").strip(),
           (sat_name or "").upper().strip())
    with _sat_dedup_lock:
        existing = _sat_dedup_index.get(key, [])
        for em in existing:
            if abs(em - qso_min) <= window_min:
                return True
        existing.append(qso_min)
        _sat_dedup_index[key] = existing
        return False


def _sat_dedup_invalidate():
    """Force a full reseed of the dedup index from the DB. Called after
    the manual Dedupe button removes rows."""
    global _sat_dedup_seeded
    with _sat_dedup_lock:
        _sat_dedup_seeded = False
    _seed_sat_dedup_index()


def _sat_fetch_and_import(quiet=False):
    """Pull /adif from the S.A.T., parse it, and import new QSOs with dedupe.
    Returns (ok, imported, skipped, error). When quiet=True, suppresses log
    output for routine background polls (only logs when QSOs are imported)."""
    global _sat_qso_counter
    sat_ip = SAT_CONTROLLER_IP or "192.168.200.194"
    try:
        resp = requests.get(f"http://{sat_ip}/adif", timeout=5)
        if not resp.ok:
            return (False, 0, 0, f"S.A.T. returned HTTP {resp.status_code}")
        adif_text = resp.text
    except Exception as e:
        return (False, 0, 0, f"Cannot reach S.A.T. at {sat_ip}: {e}")

    records = _parse_adif_records(adif_text)
    if not records:
        return (True, 0, 0, None)

    conn = get_db(DATABASE)

    imported = 0
    skipped = 0
    for rec in records:
        qso = _adif_to_qso(rec)
        if not qso:
            continue
        if not qso.get("prop_mode"):
            qso["prop_mode"] = "SAT"
        # Fast in-memory dedup (no DB query) — check_and_record is atomic
        if _sat_dedup_check_and_record(qso["callsign"], qso["date_worked"],
                                       qso.get("sat_name", ""),
                                       qso["time_worked"]):
            skipped += 1
            continue
        try:
            conn.execute("""
                INSERT INTO qso_log
                    (callsign, name, qth, date_worked, time_worked, band, mode,
                     freq_mhz, my_rst_sent, their_rst_rcvd, remarks, contest_name,
                     pota_ref, pota_p2p, state, country, gridsquare,
                     prop_mode, sat_name)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                qso["callsign"], qso["name"], qso["qth"],
                qso["date_worked"], qso["time_worked"],
                qso["band"], qso["mode"],
                float(qso["freq_mhz"]) if qso["freq_mhz"] else None,
                qso["my_rst_sent"], qso["their_rst_rcvd"],
                qso["remarks"], qso["contest_name"],
                qso["pota_ref"], qso["pota_p2p"],
                qso.get("state", ""), qso.get("country", ""),
                qso.get("gridsquare", ""),
                qso["prop_mode"], qso.get("sat_name", ""),
            ))
            imported += 1
            _worked_cache_add(qso["callsign"], qso["band"], qso["mode"])
        except Exception as e:
            _log(f"SAT-FETCH: Insert error for {qso['callsign']}: {e}")

    conn.commit()
    conn.close()
    if imported:
        _sat_qso_counter += imported
        _log(f"SAT-FETCH: Imported {imported}, skipped {skipped} duplicates from S.A.T. at {sat_ip}")
    elif not quiet:
        _log(f"SAT-FETCH: Imported {imported}, skipped {skipped} duplicates from S.A.T. at {sat_ip}")
    return (True, imported, skipped, None)


def sat_log_poller():
    """Background thread: polls the S.A.T.'s /adif endpoint every 2.5 seconds
    and auto-imports any new QSOs. Acts as a fast-path complement to the
    UDP ADIF push — many S.A.T. firmwares delay the UDP push by several
    seconds (waiting for QRZ lookup / CSN report), but /adif is updated
    immediately. Polling here typically delivers QSOs to SDRLogger+ within
    2-3 seconds of pressing ADD ENTRY on the S.A.T."""
    import time
    # Wait a few seconds at startup so settings are loaded
    time.sleep(5)
    while True:
        # Only poll when SAT integration is enabled AND user is in SAT mode.
        # Stops the periodic /adif HTTP requests when the user switches to
        # General or POTA mode — no traffic to the S.A.T. unless needed.
        if not (SAT_UDP_ENABLED and SAT_CONTROLLER_IP and ACTIVE_MODE == "sat"):
            time.sleep(3)
            continue
        try:
            _sat_fetch_and_import(quiet=True)
        except Exception as e:
            _log(f"SAT-POLLER: Error: {e}")
        time.sleep(2.5)


@app.route("/api/sat/dedupe", methods=["POST"])
def api_sat_dedupe():
    """One-shot cleanup: find SAT QSOs that are duplicates of one another
    (same callsign + date + sat_name within ±5 minutes) and delete the LATER
    one (which is typically the /adif-poll copy that arrived ~60s after the
    UDP push and lacks band/freq data). Returns a count of removed rows."""
    try:
        conn = get_db(DATABASE)
        # Pull all SAT QSOs ordered by date+time so the FIRST (earliest) one
        # in any duplicate cluster is kept.
        rows = conn.execute(
            "SELECT id, callsign, date_worked, time_worked, sat_name, band, freq_mhz "
            "FROM qso_log WHERE prop_mode='SAT' "
            "ORDER BY date_worked ASC, time_worked ASC"
        ).fetchall()

        # Group by (call, date, sat) — within each group, keep first within
        # 5min window, mark the rest as duplicates
        seen = {}    # (call, date, sat) -> [(time_min, id), ...]
        to_delete = []
        for row in rows:
            qid = row[0]
            call = (row[1] or "").upper().strip()
            dt   = (row[2] or "").strip()
            tm   = (row[3] or "").strip()
            sat  = (row[4] or "").upper().strip()
            mins = _time_to_minutes(tm)
            if mins is None:
                continue
            key = (call, dt, sat)
            kept = seen.get(key, [])
            is_dup = any(abs(km - mins) <= 5 for km, _ in kept)
            if is_dup:
                to_delete.append(qid)
            else:
                kept.append((mins, qid))
                seen[key] = kept

        if to_delete:
            placeholders = ",".join("?" * len(to_delete))
            conn.execute(f"DELETE FROM qso_log WHERE id IN ({placeholders})", to_delete)
            conn.commit()
        conn.close()
        # Reseed the in-memory dedup index since we deleted rows from the DB
        _sat_dedup_invalidate()
        _log(f"SAT-DEDUPE: Removed {len(to_delete)} duplicate SAT QSOs")
        return jsonify({"ok": True, "removed": len(to_delete),
                        "msg": f"Removed {len(to_delete)} duplicate SAT QSO{'s' if len(to_delete) != 1 else ''}"})
    except Exception as e:
        _log(f"SAT-DEDUPE: Error: {e}")
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/sat/fetch_log", methods=["POST"])
def api_sat_fetch_log():
    """Manual trigger of the S.A.T. /adif fetch + import (used by the
    'Fetch S.A.T. Log' button). Returns a user-facing message."""
    ok, imported, skipped, err = _sat_fetch_and_import(quiet=False)
    if not ok:
        return jsonify({"ok": False, "error": err})
    return jsonify({"ok": True, "imported": imported, "skipped": skipped,
                    "msg": f"Imported {imported} QSO{'s' if imported != 1 else ''}, "
                           f"skipped {skipped} duplicate{'s' if skipped != 1 else ''}"})


# ─── Entry Point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    os.makedirs("templates", exist_ok=True)
    init_db()
    _load_cty_dat()
    _rebuild_worked_cache()
    _load_app_settings()
    _load_cw_serial()
    # Start TCI WebSocket client in background thread
    tci_thread = threading.Thread(target=tci_ws_client, daemon=True)
    tci_thread.start()
    # Start Digital App integration listeners (always running; activate via settings)
    threading.Thread(target=digital_udp_listener, daemon=True).start()
    threading.Thread(target=digital_tcp_server,   daemon=True).start()
    # Start S.A.T. UDP listener (always running; activates when SAT_UDP_ENABLED is True)
    threading.Thread(target=sat_udp_listener, daemon=True).start()
    # Start S.A.T. ADIF-over-UDP listener (QSO LOG TYPE — port 1100 default)
    threading.Thread(target=sat_adif_listener, daemon=True).start()
    # Start S.A.T. /adif log poller (fast-path for QSO import — bypasses any UDP delay)
    threading.Thread(target=sat_log_poller, daemon=True).start()
    # Start flrig poller (always running; activates when FLRIG_ENABLED is True)
    threading.Thread(target=flrig_poller, daemon=True).start()
    # Start HamLib poller (always running; activates when HAMLIB_ENABLED is True)
    threading.Thread(target=hamlib_poller, daemon=True).start()
    # Start WinKeyer manager (always running; activates when WINKEYER_ENABLED is True)
    threading.Thread(target=winkeyer_manager, daemon=True).start()
    # Start ADIF file monitor
    threading.Thread(target=adif_monitor_thread, daemon=True).start()
    # Start lightning detection thread
    threading.Thread(target=lightning_thread, daemon=True).start()
    print("\n  SDRLogger+ starting...")
    print(f"  TCI    : ws://{TCI_HOST}:{TCI_PORT}")
    print(f"  flrig  : XML-RPC poller ready (enable in Settings)")
    print(f"  HamLib : rigctld poller ready (enable in Settings)")
    print(f"  Browser: http://localhost:{WEB_PORT}")
    if WEB_HOST == "0.0.0.0":
        import socket as _s
        try:
            _lan = _s.gethostbyname(_s.gethostname())
            print(f"  LAN    : http://{_lan}:{WEB_PORT}")
        except Exception:
            pass
    print()
    app.run(debug=False, host=WEB_HOST, port=WEB_PORT, threaded=True)
