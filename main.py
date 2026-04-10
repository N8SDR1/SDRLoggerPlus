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
import json
import os
import shutil
import struct
import zipfile
import requests
from datetime import datetime, date
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
    global ROTATOR_ENABLED, ROTATOR_HOST, ROTATOR_PORT, ROTATOR_PROTOCOL, ROTATOR_AUTO
    global BACKUP_PATH, FLRIG_ENABLED, FLRIG_HOST, FLRIG_PORT, FLRIG_DIGITAL_MODE, FLRIG_RTTY_MODE
    global HAMLIB_ENABLED, HAMLIB_HOST, HAMLIB_PORT
    global WINKEYER_ENABLED, WINKEYER_PORT, WINKEYER_WPM, WINKEYER_KEY_OUT, WINKEYER_MODE, WINKEYER_PTT, WINKEYER_PTT_LEAD, WINKEYER_PTT_TAIL
    global EQSL_USER, EQSL_PASS, EQSL_UPLOAD_ENABLED
    global POTA_MY_PARK, POTA_USER, POTA_PASS
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
        if "backup_path" in data:               BACKUP_PATH               = data["backup_path"].strip()
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
QRZ_LOGBOOK_UPLOAD_ENABLED = True             # Auto-upload QSOs to QRZ Logbook (toggleable from Settings)
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
VERSION  = "0.49 Beta"

# ─── Digital App Integration (WSJT-X / JTDX / MSHV / VarAC etc.) ─────────────
DIGITAL_UDP_ENABLED = False       # Listen for UDP QSOLogged packets (WSJT-X binary / ADIF text)
DIGITAL_UDP_PORT    = 2237        # Default WSJT-X port; VarAC can be pointed here too
DIGITAL_TCP_ENABLED = False       # Listen for TCP ADIF connections (VarAC / Logger32 / DXKeeper style)
DIGITAL_TCP_PORT    = 52001       # Standard TCP ADIF port used by N1MM/DXKeeper

# ─── Rotator Control ──────────────────────────────────────────────────────────
ROTATOR_ENABLED  = False
ROTATOR_HOST     = "127.0.0.1"
ROTATOR_PORT     = 12000          # PstRotator default
ROTATOR_PROTOCOL = "pstrotator"  # pstrotator | gs232 | easycomm
ROTATOR_AUTO     = True           # Auto-rotate when clicking a DX spot or entering a callsign

# Live azimuth updated by the PstRotator background poller thread.
# None until the first successful status read.
_rot_live_az     = None
_rot_live_az_lock = threading.Lock()

# ─── Backup ───────────────────────────────────────────────────────────────────
BACKUP_PATH = ""   # User-configured local folder for DB backups (empty = browser download)

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


# ─── POTA (Parks on the Air) ──────────────────────────────────────────────────
POTA_DATABASE   = "pota.db"     # Separate DB for POTA activations
ACTIVE_MODE     = "general"     # "general" | "pota"
POTA_MY_PARK    = ""            # Sticky park reference (e.g. K-1234) for current activation
POTA_USER       = ""            # POTA.app username for self-spotting
POTA_PASS       = ""            # POTA.app password for self-spotting
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
tci_spot_registry      = {}   # {freq_hz: callsign}
tci_spot_registry_lock = threading.Lock()

# Digital app event queue — browser polls /api/digital_events to get auto-logged QSOs
import collections, re as _re
_digital_events = collections.deque(maxlen=50)   # {callsign, mode, freq_mhz, source, time}


# ─── Database Setup ────────────────────────────────────────────────────────────
def get_db(db_path=None):
    """Return a DB connection. Uses POTA_DATABASE when in pota mode unless overridden."""
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
    for col in ["name", "qth", "pota_ref", "pota_p2p"]:
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
            _DIGITAL_PASSTHROUGH_USB = {"FT8","FT4","JS8","WSPR","JT65","JT9","DIGI","PSK31","DIGU","DATA-U","PKT-U"}
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
        "DIGI":"PKTUSB","DATA":"PKTUSB","VARAC":"PKTUSB",
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
    return jsonify({"tci":      tci_ws_connected,
                    "flrig":    bool(FLRIG_ENABLED and flrig_connected),
                    "hamlib":   bool(HAMLIB_ENABLED and hamlib_connected),
                    "winkeyer": bool(WINKEYER_ENABLED and _wk_is_open),
                    "wpm": _cw_wpm, "breakin": _cw_break_in,
                    "mycall": MY_CALLSIGN, "myname": MY_NAME,
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
             pota_ref, pota_p2p)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
    ))
    conn.commit()
    conn.close()

    # Optional telnet spot
    if TELNET_ENABLED:
        threading.Thread(target=send_telnet_spot,
                         args=(callsign, data.get("freq_mhz"), data.get("mode")),
                         daemon=True).start()

    # Auto-upload to QRZ Logbook if key is configured
    qrz_logid = None
    qrz_msg = None
    if QRZ_LOGBOOK_KEY and QRZ_LOGBOOK_UPLOAD_ENABLED:
        qrz_logid, qrz_err = qrz_logbook_upload(data | {"callsign": callsign})
        if qrz_err:
            print(f"QRZ Logbook upload failed: {qrz_err}")
            qrz_msg = qrz_err
        else:
            print(f"QRZ Logbook upload OK — LOGID {qrz_logid}")

    # Auto-upload to LoTW via TQSL if configured
    lotw_ok = None
    lotw_msg = None
    if LOTW_TQSL_PATH and LOTW_UPLOAD_ENABLED:
        lotw_ok, lotw_err = lotw_upload(data | {"callsign": callsign})
        if lotw_err:
            print(f"LoTW upload failed: {lotw_err}")
            lotw_msg = lotw_err
        else:
            print(f"LoTW upload OK — {callsign}")

    # Auto-upload to Club Log realtime API if configured
    clublog_ok = None
    clublog_msg = None
    if CLUBLOG_UPLOAD_ENABLED and not _clublog_blocked:
        clublog_ok, clublog_err = clublog_upload(data | {"callsign": callsign})
        if clublog_err:
            print(f"Club Log upload failed: {clublog_err}")
            clublog_msg = clublog_err
        else:
            print(f"Club Log upload OK — {callsign}")

    # Auto-upload to eQSL.cc if configured
    eqsl_ok = None
    eqsl_msg = None
    if EQSL_UPLOAD_ENABLED:
        eqsl_ok, eqsl_err = eqsl_upload(data | {"callsign": callsign})
        if eqsl_err:
            print(f"eQSL upload failed: {eqsl_err}")
            eqsl_msg = eqsl_err
        else:
            print(f"eQSL upload OK — {callsign}")

    return jsonify({"ok": True, "callsign": callsign,
                    "qrz_logid": qrz_logid, "qrz_msg": qrz_msg,
                    "lotw_ok": lotw_ok, "lotw_msg": lotw_msg,
                    "clublog_ok": clublog_ok, "clublog_msg": clublog_msg,
                    "eqsl_ok": eqsl_ok, "eqsl_msg": eqsl_msg})


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
            timeout=8
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
            timeout=8
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
                            timeout=8)
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
                            timeout=8)
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
            timeout=15
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


@app.route("/api/callsign_lookup/<callsign>")
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
    global ROTATOR_ENABLED, ROTATOR_HOST, ROTATOR_PORT, ROTATOR_PROTOCOL, ROTATOR_AUTO
    global BACKUP_PATH
    global FLRIG_ENABLED, FLRIG_HOST, FLRIG_PORT, FLRIG_DIGITAL_MODE, FLRIG_RTTY_MODE
    global HAMLIB_ENABLED, HAMLIB_HOST, HAMLIB_PORT
    global WINKEYER_ENABLED, WINKEYER_PORT, WINKEYER_WPM, WINKEYER_KEY_OUT, WINKEYER_MODE, WINKEYER_PTT, WINKEYER_PTT_LEAD, WINKEYER_PTT_TAIL
    global EQSL_USER, EQSL_PASS, EQSL_UPLOAD_ENABLED
    global POTA_MY_PARK, POTA_USER, POTA_PASS
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
    if "backup_path"      in data: BACKUP_PATH      = data["backup_path"].strip()
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


@app.route("/api/apply_update", methods=["POST"])
def apply_update():
    """
    Accept a SDRLogger+ update zip, extract files into the app directory.
    Safety rules:
      - Rejects any zip containing path-traversal sequences (../)
      - Never overwrites .db files (user data is always preserved)
      - Only extracts into the app's own directory tree
    User must restart SDRLogger+ after this completes.
    """
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "No file uploaded"}), 400

    upload   = request.files["file"]
    zip_data = upload.read()

    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_data))
    except zipfile.BadZipFile:
        return jsonify({"ok": False, "error": "File does not appear to be a valid zip archive"}), 400

    # When running as a frozen installed exe, write to the user's AppData
    # directory where we have write access without elevation.
    # When running from source, write to the app's own directory as before.
    import sys as _sys
    if getattr(_sys, "frozen", False):
        app_dir = os.environ.get("SDRLOGGERPLUS_DATA", os.path.dirname(os.path.abspath(__file__)))
    else:
        app_dir = os.path.dirname(os.path.abspath(__file__))
    extracted = []
    skipped   = []

    for name in zf.namelist():
        # Skip directory entries
        if name.endswith("/"):
            continue
        # Block path traversal
        if ".." in name or name.startswith("/") or name.startswith("\\"):
            return jsonify({"ok": False, "error": f"Unsafe path in zip: {name}"}), 400
        # Never overwrite databases
        if name.lower().endswith(".db"):
            skipped.append(name)
            continue
        try:
            dest = os.path.join(app_dir, name)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with zf.open(name) as src, open(dest, "wb") as dst:
                dst.write(src.read())
            extracted.append(name)
        except Exception as e:
            return jsonify({"ok": False, "error": f"Failed writing {name}: {e}"}), 500

    return jsonify({
        "ok":        True,
        "extracted": extracted,
        "skipped":   skipped,
        "count":     len(extracted),
    })


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
            _DIGITAL_PASSTHROUGH_USB = {"FT8","FT4","JS8","WSPR","JT65","JT9","DIGI","PSK31","DIGU","DATA-U","PKT-U"}
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
    """Switch active logging mode between 'general' and 'pota'."""
    global ACTIVE_MODE, POTA_MY_PARK
    data = request.json or {}
    mode = data.get("mode", "general")
    if mode not in ("general", "pota"):
        return jsonify({"ok": False, "error": "Invalid mode"}), 400
    ACTIVE_MODE  = mode
    if "pota_my_park" in data:
        POTA_MY_PARK = data["pota_my_park"].strip().upper()
    # Make sure the target DB schema is ready
    _init_one_db(POTA_DATABASE if mode == "pota" else DATABASE)
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
            "WSPR": "DIGU", "JT65": "DIGU", "JT9":  "DIGU",
            "PSK31":"DIGU", "DIGI": "DIGU", "DATA": "DIGU", "VARAC":"DIGU",
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
    """Return the live azimuth read by the PstRotator background poller.
    Returns null when the protocol is not pstrotator or the poller has not
    yet connected.  The frontend polls this every second to animate the
    Current AZ display while the antenna is moving."""
    with _rot_live_az_lock:
        az = _rot_live_az
    return jsonify({"az": az, "protocol": ROTATOR_PROTOCOL,
                    "enabled": ROTATOR_ENABLED})


@app.route("/api/digital_status")
def digital_status():
    return jsonify({
        "udp_enabled": DIGITAL_UDP_ENABLED, "udp_port": DIGITAL_UDP_PORT,
        "tcp_enabled": DIGITAL_TCP_ENABLED, "tcp_port": DIGITAL_TCP_PORT,
        "rotator_enabled": ROTATOR_ENABLED, "rotator_host": ROTATOR_HOST,
        "rotator_port": ROTATOR_PORT, "rotator_protocol": ROTATOR_PROTOCOL,
        "rotator_auto": ROTATOR_AUTO,
    })


# ─── Graceful shutdown via keepalive heartbeat ────────────────────────────────
# Each browser page sends POST /api/keepalive every 20 s.
# A background thread exits the process after KEEPALIVE_TIMEOUT seconds of
# silence — meaning all browser tabs/windows have been closed.
# This replaces the old pagehide/sendBeacon/cancel approach which suffered from
# a race condition: navigating between pages (e.g. index → help) could fire
# sendBeacon before the new page cancelled the timer, killing the process.

import time as _time

KEEPALIVE_TIMEOUT = 180   # seconds — 3 min; covers normal page-to-page navigation
_last_keepalive   = _time.monotonic()
_keepalive_lock   = threading.Lock()

@app.route("/api/keepalive", methods=["POST"])
def api_keepalive():
    """Heartbeat sent by every page every 20 s to keep the server alive."""
    global _last_keepalive
    with _keepalive_lock:
        _last_keepalive = _time.monotonic()
    return "", 204

def _keepalive_watchdog():
    """Exit the process when no keepalive has arrived for KEEPALIVE_TIMEOUT s."""
    while True:
        _time.sleep(10)   # check every 10 s
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
            timeout=8
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
    if search:
        like  = f"%{search}%"
        total = conn.execute(
            "SELECT COUNT(*) FROM qso_log WHERE callsign LIKE ? OR band LIKE ? OR mode LIKE ? OR contest_name LIKE ?",
            (like, like, like, like)
        ).fetchone()[0]
        rows  = conn.execute(
            "SELECT * FROM qso_log WHERE callsign LIKE ? OR band LIKE ? OR mode LIKE ? OR contest_name LIKE ? "
            "ORDER BY date_worked DESC, time_worked DESC LIMIT ? OFFSET ?",
            (like, like, like, like, limit, offset)
        ).fetchall()
    else:
        total = conn.execute("SELECT COUNT(*) FROM qso_log").fetchone()[0]
        rows  = conn.execute(
            "SELECT * FROM qso_log ORDER BY date_worked DESC, time_worked DESC LIMIT ? OFFSET ?",
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
    return jsonify({"ok": True})


@app.route("/api/qso/<int:qso_id>", methods=["PUT"])
def update_qso(qso_id):
    data = request.json
    conn = get_db()
    conn.execute("""
        UPDATE qso_log SET
            callsign=?, name=?, qth=?, date_worked=?, time_worked=?,
            band=?, mode=?, freq_mhz=?, my_rst_sent=?, their_rst_rcvd=?,
            remarks=?, contest_name=?
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
        qso_id,
    ))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


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

        conn.execute("""
            INSERT INTO qso_log
                (callsign, name, qth, date_worked, time_worked, band, mode,
                 freq_mhz, my_rst_sent, their_rst_rcvd, remarks, contest_name)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
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
        ))
        imported += 1
    conn.commit()
    conn.close()
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


# ─── Entry Point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    os.makedirs("templates", exist_ok=True)
    init_db()
    _load_app_settings()
    _load_cw_serial()
    # Start TCI WebSocket client in background thread
    tci_thread = threading.Thread(target=tci_ws_client, daemon=True)
    tci_thread.start()
    # Start Digital App integration listeners (always running; activate via settings)
    threading.Thread(target=digital_udp_listener, daemon=True).start()
    threading.Thread(target=digital_tcp_server,   daemon=True).start()
    # Start flrig poller (always running; activates when FLRIG_ENABLED is True)
    threading.Thread(target=flrig_poller, daemon=True).start()
    # Start HamLib poller (always running; activates when HAMLIB_ENABLED is True)
    threading.Thread(target=hamlib_poller, daemon=True).start()
    # Start WinKeyer manager (always running; activates when WINKEYER_ENABLED is True)
    threading.Thread(target=winkeyer_manager, daemon=True).start()
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
