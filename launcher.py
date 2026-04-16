"""
SDRLogger+ — Launcher / Entry Point
-----------------------------------
This file is what PyInstaller wraps into the executable.
It handles:
  • Locating bundled resources (templates, config, icon) whether frozen or running from source
  • Routing the SQLite database + config.json to a writable user-data directory
  • Respecting the port / bind-host settings in config.json
  • Running Flask in a background thread (no console window)
  • System tray icon — right-click to open browser or exit
"""

import sys
import os
import threading
import webbrowser
import time
import json
import shutil
import socket


# ── Resource paths ─────────────────────────────────────────────────────────────

def bundle_dir() -> str:
    """Directory containing bundled files (templates, main.py, config.json, assets)."""
    if getattr(sys, "frozen", False):
        return sys._MEIPASS
    return os.path.dirname(os.path.abspath(__file__))


def data_dir() -> str:
    """Writable user-data directory for the SQLite database, config, and templates."""
    if sys.platform == "win32":
        base = os.environ.get("APPDATA", os.path.expanduser("~"))
        return os.path.join(base, "SDRLoggerPlus")
    elif sys.platform == "darwin":
        return os.path.join(
            os.path.expanduser("~"), "Library", "Application Support", "SDRLoggerPlus"
        )
    else:
        return os.path.join(os.path.expanduser("~"), ".sdrloggerplus")


# ── Bootstrap ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    _bundle = bundle_dir()
    _data   = data_dir()

    os.makedirs(_data, exist_ok=True)

    # ── Seed config files into data dir on first run ───────────────────────────
    _cfg_src = os.path.join(_bundle, "config.json")
    _cfg_dst = os.path.join(_data,   "config.json")
    if not os.path.exists(_cfg_dst) and os.path.exists(_cfg_src):
        shutil.copy2(_cfg_src, _cfg_dst)

    _app_src = os.path.join(_bundle, "app_settings.json")
    _app_dst = os.path.join(_data,   "app_settings.json")
    if not os.path.exists(_app_dst) and os.path.exists(_app_src):
        shutil.copy2(_app_src, _app_dst)

    # ── Read port / host from user's config.json ──────────────────────────────
    _port = 5000
    _host = "0.0.0.0"
    try:
        with open(_cfg_dst) as _f:
            _cfg = json.load(_f)
            _port = int(_cfg.get("web_port", 5000))
            _host = _cfg.get("web_host", "0.0.0.0")
    except Exception:
        pass

    # ── Seed templates into AppData (only if not already there) ───────────────
    # Templates live in AppData so the zip updater can overwrite them without
    # needing admin rights. A new installer clears this folder so fresh
    # bundled templates take effect after an upgrade.
    _tmpl_src = os.path.join(_bundle, "templates")
    _tmpl_dst = os.path.join(_data,   "templates")
    if not os.path.exists(_tmpl_dst):
        shutil.copytree(_tmpl_src, _tmpl_dst)

    # Change CWD to data dir so hamlog.db is created there
    os.chdir(_data)

    # Expose data directory to main.py so the zip updater knows where to write
    os.environ["SDRLOGGERPLUS_DATA"] = _data

    # Make main.py importable — data dir is inserted first so a zip-updated
    # main.py in AppData takes priority over the bundled (frozen) copy.
    sys.path.insert(0, _bundle)
    sys.path.insert(0, _data)   # prepend: searched before _bundle

    # ── Import the Flask application ──────────────────────────────────────────
    import main as _app_module

    _app_module._CONFIG_FILE        = _cfg_dst
    _app_module._APP_SETTINGS_FILE  = os.path.join(_data, "app_settings.json")
    _app_module.WEB_PORT            = _port
    _app_module.WEB_HOST            = _host
    _app_module.app.template_folder = _tmpl_dst
    _app_module.app.static_folder   = os.path.join(_bundle, "static")

    # ── Initialise DB, restore settings, and start background threads ─────────
    _app_module.init_db()
    _app_module._load_cty_dat()
    _app_module._rebuild_worked_cache()
    _app_module._load_app_settings()
    _app_module._load_cw_serial()
    threading.Thread(target=_app_module.tci_ws_client,          daemon=True).start()
    threading.Thread(target=_app_module.digital_udp_listener,   daemon=True).start()
    threading.Thread(target=_app_module.digital_tcp_server,     daemon=True).start()
    threading.Thread(target=_app_module.sat_udp_listener,       daemon=True).start()
    threading.Thread(target=_app_module.sat_adif_listener,      daemon=True).start()
    threading.Thread(target=_app_module.sat_log_poller,         daemon=True).start()
    threading.Thread(target=_app_module.flrig_poller,           daemon=True).start()
    threading.Thread(target=_app_module.hamlib_poller,          daemon=True).start()
    threading.Thread(target=_app_module.winkeyer_manager,       daemon=True).start()
    threading.Thread(target=_app_module.adif_monitor_thread,    daemon=True).start()
    threading.Thread(target=_app_module.lightning_thread,       daemon=True).start()

    # ── Start Flask in a background thread ────────────────────────────────────
    def _run_flask():
        _app_module.app.run(
            debug        = False,
            host         = _host,
            port         = _port,
            threaded     = True,
            use_reloader = False,
        )

    threading.Thread(target=_run_flask, daemon=True).start()

    # ── Auto-open browser once Flask is ready ─────────────────────────────────
    def _open_browser():
        time.sleep(2)
        webbrowser.open(f"http://127.0.0.1:{_port}")

    threading.Thread(target=_open_browser, daemon=True).start()

    # ── Build tray tooltip with LAN address if network access is enabled ───────
    _tooltip = f"SDRLogger+ v{_app_module.VERSION}  —  port {_port}"
    if _host == "0.0.0.0":
        try:
            _lan_ip = socket.gethostbyname(socket.gethostname())
            _tooltip += f"\nLAN: http://{_lan_ip}:{_port}"
        except Exception:
            pass

    # ── System tray icon ──────────────────────────────────────────────────────
    import pystray
    from PIL import Image

    # Load icon — prefer static/img/sdrLogger_icon.png bundled alongside the exe
    _ico_path = os.path.join(_bundle, "static", "img", "sdrLogger_icon.png")
    if os.path.exists(_ico_path):
        _tray_img = Image.open(_ico_path).resize((64, 64)).convert("RGBA")
    else:
        # Fallback: plain cyan square so the tray icon is never blank
        _tray_img = Image.new("RGBA", (64, 64), (0, 229, 255, 255))

    def _on_open(icon, item):
        webbrowser.open(f"http://127.0.0.1:{_port}")

    def _on_quit(icon, item):
        icon.stop()
        os._exit(0)

    _tray = pystray.Icon(
        "SDRLogger+",
        _tray_img,
        _tooltip,
        menu=pystray.Menu(
            pystray.MenuItem("Open SDRLogger+", _on_open, default=True),
            pystray.MenuItem("Stop SDRLogger+", _on_quit),
        ),
    )

    # run() blocks the main thread — that's intentional.
    # The app stays alive as long as the tray icon is present.
    _tray.run()
