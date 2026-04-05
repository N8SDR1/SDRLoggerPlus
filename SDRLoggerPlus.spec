# -*- mode: python ; coding: utf-8 -*-
#
# SDRLogger+ — PyInstaller spec file
#
# Build command (from the hamlog/ directory):
#   pip install pyinstaller
#   pyinstaller SDRLoggerPlus.spec
#
# Output: dist/SDRLoggerPlus/   (a folder — feed this to Inno Setup / DMG builder)

import sys
import os

block_cipher = None

# ── Collect Flask & Jinja2 hidden imports ─────────────────────────────────────
# Flask's template engine and Werkzeug use a lot of dynamic imports that
# PyInstaller's static analysis can miss.
hidden = [
    "flask",
    "flask.templating",
    "jinja2",
    "jinja2.ext",
    "werkzeug",
    "werkzeug.serving",
    "werkzeug.routing",
    "werkzeug.middleware.proxy_fix",
    "flask_sock",
    "simple_websocket",
    "requests",
    "requests.adapters",
    "urllib3",
    "urllib3.util.retry",
    "sqlite3",
    "xml.etree.ElementTree",
    "email.mime.text",
    "email.mime.multipart",
    "pystray",
    "pystray._win32",
    "PIL",
    "PIL.Image",
]

# ── Data files to bundle ───────────────────────────────────────────────────────
# Format: (source_glob_or_path, dest_folder_inside_bundle)
datas = [
    # HTML templates
    (os.path.join("templates", "*.html"), "templates"),
    # main.py must be importable at runtime by launcher.py
    ("main.py", "."),
    # Default server config — seeded to user data dir on first run
    ("config.json", "."),
    # Entire static folder — icons, images, future additions all included automatically
    ("static", "static"),
]

# ── Optional: add your icon asset if it exists ────────────────────────────────
_ico = os.path.join("static", "img", "sdrlogger.ico")
_icon_arg = _ico if os.path.exists(_ico) else None

# ── Analysis ──────────────────────────────────────────────────────────────────
a = Analysis(
    ["launcher.py"],
    pathex=["."],
    binaries=[],
    datas=datas,
    hiddenimports=hidden,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        "tkinter",
        "matplotlib",
        "numpy",
        "pandas",
        "scipy",
        "PyQt5",
        "PyQt6",
        "PySide2",
        "PySide6",
        "wx",
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="SDRLoggerPlus",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    # console=False → no terminal window; app runs silently in the system tray
    console=False,
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=_icon_arg,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="SDRLoggerPlus",
)

# ── macOS: wrap in a .app bundle ──────────────────────────────────────────────
# Only active when building on macOS (PyInstaller ignores BUNDLE on Windows/Linux)
_icns = os.path.join("static", "img", "SDRLoggerPlus.icns")

app = BUNDLE(
    coll,
    name="SDRLoggerPlus.app",
    icon=_icns if os.path.exists(_icns) else None,
    bundle_identifier="com.n8sdr.sdrloggerplus",
    info_plist={
        "CFBundleName": "SDRLogger+",
        "CFBundleDisplayName": "SDRLogger+",
        "CFBundleShortVersionString": "0.43",
        "CFBundleVersion": "0.43",
        "NSHumanReadableCopyright": "Rick N8SDR",
        "LSUIElement": False,           # show in Dock
        "NSHighResolutionCapable": True,
    },
)
