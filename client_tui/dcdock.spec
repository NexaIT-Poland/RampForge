# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec file for DCDock TUI client.

Build portable executable:
    pyinstaller dcdock.spec

Output in dist/ directory.
"""

import sys
from pathlib import Path

block_cipher = None

# Get the app directory
app_dir = Path('app')

a = Analysis(
    ['run.py'],
    pathex=[],
    binaries=[],
    datas=[
        # Include app package
        (str(app_dir), 'app'),
    ],
    hiddenimports=[
        'app',
        'app.main',
        'app.screens',
        'app.screens.login',
        'app.screens.board',
        'app.services',
        'app.services.api_client',
        'app.services.websocket_client',
        'textual',
        'httpx',
        'websockets',
        'pydantic',
        'rich',
        'markdown_it',
        'pygments',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'matplotlib',
        'numpy',
        'pandas',
        'scipy',
        'PIL',
        'tk',
        'tcl',
        'tkinter',
        '_tkinter',
        'PyQt5',
        'PyQt6',
        'PySide2',
        'PySide6',
        'wx',
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
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='dcdock',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    # Icon file (optional - add dcdock.ico for Windows, dcdock.icns for macOS)
    # icon='dcdock.ico',
)

# macOS app bundle
if sys.platform == 'darwin':
    app = BUNDLE(
        exe,
        name='dcdock.app',
        icon=None,  # Add 'dcdock.icns' if available
        bundle_identifier='com.dcdock.tui',
        info_plist={
            'NSPrincipalClass': 'NSApplication',
            'NSHighResolutionCapable': 'True',
            'CFBundleName': 'DCDock',
            'CFBundleDisplayName': 'DCDock TUI',
            'CFBundleShortVersionString': '0.1.0',
            'CFBundleVersion': '0.1.0',
        },
    )
