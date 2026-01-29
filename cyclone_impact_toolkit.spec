# -*- mode: python ; coding: utf-8 -*-

import os
import pkgutil

# Import geopandas to get its path for submodule discovery
try:
    import geopandas
    import fiona
    import rasterio
except ImportError:
    geopandas = None
    fiona = None
    rasterio = None

# List all geopandas, fiona, and rasterio submodules to include them in the package
additional_packages = list()

if geopandas:
    for package in pkgutil.iter_modules(geopandas.__path__, prefix="geopandas."):
        additional_packages.append(package.name)

if fiona:
    for package in pkgutil.iter_modules(fiona.__path__, prefix="fiona."):
        additional_packages.append(package.name)

if rasterio:
    for package in pkgutil.iter_modules(rasterio.__path__, prefix="rasterio."):
        additional_packages.append(package.name)

# Add other common hidden imports that might be needed
additional_packages.extend([
    'shapely',
    'shapely.geometry',
    'shapely.geos',
    'pyproj',
    'webview',
    'tinydb',
    'pandas',
    'numpy',
])

a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('frontend/static', 'frontend/static'),
        ('database', 'database'),
        ('app.ico', '.'),
    ],
    hiddenimports=additional_packages,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='cyclone_impact_toolkit',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,  # GUI application, no console window
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='app.ico'
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='cyclone_impact_toolkit',
)

