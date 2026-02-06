"""
Multi-Datensatz H3 Converter
-----------------------------
Liest mehrere Geodaten-Dateien, fuegt sie zusammen und konvertiert
alle Geometrien zu H3-Zellen mit adaptiver Aufloesung.

Konvertierungslogik:
  - Polygon / MultiPolygon:        adaptive Resolution (target_cells, min/max_resolution).
                                   MultiPolygone werden intern in Einzelpolygone
                                   aufgesplittet, konvertiert, und die Cells werden
                                   wieder zu einem Set zusammengefuegt.
                                   Jede Zeile im Input bleibt eine Zeile im Output.
  - Point / MultiPoint:            feste Resolution = max_resolution
  - LineString / MultiLineString:  feste Resolution = max_resolution

Konfiguration:
  Beim Start wird config.yaml aus dem Projektverzeichnis geladen.
  Die Defaults werden angezeigt und der User kann sie akzeptieren [y]
  oder interaktiv ueberschreiben [n].

Verwendung:
  Aus dem Projektverzeichnis ausfuehren:
      python scripts/convert_h3_multi.py
"""

import sys
import time
from pathlib import Path

import yaml
import geopandas as gpd
import pandas as pd

# Projektverzeichnis zum Importpfad hinzufuegen
sys.path.insert(0, str(Path(__file__).parent.parent))
from converter.converter import convert_geodataframe_to_h3_adaptive, ContainmentMode


# ---------------------------------------------------------------------------
# Konstanten
# ---------------------------------------------------------------------------

# config.yaml liegt im Projektverzeichnis (Elternverzeichnis von scripts/)
CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"

# Gueltige Containment-Modi aus der ContainmentMode-Enum
VALID_CONTAINMENT_MODES: dict[str, ContainmentMode] = {
    m.value: m for m in ContainmentMode
}


# ---------------------------------------------------------------------------
# Config laden
# ---------------------------------------------------------------------------


def load_config(path: Path) -> dict | None:
    """Liest config.yaml. Gibt None zurueck wenn die Datei nicht existiert."""
    if not path.exists():
        return None
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Interaktive Eingabe
# ---------------------------------------------------------------------------


def _prompt_line(label: str) -> str:
    """Liest eine nicht-leere Zeile vom User."""
    while True:
        val = input(f"  {label}: ").strip()
        if val:
            return val
        print("    (nicht leer)")


def prompt_input_files() -> list[str]:
    """Fragt nach Input-Dateien, eine pro Zeile. Leere Zeile beendet die Eingabe."""
    print("\n  Input-Dateien (eine pro Zeile, leere Zeile zum Abschluss):")
    files: list[str] = []
    while True:
        val = input(f"    Datei {len(files) + 1}: ").strip()
        if not val:
            if not files:
                print("      Mindestens eine Datei erforderlich.")
                continue
            break
        files.append(val)
    return files


def prompt_int(
    label: str, min_val: int | None = None, max_val: int | None = None
) -> int:
    """Fragt nach einem Integer mit optionalen Grenzen."""
    while True:
        raw = input(f"  {label}: ").strip()
        try:
            val = int(raw)
        except ValueError:
            print("    Ungueltige Eingabe -- bitte eine Ganzzahl.")
            continue
        if min_val is not None and val < min_val:
            print(f"    Minimum: {min_val}")
            continue
        if max_val is not None and val > max_val:
            print(f"    Maximum: {max_val}")
            continue
        return val


def prompt_containment_mode() -> str:
    """Fragt nach dem Containment-Modus aus den gueltigen Optionen."""
    options = list(VALID_CONTAINMENT_MODES.keys())
    print(f"  Containment-Modus -- gueltige Optionen: {options}")
    while True:
        val = input("    Auswahl: ").strip().lower()
        if val in VALID_CONTAINMENT_MODES:
            return val
        print(f"    Gueltige Optionen: {options}")


def collect_params_interactive() -> dict:
    """Sammelt alle Parameter interaktiv vom User."""
    print("\n" + "-" * 60)
    print("  Parameter interaktiv eingeben")
    print("-" * 60)

    input_files = prompt_input_files()
    output_file = _prompt_line("Output-Datei (GPKG-Pfad)")
    target_cells = prompt_int("Target Cells (Ziel pro Polygon)", min_val=1)
    min_res = prompt_int("Min Resolution (0-15)", min_val=0, max_val=15)
    max_res = prompt_int("Max Resolution (0-15)", min_val=0, max_val=15)
    containment = prompt_containment_mode()

    return {
        "input_files": input_files,
        "output_file": output_file,
        "target_cells": target_cells,
        "min_resolution": min_res,
        "max_resolution": max_res,
        "containment_mode": containment,
    }


# ---------------------------------------------------------------------------
# Anzeige + Validierung
# ---------------------------------------------------------------------------


def display_config(config: dict) -> None:
    """Gibt die Konfiguration formatiert aus."""
    print("\n" + "=" * 60)
    print("  Konfiguration (aus config.yaml)")
    print("=" * 60)
    print("  Input-Dateien:")
    for f in config["input_files"]:
        print(f"      - {f}")
    print(f"  Output-Datei:       {config['output_file']}")
    print(f"  Target Cells:       {config['target_cells']}")
    print(f"  Min Resolution:     {config['min_resolution']}")
    print(f"  Max Resolution:     {config['max_resolution']}")
    print(f"  Containment-Modus:  {config['containment_mode']}")
    print("=" * 60)


def validate_config(config: dict) -> None:
    """Prueft die Konfiguration auf Fehler. Gibt eine Liste aus und beendet das Skript falls noetig."""
    errors: list[str] = []

    if not config.get("input_files"):
        errors.append("Keine Input-Dateien angegeben.")

    if not config.get("output_file"):
        errors.append("Kein Output-Pfad angegeben.")

    if config.get("min_resolution", 0) > config.get("max_resolution", 15):
        errors.append(
            f"min_resolution ({config['min_resolution']}) > max_resolution ({config['max_resolution']})"
        )

    mode = config.get("containment_mode")
    if mode not in VALID_CONTAINMENT_MODES:
        errors.append(
            f"Ungueltiger containment_mode: '{mode}'. "
            f"Gueltig: {list(VALID_CONTAINMENT_MODES.keys())}"
        )

    for f in config.get("input_files", []):
        if not Path(f).exists():
            errors.append(f"Input-Datei nicht gefunden: {f}")

    if errors:
        print("\nFehler in der Konfiguration:")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Hauptlogik
# ---------------------------------------------------------------------------


def run_conversion(config: dict) -> None:
    """Laden -- Zusammenfuegen -- Konvertieren -- Speichern."""

    input_paths = [Path(f) for f in config["input_files"]]
    output_path = Path(config["output_file"])
    target_cells = config["target_cells"]
    min_res = config["min_resolution"]
    max_res = config["max_resolution"]
    containment_mode = VALID_CONTAINMENT_MODES[config["containment_mode"]]

    # ------------------------------------------------------------------
    # 1. Laden & Zusammenfuegen
    # ------------------------------------------------------------------
    print(f"\n1. Laden der {len(input_paths)} Datensätze...")

    gdfs: list[gpd.GeoDataFrame] = []
    for path in input_paths:
        print(f"   Lese: {path}")
        gdf = gpd.read_file(path)
        gdf["_source_file"] = path.name
        geom_summary = dict(gdf.geometry.geom_type.value_counts())
        print(f"     -> {len(gdf)} Zeilen | CRS: {gdf.crs} | Typen: {geom_summary}")
        gdfs.append(gdf)

    # CRS-Konsistenz pruefen: alle auf das CRS des ersten reprojizieren falls noetig
    crs_set = {str(g.crs) for g in gdfs}
    if len(crs_set) > 1:
        print(f"\n   Warnung: unterschiedliche CRS erkannt: {crs_set}")
        print(
            "   Alle Datensätze werden auf das CRS des ersten Datensatzes reprojiziert."
        )
        target_crs = gdfs[0].crs
        gdfs = [
            g.to_crs(target_crs) if str(g.crs) != str(target_crs) else g for g in gdfs
        ]

    combined = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)
    print(f"\n   Zusammengefuegt: {len(combined)} Zeilen")
    print(f"   Geometrie-Typen: {dict(combined.geometry.geom_type.value_counts())}")

    # ------------------------------------------------------------------
    # 2. H3-Konvertierung
    # ------------------------------------------------------------------
    print(f"\n2. H3-Konvertierung (adaptiv)...")
    print(
        f"   Polygone/MultiPolygone : adaptiv | Ziel {target_cells} Cells | Res {min_res}-{max_res}"
    )
    print(f"   Punkte/Linien/Multi*   : feste Resolution {max_res}")
    print(f"   Containment-Modus      : {containment_mode.value}")

    start = time.time()
    h3_cells, resolutions = convert_geodataframe_to_h3_adaptive(
        combined,
        target_cells=target_cells,
        min_resolution=min_res,
        max_resolution=max_res,
        containment_mode=containment_mode,
    )
    elapsed = time.time() - start

    combined["h3_cells"] = h3_cells
    combined["h3_resolution"] = resolutions
    combined["h3_cell_count"] = [len(s) for s in h3_cells]

    print(
        f"\n   Konvertierung: {elapsed:.2f} s  ({elapsed / len(combined) * 1000:.1f} ms/Zeile)"
    )

    # ------------------------------------------------------------------
    # 3. Statistiken
    # ------------------------------------------------------------------
    print(f"\n3. Statistiken:")
    print(f"   Gesamt H3-Cells:          {combined['h3_cell_count'].sum():,}")
    print(f"   Durchschnitt Cells/Zeile: {combined['h3_cell_count'].mean():.0f}")
    print(
        f"   Min / Max Cells:          {combined['h3_cell_count'].min()} / {combined['h3_cell_count'].max()}"
    )

    print(f"\n   Resolution-Verteilung:")
    for res in sorted(combined["h3_resolution"].unique()):
        count = int((combined["h3_resolution"] == res).sum())
        print(f"     Res {res:2d}: {count:5d} Zeilen")

    print(f"\n   Pro Quelldatei:")
    for src in combined["_source_file"].unique():
        mask = combined["_source_file"] == src
        sub = combined.loc[mask]
        print(
            f"     {src:40s} {mask.sum():5d} Zeilen | "
            f"{sub['h3_cell_count'].sum():>8,} Cells | "
            f"Res {sub['h3_resolution'].min()}-{sub['h3_resolution'].max()}"
        )

    # ------------------------------------------------------------------
    # 4. Speichern
    # ------------------------------------------------------------------
    print(f"\n4. Speichere: {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    save_gdf = combined.copy()
    # h3_cells: Set -> Semikolon-getrennter String
    # GPKG unterstuetzt keine native Listen/Arrays.
    # Zuruecklesen: spalte.str.split(";")
    save_gdf["h3_cells"] = save_gdf["h3_cells"].apply(lambda s: ";".join(sorted(s)))
    save_gdf.to_file(output_path, driver="GPKG")

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"   Gespeichert: {size_mb:.2f} MB")

    print("\n" + "=" * 60)
    print("  Fertig.")
    print("=" * 60)
    print(f"\n  Output:  {output_path}")
    print(f"  Spalten: h3_cells, h3_resolution, h3_cell_count, _source_file")
    print(f"  Hinweis: h3_cells ist semikolon-getrennt -> .split(';') zum Einlesen")


# ---------------------------------------------------------------------------
# Entry-Point
# ---------------------------------------------------------------------------


def main():
    print("\n" + "=" * 60)
    print("  H3 Multi-Datensatz Converter")
    print("=" * 60)

    config = load_config(CONFIG_PATH)

    if config:
        display_config(config)
        choice = input("\nConfig-Defaults verwenden? [y/n]: ").strip().lower()
        if choice != "y":
            config = collect_params_interactive()
    else:
        print(f"\n  Keine config.yaml gefunden ({CONFIG_PATH})")
        print("  Parameter werden interaktiv eingegeben.")
        config = collect_params_interactive()

    validate_config(config)
    run_conversion(config)


if __name__ == "__main__":
    main()
