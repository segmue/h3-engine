"""
Unified H3 Converter with Direct DuckDB Import
-----------------------------------------------
Liest mehrere Geodaten-Dateien, fuegt sie zusammen, konvertiert zu H3-Zellen
und importiert direkt in DuckDB - OHNE intermediäre GPKG-Datei.

Konvertierungslogik:
  - Polygon / MultiPolygon:        adaptive Resolution (target_cells, min/max_resolution)
  - Point / MultiPoint:            feste Resolution = max_resolution
  - LineString / MultiLineString:  feste Resolution = max_resolution

Features:
  - Direkte DuckDB-Integration (keine GPKG-Zwischendatei)
  - DuckDB Spatial Extension mit GEOMETRY-Typ
  - H3 Cells als UBIGINT[] Arrays
  - Vektorisierte Bulk-Operationen

Verwendung:
  python scripts/convert_and_import.py
"""

import sys
import time
from pathlib import Path

import duckdb
import geopandas as gpd
import pandas as pd
import yaml

# Projektverzeichnis zum Importpfad hinzufuegen
sys.path.insert(0, str(Path(__file__).parent.parent))
from converter.converter import convert_geodataframe_to_h3, ContainmentMode


# ---------------------------------------------------------------------------
# Konstanten
# ---------------------------------------------------------------------------

CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"

VALID_CONTAINMENT_MODES: dict[str, ContainmentMode] = {
    m.value: m for m in ContainmentMode
}


# ---------------------------------------------------------------------------
# Config
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
    output_file = _prompt_line("Output-Datei (DuckDB-Pfad, .duckdb)")
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
    """Prueft die Konfiguration auf Fehler."""
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
# DuckDB Setup
# ---------------------------------------------------------------------------


def setup_duckdb(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Erstellt DuckDB Connection und laedt Spatial + H3 Extensions."""
    conn = duckdb.connect(str(db_path))

    # Spatial Extension laden (fuer GEOMETRY-Typ und ST_* Funktionen)
    print("   Lade Spatial Extension...")
    conn.execute("INSTALL spatial FROM core;")
    conn.execute("LOAD spatial;")

    # H3 Extension laden
    print("   Lade H3 Extension...")
    conn.execute("INSTALL h3 FROM community;")
    conn.execute("LOAD h3;")

    return conn


def create_features_table(
    conn: duckdb.DuckDBPyConnection, sample_gdf: gpd.GeoDataFrame
) -> None:
    """Erstellt die features Tabelle mit GEOMETRY-Typ und H3 Arrays."""
    columns = []

    for col in sample_gdf.columns:
        if col == "geometry":
            columns.append("geometry GEOMETRY")  # Native GEOMETRY-Typ!
        elif col == "h3_cells":
            columns.append("h3_cells UBIGINT[]")
        elif col == "h3_resolution":
            columns.append("h3_resolution TINYINT")
        elif col == "h3_cell_count":
            columns.append("h3_cell_count INTEGER")
        else:
            dtype = sample_gdf[col].dtype
            if dtype == "int64":
                columns.append(f'"{col}" BIGINT')
            elif dtype == "float64":
                columns.append(f'"{col}" DOUBLE')
            else:
                columns.append(f'"{col}" VARCHAR')

    columns_sql = ",\n    ".join(columns)

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS features (
            feature_id INTEGER PRIMARY KEY,
            {columns_sql}
        );
    """)


# ---------------------------------------------------------------------------
# Load & Convert
# ---------------------------------------------------------------------------


def load_and_merge_geodata(config: dict) -> gpd.GeoDataFrame:
    """Laedt und fuegt alle Input-Dateien zusammen."""
    input_paths = [Path(f) for f in config["input_files"]]

    print(f"\n1. Laden der {len(input_paths)} Datensätze...")

    gdfs: list[gpd.GeoDataFrame] = []
    for path in input_paths:
        print(f"   Lese: {path}")
        gdf = gpd.read_file(path)
        gdf["_source_file"] = path.name
        geom_summary = dict(gdf.geometry.geom_type.value_counts())
        print(f"     -> {len(gdf)} Zeilen | CRS: {gdf.crs} | Typen: {geom_summary}")
        gdfs.append(gdf)

    # CRS-Konsistenz
    crs_set = {str(g.crs) for g in gdfs}
    if len(crs_set) > 1:
        print(f"\n   Warnung: unterschiedliche CRS erkannt: {crs_set}")
        print("   Alle Datensätze werden auf das CRS des ersten reprojiziert.")
        target_crs = gdfs[0].crs
        gdfs = [
            g.to_crs(target_crs) if str(g.crs) != str(target_crs) else g for g in gdfs
        ]

    combined = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)
    print(f"\n   Zusammengefuegt: {len(combined)} Zeilen")
    print(f"   Geometrie-Typen: {dict(combined.geometry.geom_type.value_counts())}")

    return combined


def convert_to_h3(gdf: gpd.GeoDataFrame, config: dict) -> gpd.GeoDataFrame:
    """Konvertiert Geometrien zu H3-Zellen (adaptiv)."""
    target_cells = config["target_cells"]
    min_res = config["min_resolution"]
    max_res = config["max_resolution"]
    containment_mode = VALID_CONTAINMENT_MODES[config["containment_mode"]]

    print(f"\n2. H3-Konvertierung (adaptiv)...")
    print(
        f"   Polygone/MultiPolygone : adaptiv | Ziel {target_cells} Cells | Res {min_res}-{max_res}"
    )
    print(f"   Punkte/Linien/Multi*   : feste Resolution {max_res}")
    print(f"   Containment-Modus      : {containment_mode.value}")

    start = time.time()
    h3_cells, resolutions = convert_geodataframe_to_h3(
        gdf,
        target_cells=target_cells,
        min_resolution=min_res,
        max_resolution=max_res,
        containment_mode=containment_mode,
    )
    elapsed = time.time() - start

    gdf["h3_cells"] = h3_cells
    gdf["h3_resolution"] = resolutions
    gdf["h3_cell_count"] = [len(s) for s in h3_cells]

    print(
        f"\n   Konvertierung: {elapsed:.2f} s  ({elapsed / len(gdf) * 1000:.1f} ms/Zeile)"
    )

    return gdf


def print_statistics(gdf: gpd.GeoDataFrame) -> None:
    """Gibt Statistiken ueber die konvertierten Daten aus."""
    print(f"\n3. Statistiken:")
    print(f"   Gesamt H3-Cells:          {gdf['h3_cell_count'].sum():,}")
    print(f"   Durchschnitt Cells/Zeile: {gdf['h3_cell_count'].mean():.0f}")
    print(
        f"   Min / Max Cells:          {gdf['h3_cell_count'].min()} / {gdf['h3_cell_count'].max()}"
    )

    print(f"\n   Resolution-Verteilung:")
    for res in sorted(gdf["h3_resolution"].unique()):
        count = int((gdf["h3_resolution"] == res).sum())
        print(f"     Res {res:2d}: {count:5d} Zeilen")

    print(f"\n   Pro Quelldatei:")
    for src in gdf["_source_file"].unique():
        mask = gdf["_source_file"] == src
        sub = gdf.loc[mask]
        print(
            f"     {src:40s} {mask.sum():5d} Zeilen | "
            f"{sub['h3_cell_count'].sum():>8,} Cells | "
            f"Res {sub['h3_resolution'].min()}-{sub['h3_resolution'].max()}"
        )


# ---------------------------------------------------------------------------
# DuckDB Import
# ---------------------------------------------------------------------------


def prepare_dataframe_for_duckdb(gdf: gpd.GeoDataFrame) -> tuple[pd.DataFrame, list[str], list[str]]:
    """
    Bereitet GeoDataFrame fuer DuckDB-Import vor.

    - Geometrien werden als WKB-Bytes gespeichert (fuer ST_GeomFromWKB)
    - H3-Cells werden als Semikolon-Strings gespeichert (DuckDB konvertiert vektorisiert)

    Returns:
        (DataFrame, insert_columns, select_expressions)
    """
    df = pd.DataFrame({"feature_id": range(len(gdf))})

    insert_cols = ["feature_id"]
    select_exprs = ["feature_id"]

    for col in gdf.columns:
        if col == "geometry":
            # Geometrie → WKB Bytes
            df["geometry_wkb"] = gdf.geometry.apply(lambda g: g.wkb if g else None)
            insert_cols.append("geometry")
            # ST_GeomFromWKB konvertiert WKB → GEOMETRY
            select_exprs.append("ST_GeomFromWKB(geometry_wkb) AS geometry")

        elif col == "h3_cells":
            # Set → Semikolon-String (DuckDB konvertiert vektorisiert zu Array)
            df["h3_cells_raw"] = gdf["h3_cells"].apply(
                lambda s: ";".join(sorted(s)) if s else ""
            )
            insert_cols.append("h3_cells")
            select_exprs.append(
                "CASE WHEN h3_cells_raw = '' THEN []::UBIGINT[] "
                "ELSE list_transform("
                "  list_filter(str_split(h3_cells_raw, ';'), x -> x != ''),"
                "  x -> h3_string_to_h3(x)"
                ") END AS h3_cells"
            )

        else:
            df[col] = gdf[col]
            insert_cols.append(f'"{col}"')
            select_exprs.append(f'"{col}"')

    return df, insert_cols, select_exprs


def import_to_duckdb(
    conn: duckdb.DuckDBPyConnection, gdf: gpd.GeoDataFrame
) -> None:
    """Bulk-Import aller Features in DuckDB mit vektorisierten Transformationen."""
    print(f"\n4. DuckDB Bulk-Import...")
    print(f"   Bereite {len(gdf):,} Features vor...")

    start = time.time()
    df, insert_cols, select_exprs = prepare_dataframe_for_duckdb(gdf)
    print(f"   DataFrame vorbereitet in {time.time() - start:.1f}s")

    print(f"   Starte vektorisierten Import...")
    start = time.time()

    # DataFrame als temporaere View registrieren
    conn.register("raw_import", df)

    # Ein einziger INSERT ... SELECT mit DuckDB-seitiger Transformation
    insert_sql = ", ".join(insert_cols)
    select_sql = ", ".join(select_exprs)

    conn.execute(f"""
        INSERT INTO features ({insert_sql})
        SELECT {select_sql}
        FROM raw_import
    """)

    conn.unregister("raw_import")

    elapsed = time.time() - start
    total_cells = conn.execute(
        "SELECT COALESCE(SUM(h3_cell_count), 0) FROM features"
    ).fetchone()[0]

    print(
        f"   {len(gdf):,} Features mit {total_cells:,} H3 Cells "
        f"importiert in {elapsed:.1f}s"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run_unified_conversion(config: dict) -> None:
    """Hauptfunktion: Laden → Konvertieren → Direkt in DuckDB schreiben."""

    # Output-Pfad
    output_path = Path(config["output_file"])
    if not output_path.suffix == ".duckdb":
        output_path = output_path.with_suffix(".duckdb")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Falls DB existiert, loeschen
    if output_path.exists():
        print(f"\n  Existierende DB wird ueberschrieben: {output_path}")
        output_path.unlink()

    # Phase 1: Laden & Zusammenfuegen
    combined = load_and_merge_geodata(config)

    # Phase 2: H3-Konvertierung
    combined = convert_to_h3(combined, config)

    # Phase 3: Statistiken
    print_statistics(combined)

    # Phase 4: DuckDB Setup
    print(f"\n4. DuckDB Setup...")
    print(f"   Erstelle: {output_path}")
    conn = setup_duckdb(output_path)
    create_features_table(conn, combined)

    # Phase 5: Direkt-Import
    import_to_duckdb(conn, combined)

    # Phase 6: Finale Statistiken
    print(f"\n5. Finale DuckDB-Statistiken:")
    result = conn.execute("SELECT COUNT(*) FROM features").fetchone()
    print(f"   Features:  {result[0]:,}")

    result = conn.execute("SELECT SUM(h3_cell_count) FROM features").fetchone()
    print(f"   H3 Cells:  {result[0]:,}")

    result = conn.execute("""
        SELECT h3_resolution, COUNT(*) as cnt, SUM(h3_cell_count) as cells
        FROM features
        GROUP BY h3_resolution
        ORDER BY h3_resolution
    """).fetchall()
    print(f"   Pro Resolution:")
    for res, cnt, cells in result:
        print(f"     Res {res:2d}: {cnt:>8,} Features, {cells:>12,} Cells")

    # DB Size
    conn.close()
    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"\n   DB Groesse: {size_mb:.1f} MB")

    print("\n" + "=" * 60)
    print("  Fertig!")
    print("=" * 60)
    print(f"\n  Output: {output_path}")
    print(f"  Features:")
    print(f"    - Geometrien als GEOMETRY-Typ (Spatial Extension)")
    print(f"    - H3 Cells als UBIGINT[] Arrays")
    print(f"    - Spatial Indexing verfuegbar")
    print(f"    - ST_* Funktionen verfuegbar")


def main():
    print("\n" + "=" * 60)
    print("  H3 Unified Converter → DuckDB")
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

    start_total = time.time()
    run_unified_conversion(config)

    print(f"\n  Gesamtzeit: {time.time() - start_total:.1f}s")


if __name__ == "__main__":
    main()
