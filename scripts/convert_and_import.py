"""
Unified H3 Converter with Direct DuckDB Import
-----------------------------------------------
Liest Geodaten-Dateien (ein oder mehrere Datasets), konvertiert zu H3-Zellen
und importiert direkt in DuckDB.

Unterstuetzt Multi-Dataset-Konfiguration mit Rollen:
  - target:          Kern-Datensatz (wird beschrieben)
  - static_context:  Statischer Kontext (z.B. Gemeinde, Kanton)
  - dynamic_context: (Zukunft) Dynamischer Kontext via Assoziationsmatrix

Konvertierungslogik:
  - Polygon / MultiPolygon:        adaptive Resolution (target_cells, min/max_resolution)
  - Point / MultiPoint:            feste Resolution = max_resolution
  - LineString / MultiLineString:  feste Resolution = max_resolution

Verwendung:
  python scripts/convert_and_import.py
"""

import sys
import time
from pathlib import Path

import duckdb
import geopandas as gpd
import pandas as pd
import shapely
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


def normalize_config(config: dict) -> dict:
    """Konvertiert alte config (input_files) zu neuem datasets-Format.

    Abwaertskompatibel: Wenn 'datasets' fehlt, wird 'input_files' als
    einzelnes target-Dataset interpretiert.
    """
    if "datasets" in config:
        return config

    # Legacy-Format: input_files → einzelnes target-Dataset
    return {
        **config,
        "datasets": [
            {
                "name": "swissnames3d",
                "role": "target",
                "files": config["input_files"],
                "name_field": "NAME",
            }
        ],
    }


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

    datasets = config.get("datasets", [])
    for ds in datasets:
        role = ds.get("role", "target")
        print(f"\n  Dataset: {ds['name']} ({role})")
        for f in ds.get("files", []):
            print(f"      - {f}")
        if "label" in ds:
            print(f"      Label: {ds['label']}, Slots: {ds.get('slots', '-')}")

    print(f"\n  Output-Datei:       {config['output_file']}")
    print(f"  Target Cells:       {config['target_cells']}")
    print(f"  Min Resolution:     {config['min_resolution']}")
    print(f"  Max Resolution:     {config['max_resolution']}")
    print(f"  Containment-Modus:  {config['containment_mode']}")
    print("=" * 60)


def validate_config(config: dict) -> None:
    """Prueft die Konfiguration auf Fehler."""
    errors: list[str] = []

    datasets = config.get("datasets", [])
    if not datasets:
        errors.append("Keine Datasets konfiguriert.")

    target_count = sum(1 for ds in datasets if ds.get("role") == "target")
    if target_count == 0:
        errors.append("Kein Dataset mit role='target' gefunden.")
    elif target_count > 1:
        errors.append("Nur ein Dataset mit role='target' erlaubt.")

    for ds in datasets:
        if not ds.get("files"):
            errors.append(f"Dataset '{ds.get('name', '?')}': Keine Dateien angegeben.")
        for f in ds.get("files", []):
            if not Path(f).exists():
                errors.append(f"Dataset '{ds['name']}': Datei nicht gefunden: {f}")
        if ds.get("role") in ("static_context", "dynamic_context"):
            if "slots" not in ds:
                errors.append(f"Dataset '{ds['name']}': 'slots' fehlt fuer {ds['role']}.")
            if "label" not in ds:
                errors.append(f"Dataset '{ds['name']}': 'label' fehlt fuer {ds['role']}.")

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
    """Erstellt die features Tabelle mit GEOMETRY-Typ, H3 Arrays und dataset-Spalte."""
    columns = ["dataset VARCHAR"]

    for col in sample_gdf.columns:
        if col == "geometry":
            columns.append("geometry GEOMETRY")
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


def ensure_columns_exist(
    conn: duckdb.DuckDBPyConnection, gdf: gpd.GeoDataFrame
) -> None:
    """Fuegt fehlende Spalten zur features-Tabelle hinzu (fuer Multi-Dataset-Import)."""
    existing = {
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='features'"
        ).fetchall()
    }

    for col in gdf.columns:
        if col in ("geometry", "h3_cells", "h3_resolution", "h3_cell_count"):
            continue
        if col.lower() in {c.lower() for c in existing}:
            continue

        dtype = gdf[col].dtype
        if dtype == "int64":
            sql_type = "BIGINT"
        elif dtype == "float64":
            sql_type = "DOUBLE"
        else:
            sql_type = "VARCHAR"

        conn.execute(f'ALTER TABLE features ADD COLUMN "{col}" {sql_type};')
        print(f"     + Spalte '{col}' ({sql_type}) hinzugefuegt")


# ---------------------------------------------------------------------------
# Load & Convert
# ---------------------------------------------------------------------------


def load_dataset_geodata(dataset: dict) -> gpd.GeoDataFrame:
    """Laedt und fuegt alle Dateien eines Datasets zusammen."""
    input_paths = [Path(f) for f in dataset["files"]]
    ds_name = dataset["name"]

    print(f"\n   Laden: {ds_name} ({len(input_paths)} Datei(en))...")

    gdfs: list[gpd.GeoDataFrame] = []
    for path in input_paths:
        print(f"     Lese: {path}")
        gdf = gpd.read_file(path)

        # Z-Werte entfernen
        gdf.geometry = gdf.geometry.apply(lambda x: shapely.force_2d(x))

        # Transform to WGS84
        if gdf.crs and gdf.crs.to_string() != "EPSG:4326":
            print(f"       -> reproject from {gdf.crs} to EPSG:4326")
            gdf = gdf.to_crs("EPSG:4326")

        gdf["_source_file"] = path.name
        geom_summary = dict(gdf.geometry.geom_type.value_counts())
        print(f"       -> {len(gdf)} Zeilen | CRS: {gdf.crs} | Typen: {geom_summary}")
        gdfs.append(gdf)

    if len(gdfs) == 1:
        combined = gdfs[0]
    else:
        # CRS-Konsistenz
        crs_set = {str(g.crs) for g in gdfs}
        if len(crs_set) > 1:
            print(f"\n     Warnung: unterschiedliche CRS: {crs_set}")
            target_crs = gdfs[0].crs
            gdfs = [
                g.to_crs(target_crs) if str(g.crs) != str(target_crs) else g
                for g in gdfs
            ]
        combined = gpd.GeoDataFrame(
            pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs
        )

    # name_field Mapping: Quell-Spalte → NAME
    name_field = dataset.get("name_field", "NAME")
    if name_field != "NAME" and name_field in combined.columns:
        if "NAME" not in combined.columns:
            combined["NAME"] = combined[name_field]
        else:
            # NAME existiert schon (z.B. von einem vorherigen concat) — ueberschreiben
            combined["NAME"] = combined[name_field]

    print(f"     Zusammengefuegt: {len(combined)} Features")
    return combined


def convert_to_h3(gdf: gpd.GeoDataFrame, config: dict) -> gpd.GeoDataFrame:
    """Konvertiert Geometrien zu H3-Zellen (adaptiv)."""
    target_cells = config["target_cells"]
    min_res = config["min_resolution"]
    max_res = config["max_resolution"]
    containment_mode = VALID_CONTAINMENT_MODES[config["containment_mode"]]

    print(f"     H3-Konvertierung: Res {min_res}-{max_res}, "
          f"target={target_cells}, mode={containment_mode.value}")

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
        f"     {len(gdf):,} Features konvertiert in {elapsed:.1f}s "
        f"({elapsed / len(gdf) * 1000:.1f} ms/Feature)"
    )

    return gdf


def print_dataset_statistics(gdf: gpd.GeoDataFrame, ds_name: str) -> None:
    """Gibt Statistiken fuer ein einzelnes Dataset aus."""
    print(f"     Statistiken fuer '{ds_name}':")
    print(f"       Features:             {len(gdf):,}")
    print(f"       Gesamt H3-Cells:      {gdf['h3_cell_count'].sum():,}")
    print(f"       Durchschnitt Cells:   {gdf['h3_cell_count'].mean():.0f}")

    print(f"       Resolution-Verteilung:")
    for res in sorted(gdf["h3_resolution"].unique()):
        count = int((gdf["h3_resolution"] == res).sum())
        print(f"         Res {res:2d}: {count:5d} Features")


# ---------------------------------------------------------------------------
# DuckDB Import
# ---------------------------------------------------------------------------


def prepare_dataframe_for_duckdb(
    gdf: gpd.GeoDataFrame,
    dataset_name: str,
    feature_id_offset: int = 0,
) -> tuple[pd.DataFrame, list[str], list[str]]:
    """
    Bereitet GeoDataFrame fuer DuckDB-Import vor.

    Args:
        gdf: GeoDataFrame mit H3-Spalten
        dataset_name: Name des Datasets
        feature_id_offset: Start-Wert fuer feature_id (fuer Multi-Dataset)

    Returns:
        (DataFrame, insert_columns, select_expressions)
    """
    df = pd.DataFrame({
        "feature_id": range(feature_id_offset, feature_id_offset + len(gdf)),
        "dataset": dataset_name,
    })

    insert_cols = ["feature_id", "dataset"]
    select_exprs = ["feature_id", "dataset"]

    for col in gdf.columns:
        if col == "geometry":
            df["geometry_wkb"] = gdf.geometry.apply(lambda g: g.wkb if g else None)
            insert_cols.append("geometry")
            select_exprs.append("ST_GeomFromWKB(geometry_wkb) AS geometry")

        elif col == "h3_cells":
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
            df[col] = gdf[col].values
            insert_cols.append(f'"{col}"')
            select_exprs.append(f'"{col}"')

    return df, insert_cols, select_exprs


def import_dataset_to_duckdb(
    conn: duckdb.DuckDBPyConnection,
    gdf: gpd.GeoDataFrame,
    dataset_name: str,
    feature_id_offset: int,
) -> int:
    """Importiert ein Dataset in DuckDB.

    Returns:
        Naechster freier feature_id Wert (= offset + len(gdf))
    """
    print(f"     Import: {len(gdf):,} Features (IDs {feature_id_offset}..{feature_id_offset + len(gdf) - 1})...")

    start = time.time()
    df, insert_cols, select_exprs = prepare_dataframe_for_duckdb(
        gdf, dataset_name, feature_id_offset
    )

    conn.register("raw_import", df)

    insert_sql = ", ".join(insert_cols)
    select_sql = ", ".join(select_exprs)

    conn.execute(f"""
        INSERT INTO features ({insert_sql})
        SELECT {select_sql}
        FROM raw_import
    """)

    conn.unregister("raw_import")

    elapsed = time.time() - start
    print(f"     Importiert in {elapsed:.1f}s")

    return feature_id_offset + len(gdf)


def create_h3_lookup_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Erstellt die h3_lookup Tabelle fuer schnelle Intersection-Queries."""
    print(f"\n  Erstelle h3_lookup Tabelle...")

    start = time.time()
    conn.execute("DROP TABLE IF EXISTS h3_lookup;")
    conn.execute("""
        CREATE TABLE h3_lookup AS
        SELECT DISTINCT
            feature_id,
            UNNEST(h3_cells) as cell,
            h3_resolution as cell_res,
            dataset
        FROM features
        ORDER BY cell
    """)
    row_count = conn.execute("SELECT COUNT(*) FROM h3_lookup").fetchone()[0]
    print(f"  {row_count:,} Rows in {time.time() - start:.1f}s")

    # Statistik pro Dataset
    ds_stats = conn.execute("""
        SELECT dataset, COUNT(*) as cells, COUNT(DISTINCT feature_id) as features
        FROM h3_lookup
        GROUP BY dataset
        ORDER BY dataset
    """).fetchall()
    for ds, cells, features in ds_stats:
        print(f"    {ds}: {cells:,} cells, {features:,} features")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run_unified_conversion(config: dict) -> None:
    """Hauptfunktion: Pro Dataset laden, konvertieren, importieren."""

    # Output-Pfad
    output_path = Path(config["output_file"])
    if not output_path.suffix == ".duckdb":
        output_path = output_path.with_suffix(".duckdb")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Falls DB existiert, loeschen
    if output_path.exists():
        print(f"\n  Existierende DB wird ueberschrieben: {output_path}")
        output_path.unlink()

    datasets = config["datasets"]
    print(f"\n  {len(datasets)} Dataset(s) konfiguriert")

    # DuckDB Setup
    print(f"\n  DuckDB Setup: {output_path}")
    conn = setup_duckdb(output_path)

    feature_id_offset = 0
    table_created = False

    for i, ds in enumerate(datasets, 1):
        ds_name = ds["name"]
        ds_role = ds.get("role", "target")
        print(f"\n{'='*60}")
        print(f"  Dataset {i}/{len(datasets)}: {ds_name} (role={ds_role})")
        print(f"{'='*60}")

        # 1. Laden
        gdf = load_dataset_geodata(ds)

        # 2. H3-Konvertierung
        gdf = convert_to_h3(gdf, config)

        # 3. Statistiken
        print_dataset_statistics(gdf, ds_name)

        # 4. Tabelle erstellen (beim ersten Dataset) oder Spalten ergaenzen
        if not table_created:
            create_features_table(conn, gdf)
            table_created = True
        else:
            ensure_columns_exist(conn, gdf)

        # 5. Import
        feature_id_offset = import_dataset_to_duckdb(
            conn, gdf, ds_name, feature_id_offset
        )

    # H3 Lookup-Tabelle erstellen (einmal, ueber alle Datasets)
    create_h3_lookup_table(conn)

    # Finale Statistiken
    print(f"\n  Finale DuckDB-Statistiken:")
    result = conn.execute("SELECT COUNT(*) FROM features").fetchone()
    print(f"    Features gesamt:  {result[0]:,}")

    result = conn.execute("SELECT SUM(h3_cell_count) FROM features").fetchone()
    print(f"    H3 Cells gesamt:  {result[0]:,}")

    result = conn.execute("""
        SELECT dataset, COUNT(*) as cnt
        FROM features
        GROUP BY dataset
        ORDER BY dataset
    """).fetchall()
    print(f"    Pro Dataset:")
    for ds, cnt in result:
        print(f"      {ds}: {cnt:,} Features")

    # DB Size
    conn.close()
    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"\n    DB Groesse: {size_mb:.1f} MB")

    print("\n" + "=" * 60)
    print("  Fertig!")
    print("=" * 60)
    print(f"\n  Output: {output_path}")


def main():
    print("\n" + "=" * 60)
    print("  H3 Unified Converter → DuckDB")
    print("=" * 60)

    config = load_config(CONFIG_PATH)

    if config:
        config = normalize_config(config)
        display_config(config)
        choice = input("\nConfig-Defaults verwenden? [y/n]: ").strip().lower()
        if choice != "y":
            config = collect_params_interactive()
            config = normalize_config(config)
    else:
        print(f"\n  Keine config.yaml gefunden ({CONFIG_PATH})")
        print("  Parameter werden interaktiv eingegeben.")
        config = collect_params_interactive()
        config = normalize_config(config)

    validate_config(config)

    start_total = time.time()
    run_unified_conversion(config)

    print(f"\n  Gesamtzeit: {time.time() - start_total:.1f}s")


if __name__ == "__main__":
    main()
