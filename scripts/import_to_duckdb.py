"""
GPKG to DuckDB Importer
-----------------------
Liest ein GPKG (Output von convert_h3_multi.py) und importiert es in DuckDB.

Erstellt eine einzelne Tabelle:
  - features: Alle Spalten aus dem GPKG (Geometrie als WKB, H3 Cells als UBIGINT[])

Die H3 Cells werden als Array (UBIGINT[]) gespeichert. Parent-Cells fuer
verschiedene Resolution-Levels werden zur Query-Zeit berechnet via
DuckDB H3 Extension (h3_cell_to_parent).

Der Import nutzt DuckDB's Bulk-Operationen: Das GeoDataFrame wird als
temporaere View registriert und per INSERT INTO ... SELECT in einem
einzigen Schritt transformiert und eingefuegt. Die H3-String-Konvertierung
(str_split + h3_string_to_h3) laeuft vektorisiert in DuckDB statt in Python.

Verwendung:
    python scripts/import_to_duckdb.py
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

# ---------------------------------------------------------------------------
# Konstanten
# ---------------------------------------------------------------------------

CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def load_config(path: Path) -> dict | None:
    if not path.exists():
        return None
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# DuckDB Setup
# ---------------------------------------------------------------------------


def setup_duckdb(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Erstellt DuckDB Connection und laedt H3 Extension."""
    conn = duckdb.connect(str(db_path))

    # H3 Extension laden
    conn.execute("INSTALL h3 FROM community;")
    conn.execute("LOAD h3;")

    return conn


def create_tables(conn: duckdb.DuckDBPyConnection, sample_gdf: gpd.GeoDataFrame) -> None:
    """Erstellt die features Tabelle mit H3 Cells als Array."""

    columns = []
    for col in sample_gdf.columns:
        if col == "geometry":
            columns.append("geometry BLOB")  # WKB
        elif col == "h3_cells":
            columns.append("h3_cells UBIGINT[]")  # Array statt separate Tabelle
        elif col == "h3_resolution":
            columns.append("h3_resolution TINYINT")
        elif col == "h3_cell_count":
            columns.append("h3_cell_count INTEGER")
        else:
            # Typ-Mapping
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
# Import
# ---------------------------------------------------------------------------


def prepare_dataframe(gdf: gpd.GeoDataFrame) -> tuple[pd.DataFrame, list[str], list[str]]:
    """
    Bereitet ein GeoDataFrame fuer DuckDB Bulk-Import vor.

    Konvertiert Geometrien zu WKB-Bytes und behaelt h3_cells als Rohstring.
    Die eigentliche H3-String-Konvertierung passiert spaeter in DuckDB (vektorisiert).

    Returns:
        (DataFrame, insert_columns, select_expressions)
    """
    df = pd.DataFrame({"feature_id": range(len(gdf))})

    insert_cols = ["feature_id"]
    select_exprs = ["feature_id"]

    for col in gdf.columns:
        if col == "geometry":
            # Geometrie → WKB Bytes (einzige Python-Iteration, aber in C via Shapely)
            df["geometry"] = gdf.geometry.apply(lambda g: g.wkb if g else None)
            insert_cols.append("geometry")
            select_exprs.append("geometry")

        elif col == "h3_cells":
            # Als Rohstring behalten → DuckDB konvertiert vektorisiert
            df["h3_cells_raw"] = gdf["h3_cells"].fillna("")
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


def import_features(conn: duckdb.DuckDBPyConnection, gdf: gpd.GeoDataFrame) -> None:
    """Bulk-importiert alle Features inkl. H3 Cells als Array.

    Statt Zeile-fuer-Zeile INSERT wird das DataFrame bei DuckDB registriert
    und per INSERT INTO ... SELECT in einem Schritt transformiert:
    - str_split() spaltet die Semikolon-getrennten H3-Strings
    - h3_string_to_h3() konvertiert jeden String zu UBIGINT
    - Alles vektorisiert in DuckDB, kein Python-Loop noetig
    """
    print(f"   Bereite {len(gdf):,} Features vor...")
    start = time.time()

    df, insert_cols, select_exprs = prepare_dataframe(gdf)

    print(f"   DataFrame vorbereitet in {time.time() - start:.1f}s")
    print(f"   Starte DuckDB Bulk-Import...")
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
    total_cells = conn.execute("SELECT COALESCE(SUM(h3_cell_count), 0) FROM features").fetchone()[0]
    print(f"   {len(gdf):,} Features mit {total_cells:,} H3 Cells importiert in {elapsed:.1f}s")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    print("\n" + "=" * 60)
    print("  GPKG → DuckDB Importer")
    print("=" * 60)

    # Config laden
    config = load_config(CONFIG_PATH)
    if not config:
        print(f"Fehler: config.yaml nicht gefunden ({CONFIG_PATH})")
        sys.exit(1)

    # Pfade
    gpkg_path = Path(config["output_file"])
    db_path = gpkg_path.with_suffix(".duckdb")

    print(f"\n  Input:  {gpkg_path}")
    print(f"  Output: {db_path}")

    if not gpkg_path.exists():
        print(f"\nFehler: GPKG nicht gefunden: {gpkg_path}")
        print("Bitte zuerst convert_h3_multi.py ausfuehren.")
        sys.exit(1)

    # Falls DB existiert, loeschen
    if db_path.exists():
        print(f"\n  Existierende DB wird ueberschrieben...")
        db_path.unlink()

    # GPKG laden
    print(f"\n1. Lade GPKG...")
    start = time.time()
    gdf = gpd.read_file(gpkg_path)
    print(f"   {len(gdf)} Zeilen geladen in {time.time() - start:.1f}s")
    print(f"   Spalten: {list(gdf.columns)}")

    # DuckDB Setup
    print(f"\n2. DuckDB Setup...")
    conn = setup_duckdb(db_path)
    create_tables(conn, gdf)

    # Import
    print(f"\n3. Import Features + H3 Cells...")
    start_total = time.time()
    import_features(conn, gdf)
    print(f"   Gesamt: {time.time() - start_total:.1f}s")

    # Stats
    print(f"\n4. Statistiken:")
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
    size_mb = db_path.stat().st_size / (1024 * 1024)
    print(f"\n   DB Groesse: {size_mb:.1f} MB")

    print("\n" + "=" * 60)
    print("  Fertig!")
    print("=" * 60)
    print(f"\n  Output: {db_path}")


if __name__ == "__main__":
    main()
