"""
GPKG to DuckDB Importer
-----------------------
Liest ein GPKG (Output von convert_h3_multi.py) und importiert es in DuckDB.

Erstellt eine einzelne Tabelle:
  - features: Alle Spalten aus dem GPKG (Geometrie als WKB, H3 Cells als UBIGINT[])

Die H3 Cells werden als Array (UBIGINT[]) gespeichert. Parent-Cells fuer
verschiedene Resolution-Levels werden zur Query-Zeit berechnet via
DuckDB H3 Extension (h3_cell_to_parent).

Verwendung:
    python scripts/import_to_duckdb.py
"""

import sys
import time
from pathlib import Path

import duckdb
import geopandas as gpd
import h3
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


def import_features(conn: duckdb.DuckDBPyConnection, gdf: gpd.GeoDataFrame) -> None:
    """Importiert alle Features inkl. H3 Cells als Array."""
    print(f"   Importiere {len(gdf)} Features...")

    total_cells = 0
    cols_to_insert = list(gdf.columns)

    for idx, row in gdf.iterrows():
        values = [idx]  # feature_id
        placeholders = ["?"]

        for col in cols_to_insert:
            if col == "geometry":
                # Geometrie als WKB
                values.append(row.geometry.wkb)
            elif col == "h3_cells":
                # Semikolon-getrennte Cells → UBIGINT Array
                cells_str = row["h3_cells"]
                if cells_str:
                    cells = [h3.str_to_int(c) for c in cells_str.split(";") if c]
                else:
                    cells = []
                values.append(cells)
                total_cells += len(cells)
            else:
                values.append(row[col])
            placeholders.append("?")

        cols_sql = ", ".join(["feature_id"] + [f'"{c}"' for c in cols_to_insert])
        placeholders_sql = ", ".join(placeholders)

        conn.execute(
            f"INSERT INTO features ({cols_sql}) VALUES ({placeholders_sql})",
            values
        )

        if (idx + 1) % 1000 == 0:
            print(f"      {idx + 1:,} Features importiert...", end="\r")

    print(f"      {len(gdf):,} Features mit {total_cells:,} H3 Cells importiert.    ")


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
    start = time.time()
    import_features(conn, gdf)
    print(f"   Fertig in {time.time() - start:.1f}s")

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
