"""
GPKG to DuckDB Importer
-----------------------
Liest ein GPKG (Output von convert_h3_multi.py) und importiert es in DuckDB.

Erstellt zwei Tabellen:
  - features: Alle Spalten aus dem GPKG (Geometrie als WKB)
  - h3_index: Eine Zeile pro H3-Cell mit vorberechneten Parent-Spalten

Die h3_index Tabelle ermoeglicht schnelle Intersection-Queries ueber
verschiedene Resolution-Levels hinweg.

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

# Parent-Spalten von min_resolution bis 14 (max 15 - 1)
MIN_PARENT_RES = 5
MAX_PARENT_RES = 14


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
    """Erstellt die features und h3_index Tabellen."""

    # Features Tabelle: Dynamisch basierend auf GPKG-Spalten
    # Alle Spalten ausser geometry und h3_cells (die werden speziell behandelt)
    columns = []
    for col in sample_gdf.columns:
        if col == "geometry":
            columns.append("geometry BLOB")  # WKB
        elif col == "h3_cells":
            continue  # Wird in h3_index Tabelle aufgeloest
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

    # H3 Index Tabelle
    parent_cols = ",\n    ".join([
        f"parent_{r} UBIGINT" for r in range(MIN_PARENT_RES, MAX_PARENT_RES + 1)
    ])

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS h3_index (
            feature_id INTEGER,
            h3_cell UBIGINT,
            resolution TINYINT,
            {parent_cols},
            PRIMARY KEY (feature_id, h3_cell)
        );
    """)


def create_indexes(conn: duckdb.DuckDBPyConnection) -> None:
    """Erstellt Indexes fuer schnelle Joins."""
    print("   Erstelle Indexes...")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_h3_cell ON h3_index(h3_cell);")

    for r in range(MIN_PARENT_RES, MAX_PARENT_RES + 1):
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_parent_{r} ON h3_index(parent_{r});")

    print(f"   {MAX_PARENT_RES - MIN_PARENT_RES + 2} Indexes erstellt.")


# ---------------------------------------------------------------------------
# Import
# ---------------------------------------------------------------------------


def import_features(conn: duckdb.DuckDBPyConnection, gdf: gpd.GeoDataFrame) -> None:
    """Importiert alle Features in die features Tabelle."""
    print(f"   Importiere {len(gdf)} Features...")

    # Spalten vorbereiten (ohne h3_cells)
    cols_to_insert = [c for c in gdf.columns if c != "h3_cells"]

    for idx, row in gdf.iterrows():
        values = [idx]  # feature_id
        placeholders = ["?"]

        for col in cols_to_insert:
            if col == "geometry":
                # Geometrie als WKB
                values.append(row.geometry.wkb)
            else:
                values.append(row[col])
            placeholders.append("?")

        cols_sql = ", ".join(["feature_id"] + [f'"{c}"' for c in cols_to_insert])
        placeholders_sql = ", ".join(placeholders)

        conn.execute(
            f"INSERT INTO features ({cols_sql}) VALUES ({placeholders_sql})",
            values
        )


def import_h3_index(conn: duckdb.DuckDBPyConnection, gdf: gpd.GeoDataFrame) -> None:
    """Importiert H3 Cells in die h3_index Tabelle mit Parent-Spalten."""
    print(f"   Importiere H3 Cells...")

    total_cells = 0
    batch = []
    batch_size = 10000

    parent_cols = [f"parent_{r}" for r in range(MIN_PARENT_RES, MAX_PARENT_RES + 1)]
    cols_sql = f"feature_id, h3_cell, resolution, {', '.join(parent_cols)}"
    placeholders = ", ".join(["?"] * (3 + len(parent_cols)))

    for idx, row in gdf.iterrows():
        h3_cells_str = row["h3_cells"]
        resolution = row["h3_resolution"]

        # h3_cells ist semikolon-getrennt
        if not h3_cells_str:
            continue

        cells = h3_cells_str.split(";")

        for cell_str in cells:
            if not cell_str:
                continue

            # H3 Cell als uint64
            cell_int = h3.str_to_int(cell_str)

            # Parent-Spalten berechnen
            parents = []
            for r in range(MIN_PARENT_RES, MAX_PARENT_RES + 1):
                if r < resolution:
                    # Parent berechnen
                    parent = h3.cell_to_parent(cell_str, r)
                    parents.append(h3.str_to_int(parent))
                else:
                    # Resolution ist gleich oder groeber -> kein Parent
                    parents.append(None)

            batch.append((idx, cell_int, resolution, *parents))
            total_cells += 1

            if len(batch) >= batch_size:
                conn.executemany(
                    f"INSERT INTO h3_index ({cols_sql}) VALUES ({placeholders})",
                    batch
                )
                batch = []
                print(f"      {total_cells:,} Cells importiert...", end="\r")

    # Rest importieren
    if batch:
        conn.executemany(
            f"INSERT INTO h3_index ({cols_sql}) VALUES ({placeholders})",
            batch
        )

    print(f"      {total_cells:,} Cells importiert.    ")


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
    print(f"\n3. Import Features...")
    start = time.time()
    import_features(conn, gdf)
    print(f"   Fertig in {time.time() - start:.1f}s")

    print(f"\n4. Import H3 Index...")
    start = time.time()
    import_h3_index(conn, gdf)
    print(f"   Fertig in {time.time() - start:.1f}s")

    # Indexes
    print(f"\n5. Indexes erstellen...")
    start = time.time()
    create_indexes(conn)
    print(f"   Fertig in {time.time() - start:.1f}s")

    # Stats
    print(f"\n6. Statistiken:")
    result = conn.execute("SELECT COUNT(*) FROM features").fetchone()
    print(f"   Features:  {result[0]:,}")

    result = conn.execute("SELECT COUNT(*) FROM h3_index").fetchone()
    print(f"   H3 Cells:  {result[0]:,}")

    result = conn.execute("""
        SELECT resolution, COUNT(*) as cnt
        FROM h3_index
        GROUP BY resolution
        ORDER BY resolution
    """).fetchall()
    print(f"   Pro Resolution:")
    for res, cnt in result:
        print(f"     Res {res:2d}: {cnt:>12,} Cells")

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
