from engine import H3Engine
import time

db = H3Engine("data/swissNAMES3D_combined_h3.duckdb")

# ---------------------------------------------------------------------------
# DuckDB Relational API (db.features → DuckDBPyRelation)
# ---------------------------------------------------------------------------

# Filter als wiederverwendbare Relations
flurnamen = db.features.filter("OBJEKTART = 'Flurname swisstopo'")
orte = db.features.filter("OBJEKTART = 'Ort'")
fliessgewaesser = db.features.filter("OBJEKTART = 'Fliessgewaesser'")

# Counts via Relational API
print(flurnamen.aggregate("count(*) as n, sum(h3_cell_count) as cells").df())
print(orte.aggregate("count(*) as n, sum(h3_cell_count) as cells").df())
print(fliessgewaesser.aggregate("count(*) as n, sum(h3_cell_count) as cells").df())

# Resolutions pro Kategorie
print(db.features
    .filter("OBJEKTART IN ('Flurname swisstopo', 'Ort')")
    .aggregate("OBJEKTART, min(h3_resolution) as min_res, max(h3_resolution) as max_res", "OBJEKTART")
    .df())

print(fliessgewaesser
      .aggregate("OBJEKTART, min(h3_resolution) as min_res, max(h3_resolution) as max_res", "OBJEKTART")
      .df())

# ---------------------------------------------------------------------------
# Spatial Predicates (akzeptieren Relations oder Strings)
# ---------------------------------------------------------------------------

start = time.time()
intersection_result = db.intersection(flurnamen, orte)
cell_count = intersection_result.aggregate("count(*) as n").fetchone()[0]
elapsed = time.time() - start
print(f"Found {cell_count} intersecting cells in {elapsed:.2f} seconds.")

start = time.time()
intersection_result = db.intersection(orte, fliessgewaesser)
cell_count = intersection_result.aggregate("count(*) as n").fetchone()[0]
elapsed = time.time() - start
print(f"Found {cell_count} intersecting cells in {elapsed:.2f} seconds.")

print(db.conn.execute('''
      SELECT ST_AsText(geometry)
      FROM features
      WHERE OBJEKTART = 'See'
      LIMIT 1
  ''').fetchone())