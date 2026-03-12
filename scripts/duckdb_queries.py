from engine import H3Engine
import time

db = H3Engine("data/swissNAMES3D_combined_h3.duckdb")

# ---------------------------------------------------------------------------
# DuckDB Relational API (db.features → DuckDBPyRelation)
# ---------------------------------------------------------------------------

# Filter als wiederverwendbare Relations
sakgeb = db.features.filter("OBJEKTART = 'Sakrales Gebaeude'")
orte = db.features.filter("OBJEKTART = 'Ort'")

# Counts via Relational API
print(sakgeb.aggregate("count(*) as n, sum(h3_cell_count) as cells").df())
print(orte.aggregate("count(*) as n, sum(h3_cell_count) as cells").df())

# Resolutions pro Kategorie
print(db.features
    .filter("OBJEKTART IN ('Sakrales Gebaeude', 'Ort')")
    .aggregate("OBJEKTART, min(h3_resolution) as min_res, max(h3_resolution) as max_res", "OBJEKTART")
    .df())

# ---------------------------------------------------------------------------
# Spatial Predicates (erfordern union() vor intersection())
# ---------------------------------------------------------------------------

# Erst union() auf beide Sets, dann intersection()
union_sakgeb = db.union(sakgeb)
union_orte = db.union(orte)

start = time.time()
intersection_result = db.intersection(union_sakgeb, union_orte)
cell_count = intersection_result.aggregate("count(*) as n").fetchone()[0]
elapsed = time.time() - start
print(f"Found {cell_count} intersecting cells in {elapsed:.2f} seconds.")

# Area berechnen
db.area(intersection_result)

# Fliessgewaesser Beispiel (auskommentiert da fliessgewaesser nicht definiert)
# fliessgewaesser = db.features.filter("OBJEKTART = 'Fliessgewaesser'")
# union_fliessgewaesser = db.union(fliessgewaesser)
# start = time.time()
# intersection_result = db.intersection(union_orte, union_fliessgewaesser)
# cell_count = intersection_result.aggregate("count(*) as n").fetchone()[0]
# elapsed = time.time() - start
# print(f"Found {cell_count} intersecting cells in {elapsed:.2f} seconds.")

print(db.conn.execute('''
      SELECT ST_AsText(geometry)
      FROM features
      WHERE OBJEKTART = 'See'
      LIMIT 1
  ''').fetchone())