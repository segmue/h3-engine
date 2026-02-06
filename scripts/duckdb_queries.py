from engine import H3Engine
import time

db = H3Engine("data/swissNAMES3D_combined_h3.duckdb")

# ---------------------------------------------------------------------------
# DuckDB Relational API (db.features → DuckDBPyRelation)
# ---------------------------------------------------------------------------

# Filter als wiederverwendbare Relations
flurnamen = db.features.filter("OBJEKTART = 'Flurname swisstopo'")
orte = db.features.filter("OBJEKTART = 'Ort'")

# Counts via Relational API
print(flurnamen.aggregate("count(*) as n, sum(h3_cell_count) as cells").df())
print(orte.aggregate("count(*) as n, sum(h3_cell_count) as cells").df())

# Resolutions pro Kategorie
print(db.features
    .filter("OBJEKTART IN ('Flurname swisstopo', 'Ort')")
    .aggregate("OBJEKTART, min(h3_resolution) as min_res, max(h3_resolution) as max_res", "OBJEKTART")
    .df())

# ---------------------------------------------------------------------------
# Spatial Predicates (akzeptieren Relations oder Strings)
# ---------------------------------------------------------------------------

start = time.time()
cells, res = db.intersection(flurnamen, orte)
elapsed = time.time() - start
print(f"Found {len(cells)} intersecting cells at resolution {res} in {elapsed:.2f} seconds.")

# ---------------------------------------------------------------------------
# Alte API funktioniert weiterhin
# ---------------------------------------------------------------------------

f = db.count_features("OBJEKTART = 'Flurname swisstopo'")
c = db.count_cells("OBJEKTART = 'Flurname swisstopo'")
r = db.get_resolutions("OBJEKTART = 'Flurname swisstopo'")
print(f"For category 'Flurname swisstopo' there are {f} features, {c} total cells at resolutions {r}.")
