from engine.h3_engine import *
import time

db = H3Engine("data/swissNAMES3D_combined_h3.duckdb")

# Number of Flurname Features and Ort Features, Total Number of Cells in Index
f = db.count_features("OBJEKTART = 'Flurname swisstopo'")
c = db.count_cells("OBJEKTART = 'Flurname swisstopo'")
r = db.get_resolutions("OBJEKTART = 'Flurname swisstopo'")


f2 = db.count_features("OBJEKTART = 'Ort'")
c2 = db.count_cells("OBJEKTART = 'Ort'")
r2 = db.get_resolutions("OBJEKTART = 'Ort'")

print(f"For category 'Flurname swisstopo' there are {f} features, {c} total cells at resolutions {r}.")
print(f"For category 'Ort' there are {f2} features, {c2} total cells at resolutions {r2}.")

start = time.time()
cells, res = db.intersection("OBJEKTART = 'Flurname swisstopo'", "OBJEKTART = 'Ort'")
end = time.time()
print(f"Found {len(cells)} intersecting cells at resolution {res} in {end - start:.2f} seconds.")