from engine import H3Engine
from sentence_generator import CandidateSentenceGenerator, FeatureInput
import time

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

db = H3Engine("data/swissNAMES3D_combined_h3.duckdb")
generator = CandidateSentenceGenerator(db)

# ---------------------------------------------------------------------------
# B1 Matrix laden und Assoziationen anschauen
# ---------------------------------------------------------------------------

loader = generator._assoc_loader
print(f"B1 Matrix shape: {loader.matrix.shape}")

associated = loader.get_associated_categories("Alpiner Gipfel", threshold=0.00, max_categories=10)
print(f"\nAssoziierte Kategorien fuer 'Alpiner Gipfel':")
for kat, b1 in associated:
    print(f"  {kat}: {b1:.3f}")

# ---------------------------------------------------------------------------
# Feature suchen
# ---------------------------------------------------------------------------

results = db.conn.execute("""
    SELECT feature_id, NAME, OBJEKTART
    FROM features
    WHERE OBJEKTART = 'Alpiner Gipfel' AND NAME IS NOT NULL
    LIMIT 10
""").fetchall()

feature_id, name, objektart = results[7]
print(f"\nTest-Feature: {name} ({objektart}, ID={feature_id})")

# ---------------------------------------------------------------------------
# find_intersecting_features() testen
# ---------------------------------------------------------------------------

print("\n--- find_intersecting_features() Test ---")
tic = time.time()
intersecting = db.find_intersecting_features(
    feature_id=feature_id,
    objektart_list=["Strasse", "Grat", "See"],
    exclude_id=feature_id
)
df = intersecting.df()
elapsed = time.time() - tic
print(f"Found {len(df)} intersecting features in {elapsed:.3f}s")
if not df.empty:
    print(df.head(10).to_string())

# ---------------------------------------------------------------------------
# Slot-Allokation
# ---------------------------------------------------------------------------

slots = generator._allocate_slots(associated)
print(f"\nSlot-Allokation: {slots}")

# ---------------------------------------------------------------------------
# Satz generieren (Einzeln)
# ---------------------------------------------------------------------------

print("\n--- Einzelne Satzgenerierung ---")
feature = FeatureInput(feature_id=feature_id, name=name, objektart=objektart)
tic = time.time()
result = generator.generate(feature)
elapsed = time.time() - tic

print(f"Zeit: {elapsed:.3f}s")
print(f"Satz: {result.sentence}")
print(f"Kategorien: {result.categories_used}")
print(f"Kontext: {result.context_by_category}")

# ---------------------------------------------------------------------------
# Batch Test
# ---------------------------------------------------------------------------

print("\n--- Batch Test (5 Features) ---")
features_data = db.conn.execute("""
    SELECT feature_id, NAME, OBJEKTART
    FROM features
    WHERE NAME IS NOT NULL
    LIMIT 5
""").fetchall()

features = [FeatureInput(f[0], f[1], f[2]) for f in features_data]
tic = time.time()
results = generator.generate_batch(features)
toc = time.time()
print(f"Batch generation for {len(features)} features took {toc - tic:.2f} seconds")

for r in results:
    print(f"  [{r.feature_id}] {r.sentence}")

# ---------------------------------------------------------------------------
# Groesserer Batch Test
# ---------------------------------------------------------------------------

print("\n--- Batch Test (50 Features) ---")
features_data = db.conn.execute("""
    SELECT feature_id, NAME, OBJEKTART
    FROM features
    WHERE NAME IS NOT NULL
    LIMIT 50
""").fetchall()

features = [FeatureInput(f[0], f[1], f[2]) for f in features_data]
tic = time.time()
results = generator.generate_batch(features)
toc = time.time()
print(f"Batch generation for {len(features)} features took {toc - tic:.2f} seconds")
print(f"Average: {(toc - tic) / len(features) * 1000:.0f} ms/feature")
