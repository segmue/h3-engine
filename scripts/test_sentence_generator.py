from engine import H3Engine
from sentence_generator import CandidateSentenceGenerator, FeatureInput
from sentence_generator.association_loader import AssociationMatrixLoader
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
loader.matrix.shape  # (103, 103)

# Assoziierte Kategorien fuer "Alpiner Gipfel"
associated = loader.get_associated_categories("Alpiner Gipfel", threshold=0.00, max_categories=10)
associated  # [('Landesgrenzstein', 0.419), ('Strasse', 0.163), ...]

# ---------------------------------------------------------------------------
# Feature suchen
# ---------------------------------------------------------------------------

results = db.conn.execute("""
    SELECT feature_id, NAME, OBJEKTART
    FROM features
    WHERE OBJEKTART = 'Alpiner Gipfel' AND NAME IS NOT NULL
    LIMIT 10
""").fetchall()
results

# Ein Feature waehlen
feature_id, name, objektart = results[7]
feature_id, name, objektart

# ---------------------------------------------------------------------------
# intersects_predicate() testen
# ---------------------------------------------------------------------------

# CellSet fuer das Feature
cells = db.union(f"feature_id = {feature_id}")
cells.resolution
cells.count()

# Predicate bauen
predicate = db.intersects_predicate(cells)
print(predicate[:300])

# Intersecting Features einer Kategorie finden
intersecting = db.features.filter(f"NAME IS NOT NULL AND {predicate}")
intersecting.limit(5).df()

# ---------------------------------------------------------------------------
# Slot-Allokation
# ---------------------------------------------------------------------------

slots = generator._allocate_slots(associated)
slots  # {'Landesgrenzstein': 3, 'Strasse': 1, ...}

# ---------------------------------------------------------------------------
# Satz generieren
# ---------------------------------------------------------------------------

feature = FeatureInput(feature_id=feature_id, name=name, objektart=objektart)
result = generator.generate(feature)

result.sentence
result.context_by_category
result.categories_used

# ---------------------------------------------------------------------------
# Batch Test
# ---------------------------------------------------------------------------

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
    print(r.sentence)
