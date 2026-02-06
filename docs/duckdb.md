# H3 DuckDB Engine

DuckDB-basierte Speicherung und Abfrage von H3-indexierten Geodaten.

## Workflow

```
GPKG (GeoPackage)          DuckDB
┌─────────────────┐        ┌──────────────────────────────┐
│ Features +      │  ───►  │ features (Metadaten +        │
│ h3_cells        │        │          H3 Cells als Array) │
└─────────────────┘        └──────────────────────────────┘
     convert_h3_multi.py        import_to_duckdb.py
```

## Import

```bash
# 1. Geodaten → GPKG mit H3 Cells
poetry run python scripts/convert_h3_multi.py

# 2. GPKG → DuckDB
poetry run python scripts/import_to_duckdb.py
```

Output: `data/swissNAMES3D_combined_h3.duckdb`

## Schema

### Tabelle: `features`

Alle Spalten aus dem GPKG. H3 Cells werden als `UBIGINT[]` Array gespeichert.

| Spalte | Typ | Beschreibung |
|--------|-----|--------------|
| feature_id | INTEGER | Primary Key |
| geometry | BLOB | Original-Geometrie als WKB |
| h3_resolution | TINYINT | Verwendete H3 Resolution |
| h3_cell_count | INTEGER | Anzahl Cells |
| h3_cells | UBIGINT[] | Array aller H3 Cell IDs |
| _source_file | VARCHAR | Quelldatei |
| ... | ... | Alle weiteren Spalten aus GPKG |

**Beispiel:** Feature mit 3 Cells auf Resolution 8

```
feature_id:    42
h3_resolution: 8
h3_cell_count: 3
h3_cells:      [621496283906039808, 621496283906039809, 621496283906039810]
```

Parent-Cells werden **nicht** vorberechnet gespeichert, sondern bei Bedarf
zur Query-Zeit via DuckDB H3 Extension (`h3_cell_to_parent`) berechnet.

## Queries

Die H3 Extension stellt `h3_cell_to_parent(cell, resolution)` bereit.
In Kombination mit `UNNEST(h3_cells)` werden Arrays bei Bedarf expandiert:

```sql
-- Alle Cells eines Features, normalisiert auf Resolution 5
SELECT DISTINCT h3_cell_to_parent(UNNEST(h3_cells), 5) as parent
FROM features
WHERE kategorie = 'Wald'
```

## Predicates

```python
from engine import H3Engine

db = H3Engine("data/swissNAMES3D_combined_h3.duckdb")
```

### intersects(a, b) → bool

Prüft ob sich A und B überschneiden.

```python
db.intersects("kategorie = 'Wald'", "kategorie = 'See'")
db.intersects("feature_id = 123", "kanton = 'ZH'")
```

### within(a, b) → bool

Prüft ob A vollständig in B liegt.

```python
db.within("feature_id = 123", "kategorie = 'Kanton'")
```

### contains(a, b) → bool

Prüft ob A das gesamte B enthält. Inverse von `within`.

```python
db.contains("kategorie = 'Kanton'", "feature_id = 123")
```

### intersection(a, b) → (cells, resolution)

Gibt die überschneidenden Cells zurück (auf der feineren Resolution).

```python
cells, res = db.intersection("kategorie = 'Wald'", "name = 'Zürichsee'")
print(f"{len(cells)} Cells auf Resolution {res}")
# → ['8a1234...', '8a1235...', ...]
```

### Utilities

```python
db.count_cells("kategorie = 'Wald'")      # Anzahl H3 Cells
db.count_features("kategorie = 'Wald'")   # Anzahl Features
db.get_resolutions("kategorie = 'Wald'")  # [6, 7, 8, ...]
```

## Wie die Resolution-Normalisierung funktioniert

Bei unterschiedlichen Resolutions (z.B. A=Res 6, B=Res 12):

1. **Normalisierung** via `h3_cell_to_parent(cell, 6)` auf die gröbere Resolution
2. **Ergebnis** auf der feineren Resolution (Res 12)

```
A (Res 6):  [████████████]     ← 1 grosse Cell
B (Res 12): [▪▪▪▪▪▪▪▪▪▪▪▪▪▪]   ← viele kleine Cells

intersection(A, B) → Die B-Cells deren h3_cell_to_parent(..., 6) in A liegt
```

So bleibt die Genauigkeit erhalten, ohne vorberechnete Parent-Spalten.
