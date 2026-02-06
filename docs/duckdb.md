# H3 DuckDB Engine

DuckDB-basierte Speicherung und Abfrage von H3-indexierten Geodaten.

## Workflow

```
GPKG (GeoPackage)          DuckDB
┌─────────────────┐        ┌─────────────────────────────┐
│ Features +      │  ───►  │ features (Metadaten)        │
│ h3_cells        │        │ h3_index (Cells + Parents)  │
└─────────────────┘        └─────────────────────────────┘
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

Alle Spalten aus dem GPKG plus Metadaten.

| Spalte | Typ | Beschreibung |
|--------|-----|--------------|
| feature_id | INTEGER | Primary Key |
| geometry | BLOB | Original-Geometrie als WKB |
| h3_resolution | TINYINT | Verwendete H3 Resolution |
| h3_cell_count | INTEGER | Anzahl Cells |
| _source_file | VARCHAR | Quelldatei |
| ... | ... | Alle weiteren Spalten aus GPKG |

### Tabelle: `h3_index`

Eine Zeile pro H3 Cell mit vorberechneten Parent-Cells.

| Spalte | Typ | Beschreibung |
|--------|-----|--------------|
| feature_id | INTEGER | FK zu features |
| h3_cell | UBIGINT | H3 Cell ID |
| resolution | TINYINT | Resolution der Cell |
| parent_5 | UBIGINT | Parent auf Res 5 (NULL wenn res ≤ 5) |
| parent_6 | UBIGINT | Parent auf Res 6 (NULL wenn res ≤ 6) |
| ... | ... | ... |
| parent_14 | UBIGINT | Parent auf Res 14 (NULL wenn res ≤ 14) |

**Beispiel:** Cell auf Resolution 8

```
h3_cell:   621496283906039808  (Res 8)
parent_5:  599504582565953536  ✓ gefüllt
parent_6:  604007982352769024  ✓ gefüllt
parent_7:  608511382139584512  ✓ gefüllt
parent_8:  NULL                ✗ nicht nötig (ist die Cell selbst)
parent_9:  NULL                ✗ nicht nötig (wäre Child)
...
```

## Indexes

```sql
idx_h3_cell   ON h3_index(h3_cell)
idx_parent_5  ON h3_index(parent_5)
idx_parent_6  ON h3_index(parent_6)
...
idx_parent_14 ON h3_index(parent_14)
```

Ermöglicht schnelle Joins über verschiedene Resolution-Levels.

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

1. **Join** über `parent_6` Spalte von B
2. **Ergebnis** auf der feineren Resolution (Res 12)

```
A (Res 6):  [████████████]     ← 1 grosse Cell
B (Res 12): [▪▪▪▪▪▪▪▪▪▪▪▪▪▪]   ← viele kleine Cells

intersection(A, B) → Die B-Cells deren parent_6 in A liegt
```

So bleibt die Genauigkeit erhalten.
