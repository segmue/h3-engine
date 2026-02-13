"""
Spatial Association Analysis: NPMI + Gewichtungsmatrizen (B1, B2).

Berechnet fuer jede Kombination von OBJEKTARTs im Datensatz:
  - NPMI  (symmetrisch)  : Normalized Pointwise Mutual Information
  - B1    (asymmetrisch) : Kontextgewichtung   = NPMI × p_b / (p_a + p_b)
  - B2    (asymmetrisch) : Konfidenzgewichtung  = NPMI × p_ab / p_a

Alle Berechnungen sind flaechenbasiert (h3_cell_area)

Verwendung:
    python -m spatial_association_analysis.compute_association

    # oder als Modul:
    from spatial_association_analysis.compute_association import compute_all
    npmi, b1, b2 = compute_all("data/swissNAMES3D_combined_h3.duckdb")
"""

import sys
import time
from itertools import combinations
from pathlib import Path

import numpy as np
import pandas as pd

# Engine importieren (Repo-Root muss im PYTHONPATH sein)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from engine import H3Engine


# ---------------------------------------------------------------------------
# NPMI Berechnung
# ---------------------------------------------------------------------------

def calculate_npmi(p_a: float, p_b: float, p_ab: float) -> float:
    """Normalized Pointwise Mutual Information.

    NPMI = log2(p_ab / (p_a * p_b)) / (-log2(p_ab))

    Wertebereich: [-1, 1]
      -1 = nie zusammen
       0 = unabhaengig (Zufall)
      +1 = perfekte Co-Occurrence
    """
    eps = 1e-15
    if p_ab <= eps:
        return -1.0
    pmi = np.log2(p_ab / (p_a * p_b))
    norm_factor = -np.log2(p_ab)
    if norm_factor == 0:
        return 0.0
    result = pmi / norm_factor
    return float(max(min(result, 1.0), -1.0))


# ---------------------------------------------------------------------------
# Hauptberechnung
# ---------------------------------------------------------------------------

def compute_all(
    db_path: str,
    total_area_resolution: int = 10,
    output_dir: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Berechnet NPMI, B1 und B2 Matrizen fuer alle OBJEKTART-Paare.

    Args:
        db_path: Pfad zur DuckDB Datei
        total_area_resolution: Resolution fuer Gesamtflaechen-Vereinfachung
        output_dir: Optionaler Pfad zum Speichern der Matrizen als Parquet

    Returns:
        Tuple von (npmi_df, b1_df, b2_df) als pandas DataFrames
    """
    engine = H3Engine(db_path)

    # ------------------------------------------------------------------
    # 1. Alle OBJEKTARTs ermitteln
    # ------------------------------------------------------------------
    objektarten = engine.conn.execute("""
        SELECT DISTINCT OBJEKTART
        FROM features
        ORDER BY OBJEKTART
    """).fetchall()
    objektarten = [row[0] for row in objektarten]
    n = len(objektarten)
    print(f"Gefunden: {n} OBJEKTARTs → {n*(n-1)//2} Paare")

    # ------------------------------------------------------------------
    # 2. Gesamtflaeche (vereinfacht auf target Resolution)
    # ------------------------------------------------------------------
    print(f"\nBerechne Gesamtflaeche (Resolution {total_area_resolution})...")
    t0 = time.time()
    total_area = engine.total_area(resolution=total_area_resolution)
    print(f"  Gesamtflaeche: {total_area:,.2f} km²  ({time.time()-t0:.1f}s)")

    # ------------------------------------------------------------------
    # 3. Pro OBJEKTART: Flaeche (native Resolution, h3_cell_area)
    # ------------------------------------------------------------------
    print(f"\nBerechne Flaechen pro OBJEKTART ({n} Stueck)...")
    t0 = time.time()
    areas: dict[str, float] = {}
    for obj in objektarten:
        areas[obj] = engine.area(engine.union(f"OBJEKTART = '{obj}'"))
    elapsed = time.time() - t0
    print(f"  Fertig ({elapsed:.1f}s)")

    for obj in sorted(areas, key=areas.get, reverse=True)[:5]:
        print(f"    {obj}: {areas[obj]:,.2f} km²")
    print(f"    ... (Top 5 von {n})")

    # ------------------------------------------------------------------
    # 4. Paarweise Intersection-Flaechen
    # ------------------------------------------------------------------
    pairs = list(combinations(objektarten, 2))
    n_pairs = len(pairs)
    print(f"\nBerechne Intersection-Flaechen ({n_pairs} Paare)...")

    intersection_areas: dict[tuple[str, str], float] = {}
    t0 = time.time()
    for i, (obj_a, obj_b) in enumerate(pairs):
        intersection_areas[(obj_a, obj_b)] = engine.area(
            engine.intersection(f"OBJEKTART = '{obj_a}'", f"OBJEKTART = '{obj_b}'")
        )

        # Fortschritt alle 100 Paare
        if (i + 1) % 100 == 0 or (i + 1) == n_pairs:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            remaining = (n_pairs - i - 1) / rate if rate > 0 else 0
            print(f"  {i+1}/{n_pairs} ({elapsed:.0f}s, ~{remaining:.0f}s verbleibend)")

    # ------------------------------------------------------------------
    # 5. Matrizen aufbauen
    # ------------------------------------------------------------------
    print("\nBerechne NPMI, B1, B2 Matrizen...")
    idx = {name: i for i, name in enumerate(objektarten)}

    npmi_matrix = np.full((n, n), np.nan)
    b1_matrix = np.full((n, n), np.nan)
    b2_matrix = np.full((n, n), np.nan)

    # Diagonale: Selbst-Assoziation
    for obj in objektarten:
        i = idx[obj]
        p_a = areas[obj] / total_area
        npmi_matrix[i, i] = calculate_npmi(p_a, p_a, p_a)  # = 1.0
        b1_matrix[i, i] = npmi_matrix[i, i] * 0.5           # p_a / (p_a + p_a)
        b2_matrix[i, i] = npmi_matrix[i, i] * 1.0           # p_aa / p_a = 1.0

    # Alle Paare
    for (obj_a, obj_b), area_ab in intersection_areas.items():
        i = idx[obj_a]
        j = idx[obj_b]

        p_a = areas[obj_a] / total_area
        p_b = areas[obj_b] / total_area
        p_ab = area_ab / total_area

        npmi_val = calculate_npmi(p_a, p_b, p_ab)

        # NPMI: symmetrisch
        npmi_matrix[i, j] = npmi_val
        npmi_matrix[j, i] = npmi_val

        # B1: Kontextgewichtung (Randwahrscheinlichkeit), asymmetrisch
        denom = p_a + p_b
        if denom > 0:
            b1_matrix[i, j] = npmi_val * (p_b / denom)       # a → b
            b1_matrix[j, i] = npmi_val * (p_a / denom)       # b → a

        # B2: Konfidenzgewichtung (bedingte Wahrscheinlichkeit), asymmetrisch
        b2_matrix[i, j] = npmi_val * (p_ab / p_a) if p_a > 0 else 0.0   # a → b
        b2_matrix[j, i] = npmi_val * (p_ab / p_b) if p_b > 0 else 0.0   # b → a

    # ------------------------------------------------------------------
    # 6. Als DataFrames
    # ------------------------------------------------------------------
    npmi_df = pd.DataFrame(npmi_matrix, index=objektarten, columns=objektarten)
    b1_df = pd.DataFrame(b1_matrix, index=objektarten, columns=objektarten)
    b2_df = pd.DataFrame(b2_matrix, index=objektarten, columns=objektarten)

    # ------------------------------------------------------------------
    # 7. Optional speichern
    # ------------------------------------------------------------------
    if output_dir:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        npmi_df.to_csv(out / "npmi_matrix.csv")
        b1_df.to_csv(out / "b1_matrix.csv")
        b2_df.to_csv(out / "b2_matrix.csv")
        print(f"\nMatrizen gespeichert in {out}/")

    engine.close()

    print(f"\nFertig: {n}×{n} Matrizen berechnet.")
    return npmi_df, b1_df, b2_df


# ---------------------------------------------------------------------------
# CLI Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else "data/swissNAMES3D_combined_h3.duckdb"
    out = sys.argv[2] if len(sys.argv) > 2 else "data/association_results"
    compute_all(db, output_dir=out)
