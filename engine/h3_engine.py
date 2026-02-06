"""
H3Engine - DuckDB-basierte Spatial Predicates fuer H3 Cells.

Alle Predicates nutzen DuckDB's H3 Extension fuer Resolution-Normalisierung
via h3_cell_to_parent(). H3 Cells werden als UBIGINT[] Arrays in der
features Tabelle gespeichert und bei Bedarf via UNNEST expandiert.

Verwendung:
    from engine import H3Engine

    db = H3Engine("data.duckdb")

    # Boolean Predicates
    db.intersects("kategorie = 'Wald'", "kategorie = 'See'")
    db.within("feature_id = 123", "kategorie = 'Kanton'")
    db.contains("kategorie = 'Kanton'", "feature_id = 123")

    # Intersection mit Cells
    cells, resolution = db.intersection("kategorie = 'Wald'", "name = 'Zuerichsee'")
"""

from pathlib import Path
from typing import Union

import duckdb
import h3


class H3Engine:
    """DuckDB-basierte H3 Spatial Query Engine."""

    def __init__(self, db_path: Union[str, Path]):
        """
        Initialisiert die Engine mit einer DuckDB Datenbank.

        Args:
            db_path: Pfad zur DuckDB Datei (von import_to_duckdb.py erstellt)
        """
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Datenbank nicht gefunden: {db_path}")

        self.conn = duckdb.connect(str(self.db_path), read_only=True)
        self.conn.execute("LOAD h3;")

    def close(self):
        """Schliesst die Datenbankverbindung."""
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # -------------------------------------------------------------------------
    # Hilfsmethoden
    # -------------------------------------------------------------------------

    def _get_resolution_range(self, where: str) -> tuple[int, int]:
        """Ermittelt min/max Resolution fuer eine WHERE-Bedingung."""
        result = self.conn.execute(f"""
            SELECT MIN(h3_resolution), MAX(h3_resolution)
            FROM features
            WHERE {where}
        """).fetchone()
        return result[0], result[1]

    # -------------------------------------------------------------------------
    # Boolean Predicates
    # -------------------------------------------------------------------------

    def intersects(self, where_a: str, where_b: str) -> bool:
        """
        Prueft ob die Features von A und B sich ueberschneiden.

        Args:
            where_a: SQL WHERE-Bedingung fuer Feature-Set A
            where_b: SQL WHERE-Bedingung fuer Feature-Set B

        Returns:
            True wenn mindestens eine Cell von A mit einer Cell von B matched

        Example:
            db.intersects("kategorie = 'Wald'", "kategorie = 'See'")
        """
        min_a, max_a = self._get_resolution_range(where_a)
        min_b, max_b = self._get_resolution_range(where_b)

        if min_a is None or min_b is None:
            return False

        target_res = min(min_a, min_b)

        result = self.conn.execute(f"""
            WITH a_parents AS (
                SELECT DISTINCT h3_cell_to_parent(UNNEST(h3_cells), {target_res}) as parent
                FROM features
                WHERE {where_a}
            ),
            b_parents AS (
                SELECT DISTINCT h3_cell_to_parent(UNNEST(h3_cells), {target_res}) as parent
                FROM features
                WHERE {where_b}
            )
            SELECT 1
            FROM a_parents
            JOIN b_parents ON a_parents.parent = b_parents.parent
            LIMIT 1
        """).fetchone()

        return result is not None

    def within(self, where_a: str, where_b: str) -> bool:
        """
        Prueft ob alle Cells von A innerhalb von B liegen.

        Args:
            where_a: SQL WHERE-Bedingung fuer Feature-Set A (das "innere")
            where_b: SQL WHERE-Bedingung fuer Feature-Set B (das "aeussere")

        Returns:
            True wenn alle Cells von A in B enthalten sind

        Example:
            db.within("feature_id = 123", "kategorie = 'Kanton'")
        """
        min_a, max_a = self._get_resolution_range(where_a)
        min_b, max_b = self._get_resolution_range(where_b)

        if min_a is None or min_b is None:
            return False

        target_res = min(min_a, min_b)

        result = self.conn.execute(f"""
            WITH a_cells AS (
                SELECT DISTINCT h3_cell_to_parent(UNNEST(h3_cells), {target_res}) as cell
                FROM features
                WHERE {where_a}
            ),
            b_cells AS (
                SELECT DISTINCT h3_cell_to_parent(UNNEST(h3_cells), {target_res}) as cell
                FROM features
                WHERE {where_b}
            )
            SELECT COUNT(*)
            FROM a_cells
            WHERE cell NOT IN (SELECT cell FROM b_cells)
        """).fetchone()

        return result[0] == 0

    def contains(self, where_a: str, where_b: str) -> bool:
        """
        Prueft ob A alle Cells von B enthaelt.

        Dies ist das Inverse von within(): contains(A, B) == within(B, A)

        Args:
            where_a: SQL WHERE-Bedingung fuer Feature-Set A (das "aeussere")
            where_b: SQL WHERE-Bedingung fuer Feature-Set B (das "innere")

        Returns:
            True wenn alle Cells von B in A enthalten sind

        Example:
            db.contains("kategorie = 'Kanton'", "feature_id = 123")
        """
        return self.within(where_b, where_a)

    # -------------------------------------------------------------------------
    # Intersection (gibt Cells zurueck)
    # -------------------------------------------------------------------------

    def intersection(self, where_a: str, where_b: str) -> tuple[list[str], int]:
        """
        Berechnet die Intersection von A und B.

        Gibt die Cells zurueck die sich ueberschneiden, auf der FEINEREN
        Resolution (fuer maximale Genauigkeit).

        Args:
            where_a: SQL WHERE-Bedingung fuer Feature-Set A
            where_b: SQL WHERE-Bedingung fuer Feature-Set B

        Returns:
            Tuple von (Liste der H3 Cell IDs als Strings, Resolution)
            Bei leerer Intersection: ([], None)

        Example:
            cells, res = db.intersection("kategorie = 'Wald'", "name = 'Zuerichsee'")
            print(f"{len(cells)} Cells auf Resolution {res}")
        """
        min_a, max_a = self._get_resolution_range(where_a)
        min_b, max_b = self._get_resolution_range(where_b)

        if min_a is None or min_b is None:
            return [], None

        # Join auf der groeberen Resolution
        target_res = min(min_a, min_b)

        # Ergebnis auf der feineren Resolution
        result_res = max(max_a, max_b)

        # Bestimme welche Seite die feinere ist
        if max_a >= max_b:
            fine_where = where_a
            coarse_where = where_b
        else:
            fine_where = where_b
            coarse_where = where_a

        # Finde feine Cells deren Parent in der groben Menge ist
        result = self.conn.execute(f"""
            WITH coarse_cells AS (
                SELECT DISTINCT h3_cell_to_parent(UNNEST(h3_cells), {target_res}) as cell
                FROM features
                WHERE {coarse_where}
            )
            SELECT DISTINCT fine.cell
            FROM (
                SELECT UNNEST(h3_cells) as cell
                FROM features
                WHERE {fine_where}
            ) fine
            WHERE h3_cell_to_parent(fine.cell, {target_res}) IN (SELECT cell FROM coarse_cells)
        """).fetchall()

        # Cell IDs von uint64 zu String konvertieren
        cells = [h3.int_to_str(row[0]) for row in result]

        return cells, result_res if cells else None

    # -------------------------------------------------------------------------
    # Utility
    # -------------------------------------------------------------------------

    def count_cells(self, where: str) -> int:
        """Zaehlt die Anzahl H3 Cells fuer eine WHERE-Bedingung."""
        result = self.conn.execute(f"""
            SELECT SUM(h3_cell_count)
            FROM features
            WHERE {where}
        """).fetchone()
        return result[0] or 0

    def count_features(self, where: str) -> int:
        """Zaehlt die Anzahl Features fuer eine WHERE-Bedingung."""
        result = self.conn.execute(f"""
            SELECT COUNT(*)
            FROM features
            WHERE {where}
        """).fetchone()
        return result[0]

    def get_resolutions(self, where: str) -> list[int]:
        """Gibt alle verwendeten Resolutions fuer eine WHERE-Bedingung zurueck."""
        result = self.conn.execute(f"""
            SELECT DISTINCT h3_resolution
            FROM features
            WHERE {where}
            ORDER BY h3_resolution
        """).fetchall()
        return [row[0] for row in result]
