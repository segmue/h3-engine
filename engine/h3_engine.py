"""
H3Engine - DuckDB-basierte Spatial Predicates fuer H3 Cells.

Alle Predicates unterstuetzen verschiedene Resolution-Levels durch
vorberechnete Parent-Spalten in der h3_index Tabelle.

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

    # Parent-Spalten Range (muss mit import_to_duckdb.py uebereinstimmen)
    MIN_PARENT_RES = 5
    MAX_PARENT_RES = 14

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
            SELECT MIN(i.resolution), MAX(i.resolution)
            FROM h3_index i
            JOIN features f ON i.feature_id = f.feature_id
            WHERE {where}
        """).fetchone()
        return result[0], result[1]

    def _get_join_column(self, res_a: int, res_b: int) -> tuple[str, str, int]:
        """
        Bestimmt die Join-Spalten basierend auf den Resolutions.

        Returns:
            (column_a, column_b, target_resolution)
        """
        target_res = min(res_a, res_b)

        # Spalte fuer A
        if res_a == target_res:
            col_a = "a.h3_cell"
        else:
            col_a = f"a.parent_{target_res}"

        # Spalte fuer B
        if res_b == target_res:
            col_b = "b.h3_cell"
        else:
            col_b = f"b.parent_{target_res}"

        return col_a, col_b, target_res

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
        # Resolutions ermitteln
        min_a, max_a = self._get_resolution_range(where_a)
        min_b, max_b = self._get_resolution_range(where_b)

        if min_a is None or min_b is None:
            return False  # Keine Daten

        # Zur gröberen Resolution joinen
        target_res = min(min_a, min_b)
        col_a, col_b, _ = self._get_join_column(min_a, target_res)
        col_a2, col_b2, _ = self._get_join_column(max_a, target_res)

        # Bei gemischten Resolutions: konservativ die gröbste nehmen
        col_a = f"COALESCE(a.parent_{target_res}, a.h3_cell)" if min_a != max_a else col_a
        col_b = f"COALESCE(b.parent_{target_res}, b.h3_cell)" if min_b != max_b else col_b

        # Einfacher Fall: gleiche oder ähnliche Resolution
        result = self.conn.execute(f"""
            SELECT 1
            FROM h3_index a
            JOIN features fa ON a.feature_id = fa.feature_id
            JOIN h3_index b ON {col_a} = {col_b}
            JOIN features fb ON b.feature_id = fb.feature_id
            WHERE ({where_a.replace('feature_id', 'fa.feature_id').replace('kategorie', 'fa.kategorie')})
              AND ({where_b.replace('feature_id', 'fb.feature_id').replace('kategorie', 'fb.kategorie')})
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
        # Resolutions ermitteln
        min_a, max_a = self._get_resolution_range(where_a)
        min_b, max_b = self._get_resolution_range(where_b)

        if min_a is None or min_b is None:
            return False

        target_res = min(min_a, min_b)

        # Zaehle A-Cells die NICHT in B sind
        result = self.conn.execute(f"""
            WITH a_cells AS (
                SELECT DISTINCT COALESCE(a.parent_{target_res}, a.h3_cell) as cell
                FROM h3_index a
                JOIN features fa ON a.feature_id = fa.feature_id
                WHERE {where_a}
            ),
            b_cells AS (
                SELECT DISTINCT COALESCE(b.parent_{target_res}, b.h3_cell) as cell
                FROM h3_index b
                JOIN features fb ON b.feature_id = fb.feature_id
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
        # Resolutions ermitteln
        min_a, max_a = self._get_resolution_range(where_a)
        min_b, max_b = self._get_resolution_range(where_b)

        if min_a is None or min_b is None:
            return [], None

        # Join auf der gröberen Resolution
        target_res = min(min_a, min_b)

        # Ergebnis auf der feineren Resolution
        result_res = max(max_a, max_b)

        # Bestimme welche Seite die feinere ist
        if max_a >= max_b:
            # A ist feiner oder gleich -> A-Cells zurueckgeben
            fine_side = "a"
            coarse_side = "b"
            fine_where = where_a
            coarse_where = where_b
        else:
            # B ist feiner -> B-Cells zurueckgeben
            fine_side = "b"
            coarse_side = "a"
            fine_where = where_b
            coarse_where = where_a

        # Query: Finde feine Cells deren Parent in der groben Menge ist
        result = self.conn.execute(f"""
            WITH coarse_cells AS (
                SELECT DISTINCT COALESCE(c.parent_{target_res}, c.h3_cell) as cell
                FROM h3_index c
                JOIN features fc ON c.feature_id = fc.feature_id
                WHERE {coarse_where}
            )
            SELECT DISTINCT f.h3_cell
            FROM h3_index f
            JOIN features ff ON f.feature_id = ff.feature_id
            WHERE {fine_where}
              AND COALESCE(f.parent_{target_res}, f.h3_cell) IN (SELECT cell FROM coarse_cells)
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
            SELECT COUNT(*)
            FROM h3_index i
            JOIN features f ON i.feature_id = f.feature_id
            WHERE {where}
        """).fetchone()
        return result[0]

    def count_features(self, where: str) -> int:
        """Zaehlt die Anzahl Features fuer eine WHERE-Bedingung."""
        result = self.conn.execute(f"""
            SELECT COUNT(DISTINCT feature_id)
            FROM features
            WHERE {where}
        """).fetchone()
        return result[0]

    def get_resolutions(self, where: str) -> list[int]:
        """Gibt alle verwendeten Resolutions fuer eine WHERE-Bedingung zurueck."""
        result = self.conn.execute(f"""
            SELECT DISTINCT i.resolution
            FROM h3_index i
            JOIN features f ON i.feature_id = f.feature_id
            WHERE {where}
            ORDER BY i.resolution
        """).fetchall()
        return [row[0] for row in result]
