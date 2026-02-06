"""
H3Engine - DuckDB-basierte Spatial Predicates fuer H3 Cells.

Alle Predicates nutzen DuckDB's H3 Extension fuer Resolution-Normalisierung
via h3_cell_to_parent(). H3 Cells werden als UBIGINT[] Arrays in der
features Tabelle gespeichert und bei Bedarf via UNNEST expandiert.

Fuer allgemeine Queries wird DuckDB's Relational API direkt exponiert via
db.features (DuckDBPyRelation). Spatial Predicates akzeptieren sowohl
SQL WHERE-Strings als auch DuckDBPyRelation Objekte.

Verwendung:
    from engine import H3Engine

    db = H3Engine("data.duckdb")

    # DuckDB Relational API fuer allgemeine Queries
    wald = db.features.filter("OBJEKTART = 'Wald'")
    wald.aggregate("count(*)").df()
    wald.project("NAME, h3_resolution").order("NAME").limit(10).df()

    # Spatial Predicates (str oder DuckDBPyRelation)
    seen = db.features.filter("OBJEKTART = 'See'")
    db.intersects(wald, seen)
    db.intersects("OBJEKTART = 'Wald'", "OBJEKTART = 'See'")  # old-style

    # Intersection mit Cells
    cells, resolution = db.intersection(wald, seen)
"""

from pathlib import Path
from typing import Union

import duckdb
import h3

# Typ-Alias fuer Predicate-Argumente: SQL-String oder DuckDB Relation
FeatureSet = Union[str, duckdb.DuckDBPyRelation]


class H3Engine:
    """DuckDB-basierte H3 Spatial Query Engine.

    Kombiniert DuckDB's Relational API (fuer allgemeine Queries) mit
    spezialisierten H3 Spatial Predicates (intersects, within, contains,
    intersection).

    Attributes:
        conn: DuckDB Connection (direkt nutzbar fuer Raw SQL)
    """

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
    # DuckDB Relational API
    # -------------------------------------------------------------------------

    @property
    def features(self) -> duckdb.DuckDBPyRelation:
        """DuckDB Relation auf die features Tabelle.

        Gibt ein DuckDBPyRelation zurueck -- DuckDB's volle Relational API
        ist direkt verfuegbar:

            db.features.filter("OBJEKTART = 'Wald'").aggregate("count(*)").df()
            db.features.project("NAME, h3_resolution").order("NAME").limit(10).df()

        Kann auch als Input fuer Spatial Predicates genutzt werden:

            wald = db.features.filter("OBJEKTART = 'Wald'")
            db.intersects(wald, seen)
        """
        return self.conn.table("features")

    # -------------------------------------------------------------------------
    # Hilfsmethoden
    # -------------------------------------------------------------------------

    def _to_table_expr(self, condition: FeatureSet) -> str:
        """Konvertiert str oder DuckDBPyRelation zu einem SQL-Tabellenausdruck.

        Args:
            condition: SQL WHERE-String oder DuckDBPyRelation

        Returns:
            SQL-Fragment das als FROM-Quelle nutzbar ist
        """
        if isinstance(condition, str):
            return f"(SELECT * FROM features WHERE {condition})"

        # DuckDBPyRelation: als temporaere View registrieren
        view_name = f"_h3_tmp_{id(condition)}"
        self.conn.register(view_name, condition)
        return view_name

    def _cleanup_views(self, *conditions: FeatureSet) -> None:
        """Raeumt temporaere Views auf die fuer Relations erstellt wurden."""
        for cond in conditions:
            if isinstance(cond, duckdb.DuckDBPyRelation):
                view_name = f"_h3_tmp_{id(cond)}"
                try:
                    self.conn.unregister(view_name)
                except Exception:
                    pass

    def _get_resolution_range(self, table_expr: str) -> tuple[int, int]:
        """Ermittelt min/max Resolution fuer einen Tabellenausdruck."""
        result = self.conn.execute(f"""
            SELECT MIN(h3_resolution), MAX(h3_resolution)
            FROM {table_expr}
        """).fetchone()
        return result[0], result[1]

    # -------------------------------------------------------------------------
    # Boolean Predicates
    # -------------------------------------------------------------------------

    def intersects(self, a: FeatureSet, b: FeatureSet) -> bool:
        """
        Prueft ob die Features von A und B sich ueberschneiden.

        Args:
            a: Feature-Set A (SQL WHERE-String oder DuckDBPyRelation)
            b: Feature-Set B (SQL WHERE-String oder DuckDBPyRelation)

        Returns:
            True wenn mindestens eine Cell von A mit einer Cell von B matched

        Example:
            wald = db.features.filter("OBJEKTART = 'Wald'")
            seen = db.features.filter("OBJEKTART = 'See'")
            db.intersects(wald, seen)
            db.intersects("OBJEKTART = 'Wald'", "OBJEKTART = 'See'")
        """
        expr_a = self._to_table_expr(a)
        expr_b = self._to_table_expr(b)

        try:
            min_a, max_a = self._get_resolution_range(expr_a)
            min_b, max_b = self._get_resolution_range(expr_b)

            if min_a is None or min_b is None:
                return False

            target_res = min(min_a, min_b)

            result = self.conn.execute(f"""
                WITH a_parents AS (
                    SELECT DISTINCT h3_cell_to_parent(UNNEST(h3_cells), {target_res}) as parent
                    FROM {expr_a}
                ),
                b_parents AS (
                    SELECT DISTINCT h3_cell_to_parent(UNNEST(h3_cells), {target_res}) as parent
                    FROM {expr_b}
                )
                SELECT 1
                FROM a_parents
                JOIN b_parents ON a_parents.parent = b_parents.parent
                LIMIT 1
            """).fetchone()

            return result is not None
        finally:
            self._cleanup_views(a, b)

    def within(self, a: FeatureSet, b: FeatureSet) -> bool:
        """
        Prueft ob alle Cells von A innerhalb von B liegen.

        Args:
            a: Feature-Set A, das "innere" (SQL WHERE-String oder DuckDBPyRelation)
            b: Feature-Set B, das "aeussere" (SQL WHERE-String oder DuckDBPyRelation)

        Returns:
            True wenn alle Cells von A in B enthalten sind

        Example:
            db.within("feature_id = 123", db.features.filter("OBJEKTART = 'Kanton'"))
        """
        expr_a = self._to_table_expr(a)
        expr_b = self._to_table_expr(b)

        try:
            min_a, max_a = self._get_resolution_range(expr_a)
            min_b, max_b = self._get_resolution_range(expr_b)

            if min_a is None or min_b is None:
                return False

            target_res = min(min_a, min_b)

            result = self.conn.execute(f"""
                WITH a_cells AS (
                    SELECT DISTINCT h3_cell_to_parent(UNNEST(h3_cells), {target_res}) as cell
                    FROM {expr_a}
                ),
                b_cells AS (
                    SELECT DISTINCT h3_cell_to_parent(UNNEST(h3_cells), {target_res}) as cell
                    FROM {expr_b}
                )
                SELECT COUNT(*)
                FROM a_cells
                WHERE cell NOT IN (SELECT cell FROM b_cells)
            """).fetchone()

            return result[0] == 0
        finally:
            self._cleanup_views(a, b)

    def contains(self, a: FeatureSet, b: FeatureSet) -> bool:
        """
        Prueft ob A alle Cells von B enthaelt.

        Dies ist das Inverse von within(): contains(A, B) == within(B, A)

        Args:
            a: Feature-Set A, das "aeussere" (SQL WHERE-String oder DuckDBPyRelation)
            b: Feature-Set B, das "innere" (SQL WHERE-String oder DuckDBPyRelation)

        Returns:
            True wenn alle Cells von B in A enthalten sind

        Example:
            kanton = db.features.filter("OBJEKTART = 'Kanton'")
            db.contains(kanton, "feature_id = 123")
        """
        return self.within(b, a)

    # -------------------------------------------------------------------------
    # Intersection (gibt Cells zurueck)
    # -------------------------------------------------------------------------

    def intersection(self, a: FeatureSet, b: FeatureSet) -> tuple[list[str], int]:
        """
        Berechnet die Intersection von A und B.

        Gibt die Cells zurueck die sich ueberschneiden, auf der FEINEREN
        Resolution (fuer maximale Genauigkeit).

        Args:
            a: Feature-Set A (SQL WHERE-String oder DuckDBPyRelation)
            b: Feature-Set B (SQL WHERE-String oder DuckDBPyRelation)

        Returns:
            Tuple von (Liste der H3 Cell IDs als Strings, Resolution)
            Bei leerer Intersection: ([], None)

        Example:
            wald = db.features.filter("OBJEKTART = 'Wald'")
            orte = db.features.filter("OBJEKTART = 'Ort'")
            cells, res = db.intersection(wald, orte)
            print(f"{len(cells)} Cells auf Resolution {res}")
        """
        expr_a = self._to_table_expr(a)
        expr_b = self._to_table_expr(b)

        try:
            min_a, max_a = self._get_resolution_range(expr_a)
            min_b, max_b = self._get_resolution_range(expr_b)

            if min_a is None or min_b is None:
                return [], None

            # Join auf der groeberen Resolution
            target_res = min(min_a, min_b)

            # Ergebnis auf der feineren Resolution
            result_res = max(max_a, max_b)

            # Bestimme welche Seite die feinere ist
            if max_a >= max_b:
                fine_expr = expr_a
                coarse_expr = expr_b
            else:
                fine_expr = expr_b
                coarse_expr = expr_a

            # Finde feine Cells deren Parent in der groben Menge ist
            result = self.conn.execute(f"""
                WITH coarse_cells AS (
                    SELECT DISTINCT h3_cell_to_parent(UNNEST(h3_cells), {target_res}) as cell
                    FROM {coarse_expr}
                )
                SELECT DISTINCT fine.cell
                FROM (
                    SELECT UNNEST(h3_cells) as cell
                    FROM {fine_expr}
                ) fine
                WHERE h3_cell_to_parent(fine.cell, {target_res}) IN (SELECT cell FROM coarse_cells)
            """).fetchall()

            # Cell IDs von uint64 zu String konvertieren
            cells = [h3.int_to_str(row[0]) for row in result]

            return cells, result_res if cells else None
        finally:
            self._cleanup_views(a, b)

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
