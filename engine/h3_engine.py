"""
H3Engine - DuckDB-basierte Spatial Predicates fuer H3 Cells.

Alle Predicates nutzen DuckDB's H3 Extension fuer Resolution-Normalisierung
via h3_cell_to_parent(). H3 Cells werden als UBIGINT[] Arrays in der
features Tabelle gespeichert und bei Bedarf via UNNEST expandiert.

Geometrien werden als GEOMETRY-Typ gespeichert (via DuckDB Spatial Extension),
was native Spatial Indexing und ST_* Funktionen ermoeglicht.

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

    # Set-Operationen (geben DuckDBPyRelation mit 'cell' Spalte zurueck)
    cells = db.intersection(wald, seen)
    cells = db.union(wald)

    # Composable mit area():
    db.area(db.intersection(wald, seen))
    db.area(db.union(wald))
"""

from pathlib import Path
from typing import Union

import duckdb

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
        self.conn.execute("INSTALL spatial; LOAD spatial;")
        self.conn.execute("INSTALL h3 FROM community; LOAD h3;")

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
    # Set-Operationen (geben DuckDBPyRelation mit 'cell' Spalte zurueck)
    # -------------------------------------------------------------------------

    def intersection(self, a: FeatureSet, b: FeatureSet) -> duckdb.DuckDBPyRelation:
        """
        Berechnet die Intersection von A und B.

        Gibt eine DuckDBPyRelation mit 'cell' Spalte (UBIGINT) zurueck,
        normalisiert auf die feinste vorkommende Resolution.
        Composable mit area(): engine.area(engine.intersection(a, b))

        Args:
            a: Feature-Set A (SQL WHERE-String oder DuckDBPyRelation)
            b: Feature-Set B (SQL WHERE-String oder DuckDBPyRelation)

        Returns:
            DuckDBPyRelation mit 'cell' Spalte (UBIGINT)

        Example:
            db.area(db.intersection("OBJEKTART = 'Wald'", "OBJEKTART = 'See'"))
        """
        expr_a = self._to_table_expr(a)
        expr_b = self._to_table_expr(b)

        min_a, max_a = self._get_resolution_range(expr_a)
        min_b, max_b = self._get_resolution_range(expr_b)

        if min_a is None or min_b is None:
            return self.conn.sql("SELECT NULL::UBIGINT as cell WHERE false")

        # Join auf der groeberen Resolution
        target_res = min(min_a, min_b)

        # Feinste Resolution im Gesamtresultat (fuer Normalisierung)
        finest_res = max(max_a, max_b)

        # Bestimme welche Seite die feinere ist
        if max_a >= max_b:
            fine_expr = expr_a
            coarse_expr = expr_b
        else:
            fine_expr = expr_b
            coarse_expr = expr_a

        return self.conn.sql(f"""
            WITH coarse_cells AS (
                SELECT DISTINCT h3_cell_to_parent(UNNEST(h3_cells), {target_res}) as cell
                FROM {coarse_expr}
            ),
            intersection_raw AS (
                SELECT fine.cell, fine.res
                FROM (
                    SELECT UNNEST(h3_cells) as cell, h3_resolution as res
                    FROM {fine_expr}
                ) fine
                WHERE h3_cell_to_parent(fine.cell, {target_res})
                      IN (SELECT cell FROM coarse_cells)
            )
            SELECT DISTINCT cell FROM (
                SELECT cell
                FROM intersection_raw
                WHERE res = {finest_res}

                UNION ALL

                SELECT UNNEST(h3_cell_to_children(cell, {finest_res}))
                FROM intersection_raw
                WHERE res < {finest_res}
            )
        """)

    # -------------------------------------------------------------------------
    # Area / Messung
    # -------------------------------------------------------------------------

    def union(self, feature_set: FeatureSet) -> duckdb.DuckDBPyRelation:
        """Normalisiert alle Cells eines Feature-Sets auf die feinste Resolution.

        Expandiert groebere Cells via h3_cell_to_children() und
        dedupliziert, um eine korrekte Union ohne Doppelzaehlung zu erhalten.
        Composable mit area(): engine.area(engine.union(feature_set))

        Args:
            feature_set: SQL WHERE-String oder DuckDBPyRelation

        Returns:
            DuckDBPyRelation mit 'cell' Spalte (UBIGINT)
        """
        expr = self._to_table_expr(feature_set)
        min_res, max_res = self._get_resolution_range(expr)

        if max_res is None:
            return self.conn.sql("SELECT NULL::UBIGINT as cell WHERE false")

        # Alle Resolutions gleich: keine Normalisierung noetig
        if min_res == max_res:
            return self.conn.sql(f"""
                SELECT DISTINCT UNNEST(h3_cells) as cell
                FROM {expr}
            """)

        # Normalisierung: Coarse Cells zu Children auf feinster Resolution expandieren
        return self.conn.sql(f"""
            SELECT DISTINCT cell FROM (
                SELECT UNNEST(h3_cells) as cell
                FROM {expr}
                WHERE h3_resolution = {max_res}

                UNION ALL

                SELECT UNNEST(h3_cell_to_children(cell, {max_res}))
                FROM (
                    SELECT UNNEST(h3_cells) as cell
                    FROM {expr}
                    WHERE h3_resolution < {max_res}
                ) coarse
            )
        """)

    def area(self, cell_set: FeatureSet, unit: str = "km^2") -> float:
        """Berechnet die Flaeche einer Menge von H3 Cells.

        Akzeptiert:
          - DuckDBPyRelation mit 'cell' Spalte (von union()/intersection())
          - FeatureSet (SQL-String oder Relation mit h3_cells) fuer einzelne Features

        Args:
            cell_set: Cell-Relation oder FeatureSet
            unit: Flaecheneinheit ('km^2' oder 'm^2')

        Returns:
            Flaeche in der angegebenen Einheit

        Example:
            db.area(db.union("OBJEKTART = 'Wald'"))
            db.area(db.intersection(wald, seen))
            db.area("feature_id = 123")  # einzelnes Feature
        """
        # Cell-Relation von union()/intersection(): hat 'cell' Spalte
        if (isinstance(cell_set, duckdb.DuckDBPyRelation)
                and 'cell' in cell_set.columns):
            view = self._to_table_expr(cell_set)
            try:
                result = self.conn.execute(f"""
                    SELECT COALESCE(SUM(h3_cell_area(cell, '{unit}')), 0)
                    FROM {view}
                """).fetchone()
                return result[0]
            finally:
                self._cleanup_views(cell_set)

        # FeatureSet: Cells unnesten (fuer einzelne Features / eine Resolution)
        expr = self._to_table_expr(cell_set)
        try:
            result = self.conn.execute(f"""
                WITH distinct_cells AS (
                    SELECT DISTINCT UNNEST(h3_cells) as cell
                    FROM {expr}
                )
                SELECT COALESCE(SUM(h3_cell_area(cell, '{unit}')), 0)
                FROM distinct_cells
            """).fetchone()
            return result[0]
        finally:
            self._cleanup_views(cell_set)

    def total_area(self, resolution: int = 8, unit: str = "km^2") -> float:
        """Berechnet die Gesamtflaeche des Datensatzes (vereinfacht).

        Normalisiert alle Cells auf die angegebene Resolution via
        h3_cell_to_parent(), nimmt DISTINCT, und summiert die Flaechen.

        Args:
            resolution: Ziel-Resolution fuer Normalisierung (default 8)
            unit: Flaecheneinheit ('km^2' oder 'm^2')

        Returns:
            Gesamtflaeche in der angegebenen Einheit
        """
        result = self.conn.execute(f"""
            LOAD h3;

WITH processed_arrays AS (
    SELECT 
        CASE 
            -- Fall A: Zu fein (z.B. Res 11) -> Jedes Element im Array zu Parent Res 10
            WHEN h3_resolution > {resolution} THEN 
                list_transform(h3_cells, x -> h3_cell_to_parent(x, {resolution}))
            
            -- Fall B: Zu grob (z.B. Res 8) -> Jedes Element zu Kindern Res 10 (ergibt Liste von Listen)
            -- flatten() macht daraus wieder ein einfaches Array
            WHEN h3_resolution < {resolution} THEN 
                flatten(list_transform(h3_cells, x -> h3_cell_to_children(x, {resolution})))
            
            -- Fall C: Bereits Res 10
            ELSE h3_cells 
        END AS normalized_array
    FROM features
),
unique_cells AS (
    -- Jetzt erst unnesten wir die bereits transformierten Arrays
    -- DISTINCT verhindert Doppelzählungen überlappender Features
    SELECT DISTINCT UNNEST(normalized_array) AS cell
    FROM processed_arrays
)
SELECT 
    COALESCE(SUM(h3_cell_area(cell, '{unit}')), 0) AS total_area_km2
FROM unique_cells;
        """).fetchone()
        return result[0]

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
