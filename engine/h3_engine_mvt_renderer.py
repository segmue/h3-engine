from typing import Optional

import duckdb

from h3_engine import H3Engine

# Zoom level to H3 resolution mapping for MVT rendering
ZOOM_TO_H3_RESOLUTION = {
    0: 3, 1: 3,
    2: 4, 3: 4,
    4: 5,
    5: 6,
    6: 7,
    7: 8,
    8: 9,
    9: 10,
    10: 11,
    11: 12,
    12: 13,
    13: 14,
    14: 15, 15: 15, 16: 15, 17: 15, 18: 15, 19: 15, 20: 15, 21: 15, 22: 15
}

MAX_TILE_RESOLUTION = 15


def _zoom_to_h3_resolution(zoom: int) -> int:
    """Convert map zoom level to appropriate H3 resolution."""
    zoom = max(0, min(zoom, 22))
    return min(ZOOM_TO_H3_RESOLUTION.get(zoom, 8), MAX_TILE_RESOLUTION)

class H3EngineMVT(H3Engine):

    # -------------------------------------------------------------------------
    # Rendering / MVT Tile Generation
    # -------------------------------------------------------------------------

    def rendering_tile_geometries(
        self,
        where_clause: str,
        z: int,
        x: int,
        y: int,
        layer_name: str = "geom"
    ) -> Optional[bytes]:
        """Generate MVT tile for original geometries (not H3 cells).

        Uses ST_Simplify with zoom-dependent tolerance for smaller tile sizes.

        Args:
            where_clause: SQL WHERE clause to filter features
            z: Tile zoom level
            x: Tile x coordinate
            y: Tile y coordinate
            layer_name: Name for the MVT layer

        Returns:
            MVT binary data, or None if error
        """
        # Zoom-dependent simplification tolerance (in meters, EPSG:3857)
        # Lower zoom = more simplification
        if z <= 8:
            tolerance = 500.0
        elif z <= 12:
            tolerance = 100.0
        else:
            tolerance = 10.0

        sql = f"""
        SELECT ST_AsMVT(tile_data, '{layer_name}', 4096, 'geom') AS mvt
        FROM (
            SELECT {{
                'geom': ST_AsMVTGeom(
                    ST_Simplify(
                        ST_Transform(geometry, 'EPSG:4326', 'EPSG:3857', true),
                        {tolerance}
                    ),
                    ST_Extent(ST_TileEnvelope({z}, {x}, {y})),
                    4096,
                    256,
                    true
                ),
                'feature_id': feature_id,
                'NAME': COALESCE(NAME, ''),
                'OBJEKTART': COALESCE(OBJEKTART, '')
            }} AS tile_data
            FROM features
            WHERE {where_clause}
              AND ST_Intersects(
                  ST_Transform(geometry, 'EPSG:4326', 'EPSG:3857', true),
                  ST_Extent(ST_TileEnvelope({z}, {x}, {y}))
              )
        ) tile
        """

        try:
            result = self.conn.execute(sql).fetchone()
            if result and result[0]:
                return bytes(result[0])
            return b""
        except Exception as e:
            print(f"Error generating geometry tile: {e}")
            return None

    # -------------------------------------------------------------------------
    # Session-based Rendering (optimized: register once per session)
    # -------------------------------------------------------------------------

    def rendering_register_session(
        self,
        session_id: str,
        where_clause_a: Optional[str] = None,
        where_clause_b: Optional[str] = None,
    ) -> dict:
        """Register cells for a session as DuckDB tables (once per session).

        This avoids the overhead of registering DataFrames for every tile request.
        Cells are fetched from the DB and registered as named tables that persist
        for the session lifetime.

        Args:
            session_id: Unique session identifier
            where_clause_a: SQL WHERE clause for layer A
            where_clause_b: SQL WHERE clause for layer B

        Returns:
            Dict with cell counts: {"a": count_a, "b": count_b}
        """
        import pandas as pd
        counts = {"a": 0, "b": 0}

        # Layer A
        if where_clause_a and where_clause_a != "1=0":
            try:
                sql = f"""
                SELECT
                    cell,
                    res,
                    FIRST(objektart) as objektart,
                    FIRST(name) as name
                FROM (
                    SELECT
                        UNNEST(h3_cells) as cell,
                        h3_resolution as res,
                        COALESCE(OBJEKTART, '') as objektart,
                        COALESCE(NAME, '') as name
                    FROM features
                    WHERE {where_clause_a}
                )
                GROUP BY cell, res
                """
                df_a = self.conn.execute(sql).df()
                table_name_a = f"_session_{session_id}_a"
                self.conn.register(table_name_a, df_a)
                counts["a"] = len(df_a)
                print(f"[session] Registered {table_name_a} with {len(df_a)} cells")
            except Exception as e:
                print(f"[session] Error registering layer A: {e}")

        # Layer B
        if where_clause_b and where_clause_b != "1=0":
            try:
                sql = f"""
                SELECT
                    cell,
                    res,
                    FIRST(objektart) as objektart,
                    FIRST(name) as name
                FROM (
                    SELECT
                        UNNEST(h3_cells) as cell,
                        h3_resolution as res,
                        COALESCE(OBJEKTART, '') as objektart,
                        COALESCE(NAME, '') as name
                    FROM features
                    WHERE {where_clause_b}
                )
                GROUP BY cell, res
                """
                df_b = self.conn.execute(sql).df()
                table_name_b = f"_session_{session_id}_b"
                self.conn.register(table_name_b, df_b)
                counts["b"] = len(df_b)
                print(f"[session] Registered {table_name_b} with {len(df_b)} cells")
            except Exception as e:
                print(f"[session] Error registering layer B: {e}")

        return counts

    def rendering_register_result(
        self,
        session_id: str,
        result_relation: duckdb.DuckDBPyRelation,
    ) -> int:
        """Register result cells (from intersection/union) for a session.

        Args:
            session_id: Unique session identifier
            result_relation: DuckDB relation from intersection()/union()

        Returns:
            Number of cells registered
        """
        import pandas as pd
        try:
            # Get the result cells with their actual resolution
            df = result_relation.df()
            if df.empty:
                return 0

            # The result should have 'cell' column, get resolution from h3
            # h3_get_resolution returns the resolution of a cell
            result_sql = """
            SELECT
                cell,
                h3_get_resolution(cell) as res,
                '' as objektart,
                '' as name
            FROM result_df
            """
            self.conn.register("result_df", df)
            df_with_res = self.conn.execute(result_sql).df()
            self.conn.unregister("result_df")

            table_name = f"_session_{session_id}_result"
            self.conn.register(table_name, df_with_res)
            print(f"[session] Registered {table_name} with {len(df_with_res)} cells")
            return len(df_with_res)
        except Exception as e:
            print(f"[session] Error registering result: {e}")
            return 0

    def rendering_tile_from_session(
        self,
        session_id: str,
        layer: str,
        z: int,
        x: int,
        y: int,
    ) -> Optional[bytes]:
        """Generate MVT tile from session-registered cells (fast path).

        Uses pre-registered tables instead of registering DataFrames per tile.

        Args:
            session_id: Session identifier
            layer: Layer name ("a", "b", or "result")
            z: Tile zoom level
            x: Tile x coordinate
            y: Tile y coordinate

        Returns:
            MVT binary data, or None if error
        """
        table_name = f"_session_{session_id}_{layer}"
        target_res = _zoom_to_h3_resolution(z)

        sql = f"""
        SELECT ST_AsMVT(tile_data, '{layer}', 4096, 'geom') AS mvt
        FROM (
            SELECT {{
                'geom': ST_AsMVTGeom(
                    ST_ReducePrecision(
                        ST_Transform(
                            h3_cell_to_boundary_wkt(agg_cell)::GEOMETRY,
                            'EPSG:4326', 'EPSG:3857', true
                        ),
                        1.0
                    ),
                    ST_Extent(ST_TileEnvelope({z}, {x}, {y})),
                    4096,
                    256,
                    true
                ),
                'h3_id': h3_h3_to_string(agg_cell),
                'OBJEKTART': FIRST(objektart),
                'NAME': FIRST(name),
                'h3_resolution': FIRST(res)::INTEGER
            }} AS tile_data
            FROM (
                SELECT
                    h3_cell_to_parent(
                        cell::UBIGINT,
                        LEAST({target_res}, res)::INTEGER
                    ) AS agg_cell,
                    objektart,
                    name,
                    res
                FROM {table_name}
            ) cells
            WHERE ST_Intersects(
                ST_Transform(
                    h3_cell_to_boundary_wkt(agg_cell)::GEOMETRY,
                    'EPSG:4326', 'EPSG:3857', true
                ),
                ST_Extent(ST_TileEnvelope({z}, {x}, {y}))
            )
            GROUP BY agg_cell
        ) tile
        """

        try:
            result = self.conn.execute(sql).fetchone()
            if result and result[0]:
                return bytes(result[0])
            return b""
        except Exception as e:
            # Table might not exist yet (precomputation in progress)
            if "does not exist" not in str(e).lower():
                print(f"[session] Error generating tile: {e}")
            return b""

    def rendering_unregister_session(self, session_id: str):
        """Unregister all tables for a session.

        Args:
            session_id: Session identifier to clean up
        """
        for layer in ["a", "b", "result"]:
            table_name = f"_session_{session_id}_{layer}"
            try:
                self.conn.unregister(table_name)
                print(f"[session] Unregistered {table_name}")
            except Exception:
                pass  # Table might not exist
