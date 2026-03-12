"""
H3 Engine Sandbox Page.

Interactive spatial queries on H3-indexed geodata with MapLibre visualization.
"""

import base64
import sys
import time
from pathlib import Path

import httpx
from shiny import Inputs, Outputs, Session, reactive, render, ui

# Ensure project root is in path for engine import
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from dashboard.config import DB_PATH, TILE_SERVER_URL, log
from dashboard.components.maplibre import create_maplibre_html, create_empty_map_html
from engine import H3Engine


# Sidebar UI
sandbox_sidebar = ui.sidebar(
    # Query A with Show checkbox
    ui.input_text_area("query_a", "Query A", "NAME IN ('Wehntal','Furttal')", rows=2),
    ui.input_checkbox(
        "show_a",
        ui.tags.span(
            "Show A ",
            ui.tags.span(style="display:inline-block;width:12px;height:12px;background:rgba(0,100,255,0.7);border-radius:2px;vertical-align:middle;"),
        ),
        True
    ),

    # Query B with Show checkbox
    ui.input_text_area("query_b", "Query B", "NAME = 'Lägern' AND OBJEKTART = 'Huegelzug'", rows=2),
    ui.input_checkbox(
        "show_b",
        ui.tags.span(
            "Show B ",
            ui.tags.span(style="display:inline-block;width:12px;height:12px;background:rgba(255,200,0,0.7);border-radius:2px;vertical-align:middle;"),
        ),
        True
    ),

    # Show Original Geometry
    ui.input_checkbox("show_geom", "Show Original Geometry", False),

    # Spatial Operations
    ui.hr(style="margin:8px 0;"),
    ui.tags.div(
        ui.tags.select(
            ui.tags.option("None", value="none"),
            ui.tags.option("Intersection", value="intersection", selected=True),
            ui.tags.option("Union", value="union"),
            id="spatial_operation",
            class_="form-select",
            style="height:38px;flex:1;",
            onchange="Shiny.setInputValue('spatial_operation', this.value)"
        ),
        ui.input_action_button("run_spatial", "Run", class_="btn-primary", style="height:38px;padding:0 16px;"),
        style="display:flex;gap:8px;align-items:center;"
    ),

    # Boolean Predicates
    ui.tags.div(
        ui.input_action_button("run_intersects", "Intersects", class_="btn-outline-secondary btn-sm"),
        ui.input_action_button("run_within", "A within B", class_="btn-outline-secondary btn-sm"),
        ui.input_action_button("run_contains", "A contains B", class_="btn-outline-secondary btn-sm"),
        style="display:flex;flex-direction:column;gap:2px;align-items:center;margin-top:12px;"
    ),

    # Stats output
    ui.hr(style="margin:8px 0;"),
    ui.output_ui("stats"),
    ui.tags.div(
        ui.tags.small(
            "DB query is fast. Map rendering (MVT tiles) causes most wait time.",
            style="color:#666; font-style:italic;"
        ),
        style="margin-top:8px; padding:6px; background:#f8f9fa; border-radius:4px; font-size:11px;"
    ),
    width=300,
)


# Page UI
sandbox_ui = ui.page_sidebar(
    sandbox_sidebar,
    ui.tags.div(
        ui.output_ui("notifications"),
        ui.output_ui("map_output"),
        style="position:relative; height:calc(100vh - 80px);"
    ),
)


def sandbox_server(input: Inputs, output: Outputs, session: Session):
    """Server logic for H3 Engine Sandbox page."""
    log("Initializing H3 Engine Sandbox...")
    engine = H3Engine(DB_PATH)
    log("Database connection established", "success")

    # Reactive values – store query strings and precomputed stats, not Relations
    stats_a = reactive.Value(None)       # {"count": int, "resolutions": list[int]}
    stats_b = reactive.Value(None)
    operation_result = reactive.Value(None)
    operation_elapsed_time = reactive.Value(None)
    notification_list = reactive.Value([])
    display_session_id = reactive.Value(None)
    result_session_id = reactive.Value(None)
    queries_loaded = reactive.Value(False)

    def add_notification(msg, msg_type="warning"):
        current = notification_list.get().copy()
        current.append({"type": msg_type, "message": msg})
        notification_list.set(current)

    def clear_notifications():
        notification_list.set([])

    def _query_stats(query: str) -> dict:
        """Compute feature count and resolutions for a SQL WHERE string."""
        rel = engine.features.filter(query)
        count = rel.aggregate("count(*)").fetchone()[0]
        res = (engine.features.filter(query)
               .project("h3_resolution").distinct()
               .order("h3_resolution").fetchall())
        return {"count": count, "resolutions": [r[0] for r in res]}

    def register_query_with_server(query_a: str, query_b: str, operation: str) -> str:
        """Register query with tile server and return session ID."""
        try:
            response = httpx.post(
                f"{TILE_SERVER_URL}/query",
                json={
                    "query_a": query_a,
                    "query_b": query_b,
                    "operation": operation
                },
                timeout=120.0
            )
            if response.status_code == 200:
                data = response.json()
                return data["session_id"]
            else:
                log(f"Failed to register query: {response.text}", "error")
                return None
        except Exception as e:
            log(f"Error connecting to tile server: {e}", "error")
            add_notification(f"Tile server not available: {e}", "error")
            return None

    # ========================================================================
    # Effects
    # ========================================================================

    @reactive.Effect
    @reactive.event(input.run_spatial)
    def run_spatial_operation():
        """Load queries and execute spatial operation."""
        query_a = input.query_a()
        query_b = input.query_b()
        op = input.spatial_operation()
        show_a = input.show_a()
        show_b = input.show_b()

        # Reset state
        stats_a.set(None)
        stats_b.set(None)
        operation_result.set(None)
        operation_elapsed_time.set(None)
        display_session_id.set(None)
        result_session_id.set(None)
        queries_loaded.set(False)
        clear_notifications()

        # Validate and gather stats for Query A
        has_a = False
        if query_a:
            log(f"Executing Query A: {query_a}")
            try:
                sa = _query_stats(query_a)
                log(f"Query A returned {sa['count']} features", "success")
                stats_a.set(sa)
                has_a = True
            except Exception as e:
                log(f"Query A failed: {e}", "error")
                add_notification(f"Query A failed: {e}", "error")
                return

        # Validate and gather stats for Query B
        has_b = False
        if query_b:
            log(f"Executing Query B: {query_b}")
            try:
                sb = _query_stats(query_b)
                log(f"Query B returned {sb['count']} features", "success")
                stats_b.set(sb)
                has_b = True
            except Exception as e:
                log(f"Query B failed: {e}", "error")
                add_notification(f"Query B failed: {e}", "error")

        queries_loaded.set(True)

        if op == "none":
            send_a = query_a if show_a and query_a else "1=0"
            send_b = query_b if show_b and query_b else "1=0"
            session_id = register_query_with_server(send_a, send_b, "none")
            if session_id:
                display_session_id.set(session_id)
                log(f"Display registered: {session_id}", "success")

        elif op == "intersection":
            if not has_a or not has_b:
                add_notification("Intersection requires both A and B", "warning")
                return

            log("Calculating intersection...")
            try:
                start_time = time.time()
                # Erst union() auf beide Sets, dann intersection()
                union_a = engine.union(query_a)
                union_b = engine.union(query_b)
                cells = engine.intersection(union_a, union_b)
                log(f"intersection Cells: {cells}")
                log(f"Number of cells in intersection: {len(cells)}")
                area = engine.area(cells)
                elapsed = time.time() - start_time
                operation_elapsed_time.set(elapsed)
                operation_result.set({"type": "cells", "area": area, "label": "Intersection"})
                log(f"Intersection area: {area:.2f} km2", "success")

                session_id = register_query_with_server(query_a, query_b, "intersection")
                if session_id:
                    result_session_id.set(session_id)

            except Exception as e:
                log(f"Intersection failed: {e}", "error")
                add_notification(f"Intersection failed: {e}", "error")

        elif op == "union":
            if not has_a:
                add_notification("Union requires query A", "warning")
                return

            log("Calculating union...")
            try:
                start_time = time.time()
                cells = engine.union(query_a)
                area = engine.area(cells)
                elapsed = time.time() - start_time
                operation_elapsed_time.set(elapsed)
                operation_result.set({"type": "cells", "area": area, "label": "Union"})
                log(f"Union area: {area:.2f} km2", "success")

                session_id = register_query_with_server(query_a, "1=0", "union")
                if session_id:
                    result_session_id.set(session_id)

            except Exception as e:
                log(f"Union failed: {e}", "error")
                add_notification(f"Union failed: {e}", "error")

    @reactive.Effect
    @reactive.event(input.run_intersects)
    def run_intersects():
        query_a = input.query_a()
        query_b = input.query_b()
        if not query_a or not query_b:
            add_notification("Enter queries A and B first", "warning")
            return
        log("Checking intersects...")
        try:
            start_time = time.time()
            # Erst union() auf beide Sets, dann intersects()
            union_a = engine.union(query_a)
            union_b = engine.union(query_b)
            val = engine.intersects(union_a, union_b)
            elapsed = time.time() - start_time
            operation_elapsed_time.set(elapsed)
            operation_result.set({"type": "bool", "value": val, "label": "Intersects"})
            log(f"Intersects: {val}", "success")
        except Exception as e:
            log(f"Intersects check failed: {e}", "error")
            add_notification(f"Intersects check failed: {e}", "error")

    @reactive.Effect
    @reactive.event(input.run_within)
    def run_within():
        query_a = input.query_a()
        query_b = input.query_b()
        if not query_a or not query_b:
            add_notification("Enter queries A and B first", "warning")
            return
        log("Checking within...")
        try:
            start_time = time.time()
            # Erst union() auf beide Sets, dann within()
            union_a = engine.union(query_a)
            union_b = engine.union(query_b)
            val = engine.within(union_a, union_b)
            elapsed = time.time() - start_time
            operation_elapsed_time.set(elapsed)
            operation_result.set({"type": "bool", "value": val, "label": "A within B"})
            log(f"Within: {val}", "success")
        except Exception as e:
            log(f"Within check failed: {e}", "error")
            add_notification(f"Within check failed: {e}", "error")

    @reactive.Effect
    @reactive.event(input.run_contains)
    def run_contains():
        query_a = input.query_a()
        query_b = input.query_b()
        if not query_a or not query_b:
            add_notification("Enter queries A and B first", "warning")
            return
        log("Checking contains...")
        try:
            start_time = time.time()
            # Erst union() auf beide Sets, dann contains()
            union_a = engine.union(query_a)
            union_b = engine.union(query_b)
            val = engine.contains(union_a, union_b)
            elapsed = time.time() - start_time
            operation_elapsed_time.set(elapsed)
            operation_result.set({"type": "bool", "value": val, "label": "A contains B"})
            log(f"Contains: {val}", "success")
        except Exception as e:
            log(f"Contains check failed: {e}", "error")
            add_notification(f"Contains check failed: {e}", "error")

    # ========================================================================
    # Renderers
    # ========================================================================

    @render.ui
    def stats():
        sa = stats_a.get()
        sb = stats_b.get()
        op_res = operation_result.get()
        op_time = operation_elapsed_time.get()
        loaded = queries_loaded.get()

        if not loaded:
            return ui.tags.pre(
                "Click 'Run' to start.",
                style="margin:0; padding:8px; background:#f5f5f5; border-radius:4px; font-size:12px; white-space:pre-wrap;"
            )

        lines = []

        if sa is not None:
            lines.append(f"A: {sa['count']} feat, res {sa['resolutions']}")

        if sb is not None:
            lines.append(f"B: {sb['count']} feat, res {sb['resolutions']}")

        if op_res is not None:
            if op_res["type"] == "bool":
                lines.append(f"{op_res['label']}: {op_res['value']}")
            elif op_res["type"] == "cells":
                lines.append(f"{op_res['label']}: {op_res['area']:.2f} km2")

        if op_time is not None:
            lines.append(f"Time: {op_time:.3f}s")

        if op_res and op_res["type"] == "bool":
            bg_color = "#d4edda" if op_res["value"] else "#f8d7da"
            border_color = "#28a745" if op_res["value"] else "#dc3545"
        elif loaded:
            bg_color = "#d4edda"
            border_color = "#28a745"
        else:
            bg_color = "#f5f5f5"
            border_color = "#ddd"

        return ui.tags.pre(
            "\n".join(lines),
            style=f"margin:0; padding:8px; background:{bg_color}; border:2px solid {border_color}; border-radius:4px; font-size:12px; white-space:pre-wrap; transition: all 0.3s ease;"
        )

    @render.ui
    def notifications():
        notifs = notification_list.get()
        if not notifs:
            return ui.TagList()

        items = []
        for n in notifs:
            if n["type"] == "error":
                bg, border, color, icon = "#f8d7da", "#f5c6cb", "#721c24", "X"
            else:
                bg, border, color, icon = "#fff3cd", "#ffc107", "#856404", "!"

            items.append(ui.tags.div(
                ui.tags.span(icon, style="font-weight:bold;margin-right:8px;"),
                n["message"],
                style=f"background:{bg};border:1px solid {border};color:{color};padding:8px 12px;margin-bottom:4px;border-radius:4px;font-size:13px;box-shadow:0 2px 8px rgba(0,0,0,0.15);"
            ))

        return ui.tags.div(
            *items,
            style="position:absolute;top:10px;right:10px;z-index:1000;max-width:400px;"
        )

    @render.ui
    def map_output():
        session_id = result_session_id.get() or display_session_id.get()
        show_a = input.show_a()
        show_b = input.show_b()
        show_geom = input.show_geom()
        has_result = result_session_id.get() is not None

        if session_id:
            html = create_maplibre_html(
                session_id,
                show_h3=True,
                show_geom=show_geom,
                show_a=show_a,
                show_b=show_b,
                show_result=has_result
            )
        else:
            html = create_empty_map_html()

        html_b64 = base64.b64encode(html.encode()).decode()
        return ui.HTML(f'''
            <iframe
                src="data:text/html;base64,{html_b64}"
                style="width:100%;height:calc(100vh - 100px);border:none;">
            </iframe>
        ''')