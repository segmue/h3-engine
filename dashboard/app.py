"""
H3 Spatial Sandbox - Shiny Dashboard with MapLibre.

This dashboard provides an interactive interface for spatial queries on H3-indexed
geodata. It uses MapLibre GL JS for rendering MVT tiles served by the FastAPI
tile server.

Architecture:
- Shiny (Port 8000): UI, state management, engine calls
- FastAPI (Port 8001): MVT tile serving

Run with:
    shiny run dashboard/app.py --host 0.0.0.0 --port 8000 --reload
"""

import base64
import os
import time
from pathlib import Path

import httpx
from shiny import App, Inputs, Outputs, Session, reactive, render, ui

import sys
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from engine import H3Engine

# Configuration
DB_PATH = project_root / "data" / "swissNAMES3D_combined_h3.duckdb"
TILE_SERVER_URL = os.environ.get("TILE_SERVER_URL", "http://localhost:8001")


def log(message, level="info"):
    """Log a message with timestamp."""
    timestamp = time.strftime("%H:%M:%S")
    prefix = {"info": "[i]", "success": "[+]", "warning": "[!]", "error": "[x]"}.get(level, "[i]")
    print(f"{prefix} {timestamp} {message}")


def create_maplibre_html(
    session_id: str,
    show_h3: bool = True,
    show_geom: bool = False,
    show_a: bool = True,
    show_b: bool = True,
    show_result: bool = False
) -> str:
    """Generate MapLibre GL JS HTML with vector tile sources."""

    layers_js = []

    # Layer A - Blue H3 hexagons (only if show_a)
    if show_h3 and show_a:
        layers_js.append(f"""
            map.addSource('h3-a', {{
                type: 'vector',
                tiles: ['{TILE_SERVER_URL}/tiles/a/{{z}}/{{x}}/{{y}}.mvt?session={session_id}'],
                minzoom: 0,
                maxzoom: 18
            }});
            map.addLayer({{
                id: 'h3-a-fill',
                type: 'fill',
                source: 'h3-a',
                'source-layer': 'a',
                paint: {{
                    'fill-color': 'rgba(0, 100, 255, 0.4)',
                    'fill-outline-color': 'rgba(0, 50, 200, 0.8)'
                }}
            }});
        """)

    # Layer B - Yellow H3 hexagons (only if show_b)
    if show_h3 and show_b:
        layers_js.append(f"""
            map.addSource('h3-b', {{
                type: 'vector',
                tiles: ['{TILE_SERVER_URL}/tiles/b/{{z}}/{{x}}/{{y}}.mvt?session={session_id}'],
                minzoom: 0,
                maxzoom: 18
            }});
            map.addLayer({{
                id: 'h3-b-fill',
                type: 'fill',
                source: 'h3-b',
                'source-layer': 'b',
                paint: {{
                    'fill-color': 'rgba(255, 200, 0, 0.4)',
                    'fill-outline-color': 'rgba(200, 150, 0, 0.8)'
                }}
            }});
        """)

    # Result Layer - Green H3 hexagons (only if show_result)
    if show_h3 and show_result:
        layers_js.append(f"""
            map.addSource('h3-result', {{
                type: 'vector',
                tiles: ['{TILE_SERVER_URL}/tiles/result/{{z}}/{{x}}/{{y}}.mvt?session={session_id}'],
                minzoom: 0,
                maxzoom: 18
            }});
            map.addLayer({{
                id: 'h3-result-fill',
                type: 'fill',
                source: 'h3-result',
                'source-layer': 'result',
                paint: {{
                    'fill-color': 'rgba(0, 255, 100, 0.6)',
                    'fill-outline-color': 'rgba(0, 200, 50, 1)'
                }}
            }});
        """)

    # Original geometries (respect show_a and show_b)
    if show_geom and show_a:
        layers_js.append(f"""
            map.addSource('geom-a', {{
                type: 'vector',
                tiles: ['{TILE_SERVER_URL}/tiles/geom_a/{{z}}/{{x}}/{{y}}.mvt?session={session_id}'],
                minzoom: 0,
                maxzoom: 18
            }});
            map.addLayer({{
                id: 'geom-a-fill',
                type: 'fill',
                source: 'geom-a',
                'source-layer': 'geom_a',
                paint: {{
                    'fill-color': 'rgba(0, 100, 255, 0.3)',
                    'fill-outline-color': 'rgba(0, 50, 200, 1)'
                }}
            }});
            map.addLayer({{
                id: 'geom-a-line',
                type: 'line',
                source: 'geom-a',
                'source-layer': 'geom_a',
                paint: {{
                    'line-color': 'rgba(0, 50, 200, 1)',
                    'line-width': 2
                }}
            }});
        """)

    if show_geom and show_b:
        layers_js.append(f"""
            map.addSource('geom-b', {{
                type: 'vector',
                tiles: ['{TILE_SERVER_URL}/tiles/geom_b/{{z}}/{{x}}/{{y}}.mvt?session={session_id}'],
                minzoom: 0,
                maxzoom: 18
            }});
            map.addLayer({{
                id: 'geom-b-fill',
                type: 'fill',
                source: 'geom-b',
                'source-layer': 'geom_b',
                paint: {{
                    'fill-color': 'rgba(255, 200, 0, 0.3)',
                    'fill-outline-color': 'rgba(200, 150, 0, 1)'
                }}
            }});
            map.addLayer({{
                id: 'geom-b-line',
                type: 'line',
                source: 'geom-b',
                'source-layer': 'geom_b',
                paint: {{
                    'line-color': 'rgba(200, 150, 0, 1)',
                    'line-width': 2
                }}
            }});
        """)

    layers_code = "\n".join(layers_js)

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <script src="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"></script>
        <link href="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css" rel="stylesheet">
        <style>
            body {{ margin: 0; padding: 0; }}
            #map {{ position: absolute; top: 0; bottom: 0; width: 100%; }}
            .zoom-info {{
                position: absolute;
                bottom: 10px;
                left: 10px;
                background: white;
                padding: 5px 10px;
                border-radius: 4px;
                font-family: sans-serif;
                font-size: 12px;
                box-shadow: 0 1px 4px rgba(0,0,0,0.2);
            }}
        </style>
    </head>
    <body>
        <div id="map"></div>
        <div class="zoom-info" id="zoom-info">Zoom: 7</div>
        <script>
            const map = new maplibregl.Map({{
                container: 'map',
                style: {{
                    version: 8,
                    sources: {{
                        'carto-light': {{
                            type: 'raster',
                            tiles: ['https://basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}@2x.png'],
                            tileSize: 256,
                            attribution: '&copy; <a href="https://carto.com/">CARTO</a>'
                        }}
                    }},
                    layers: [{{
                        id: 'carto-light-layer',
                        type: 'raster',
                        source: 'carto-light',
                        minzoom: 0,
                        maxzoom: 22
                    }}]
                }},
                center: [8.2, 46.8],
                zoom: 7
            }});

            map.addControl(new maplibregl.NavigationControl(), 'top-right');

            map.on('load', function() {{
                {layers_code}

                // Popup for H3 layers - query all features under cursor
                const popup = new maplibregl.Popup({{
                    closeButton: false,
                    closeOnClick: false
                }});

                const interactiveLayers = ['h3-result-fill', 'h3-a-fill', 'h3-b-fill', 'geom-a-fill', 'geom-b-fill'];
                const layerLabels = {{
                    'h3-result-fill': 'Result',
                    'h3-a-fill': 'A',
                    'h3-b-fill': 'B',
                    'geom-a-fill': 'Geom A',
                    'geom-b-fill': 'Geom B'
                }};

                map.on('mousemove', function(e) {{
                    // Query all interactive layers at cursor position
                    const features = map.queryRenderedFeatures(e.point, {{
                        layers: interactiveLayers.filter(l => map.getLayer(l))
                    }});

                    if (features.length === 0) {{
                        map.getCanvas().style.cursor = '';
                        popup.remove();
                        return;
                    }}

                    map.getCanvas().style.cursor = 'pointer';

                    // Priority: Result > A > B (first in interactiveLayers order)
                    // Group features by layer
                    const byLayer = {{}};
                    features.forEach(f => {{
                        if (!byLayer[f.layer.id]) byLayer[f.layer.id] = [];
                        byLayer[f.layer.id].push(f);
                    }});

                    // Build HTML for all visible layers at this point
                    let html = '';
                    interactiveLayers.forEach(layerId => {{
                        if (byLayer[layerId] && byLayer[layerId].length > 0) {{
                            const props = byLayer[layerId][0].properties;
                            const label = layerLabels[layerId];
                            let layerHtml = '';
                            if (props.NAME) layerHtml += props.NAME;
                            if (props.OBJEKTART) layerHtml += (layerHtml ? ' (' + props.OBJEKTART + ')' : props.OBJEKTART);
                            if (props.h3_resolution) layerHtml += ' [res ' + props.h3_resolution + ']';
                            if (layerHtml) {{
                                html += '<b>' + label + ':</b> ' + layerHtml + '<br>';
                            }} else if (props.h3_id) {{
                                html += '<b>' + label + ':</b> ' + props.h3_id + '<br>';
                            }}
                        }}
                    }});

                    if (html) {{
                        popup.setLngLat(e.lngLat).setHTML(html).addTo(map);
                    }} else {{
                        popup.remove();
                    }}
                }});

                map.on('mouseleave', function() {{
                    map.getCanvas().style.cursor = '';
                    popup.remove();
                }});
            }});

            // Zoom to H3 resolution mapping
            const zoomToH3Res = {{
                0: 3, 1: 3, 2: 4, 3: 4, 4: 5, 5: 6, 6: 7, 7: 8,
                8: 9, 9: 10, 10: 11, 11: 12, 12: 13, 13: 14,
                14: 15, 15: 15, 16: 15, 17: 15, 18: 15, 19: 15, 20: 15, 21: 15, 22: 15
            }};

            function updateZoomInfo() {{
                const zoom = Math.round(map.getZoom());
                const h3Res = zoomToH3Res[Math.min(Math.max(zoom, 0), 22)] || 8;
                document.getElementById('zoom-info').textContent = 'Zoom: ' + zoom + ' | H3 Res: ' + h3Res;
                if (window.Shiny) {{
                    Shiny.setInputValue('map_zoom', zoom);
                }}
            }}

            // Update zoom display
            map.on('zoomend', updateZoomInfo);
            map.on('load', updateZoomInfo);
        </script>
    </body>
    </html>
    """


# UI Definition
app_ui = ui.page_sidebar(
    ui.sidebar(
        ui.h4("H3 Spatial Sandbox", style="margin:0 0 8px 0;"),

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

        # Show Original Geometry (no section title)
        ui.input_checkbox("show_geom", "Show Original Geometry", False),

        # Spatial Operations (no title)
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

        # Boolean Predicates (no title, stacked vertically, centered)
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
    ),
    ui.tags.div(
        ui.output_ui("notifications"),
        ui.output_ui("map_output"),
        style="position:relative; height:100%;"
    ),
)


def server(input: Inputs, output: Outputs, session: Session):
    log("Initializing dashboard server...")
    engine = H3Engine(DB_PATH)
    log("Database connection established", "success")

    # Reactive values
    result_a = reactive.Value(None)
    result_b = reactive.Value(None)
    operation_result = reactive.Value(None)
    operation_elapsed_time = reactive.Value(None)
    notification_list = reactive.Value([])

    # Session IDs - split for different purposes
    display_session_id = reactive.Value(None)  # For base A/B display
    result_session_id = reactive.Value(None)   # For spatial operation results
    queries_loaded = reactive.Value(False)     # Track if queries have been loaded

    def add_notification(msg, msg_type="warning"):
        current = notification_list.get().copy()
        current.append({"type": msg_type, "message": msg})
        notification_list.set(current)

    def clear_notifications():
        notification_list.set([])

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
                timeout=120.0  # Large queries need more time for precomputation
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
        result_a.set(None)
        result_b.set(None)
        operation_result.set(None)
        operation_elapsed_time.set(None)
        display_session_id.set(None)
        result_session_id.set(None)
        queries_loaded.set(False)
        clear_notifications()

        # Execute Query A
        if query_a:
            log(f"Executing Query A: {query_a}")
            try:
                res_a = engine.features.filter(query_a)
                count = res_a.aggregate("count(*)").fetchone()[0]
                log(f"Query A returned {count} features", "success")
                result_a.set(res_a)
            except Exception as e:
                log(f"Query A failed: {e}", "error")
                add_notification(f"Query A failed: {e}", "error")
                return

        # Execute Query B
        if query_b:
            log(f"Executing Query B: {query_b}")
            try:
                res_b = engine.features.filter(query_b)
                count = res_b.aggregate("count(*)").fetchone()[0]
                log(f"Query B returned {count} features", "success")
                result_b.set(res_b)
            except Exception as e:
                log(f"Query B failed: {e}", "error")
                add_notification(f"Query B failed: {e}", "error")

        queries_loaded.set(True)

        a = result_a.get()
        b = result_b.get()

        if op == "none":
            # Just display queries based on checkboxes
            send_a = query_a if show_a and query_a else "1=0"
            send_b = query_b if show_b and query_b else "1=0"
            session_id = register_query_with_server(send_a, send_b, "none")
            if session_id:
                display_session_id.set(session_id)
                log(f"Display registered: {session_id}", "success")

        elif op == "intersection":
            if not a or not b:
                add_notification("Intersection requires both A and B", "warning")
                return

            log("Calculating intersection...")
            try:
                start_time = time.time()
                cells = engine.intersection(a, b)
                elapsed = time.time() - start_time
                operation_elapsed_time.set(elapsed)
                area = engine.area(cells)
                operation_result.set({"type": "cells", "value": cells, "area": area, "label": "Intersection"})
                log(f"Intersection area: {area:.2f} km2", "success")

                # Register result with tile server
                session_id = register_query_with_server(query_a, query_b, "intersection")
                if session_id:
                    result_session_id.set(session_id)

            except Exception as e:
                log(f"Intersection failed: {e}", "error")
                add_notification(f"Intersection failed: {e}", "error")

        elif op == "union":
            if not a:
                add_notification("Union requires query A", "warning")
                return

            log("Calculating union...")
            try:
                start_time = time.time()
                cells = engine.union(a)
                elapsed = time.time() - start_time
                operation_elapsed_time.set(elapsed)
                area = engine.area(cells)
                operation_result.set({"type": "cells", "value": cells, "area": area, "label": "Union"})
                log(f"Union area: {area:.2f} km2", "success")

                # Register result with tile server
                session_id = register_query_with_server(query_a, "1=0", "union")
                if session_id:
                    result_session_id.set(session_id)

            except Exception as e:
                log(f"Union failed: {e}", "error")
                add_notification(f"Union failed: {e}", "error")

    # Boolean Predicates - NO session_id.set(), NO map reload
    @reactive.Effect
    @reactive.event(input.run_intersects)
    def run_intersects():
        """Check if A and B intersect (boolean only, no map reload)."""
        a = result_a.get()
        b = result_b.get()

        if not a or not b:
            add_notification("Load queries A and B first", "warning")
            return

        log("Checking intersects...")
        try:
            start_time = time.time()
            val = engine.intersects(a, b)
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
        """Check if A is within B (boolean only, no map reload)."""
        a = result_a.get()
        b = result_b.get()

        if not a or not b:
            add_notification("Load queries A and B first", "warning")
            return

        log("Checking within...")
        try:
            start_time = time.time()
            val = engine.within(a, b)
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
        """Check if A contains B (boolean only, no map reload)."""
        a = result_a.get()
        b = result_b.get()

        if not a or not b:
            add_notification("Load queries A and B first", "warning")
            return

        log("Checking contains...")
        try:
            start_time = time.time()
            val = engine.contains(a, b)
            elapsed = time.time() - start_time
            operation_elapsed_time.set(elapsed)
            operation_result.set({"type": "bool", "value": val, "label": "A contains B"})
            log(f"Contains: {val}", "success")
        except Exception as e:
            log(f"Contains check failed: {e}", "error")
            add_notification(f"Contains check failed: {e}", "error")

    @render.ui
    def stats():
        a = result_a.get()
        b = result_b.get()
        op_res = operation_result.get()
        op_time = operation_elapsed_time.get()
        loaded = queries_loaded.get()

        if not loaded:
            return ui.tags.pre(
                "Click 'Load Queries' to start.",
                style="margin:0; padding:8px; background:#f5f5f5; border-radius:4px; font-size:12px; white-space:pre-wrap;"
            )

        lines = []

        if a is not None:
            count_a = a.aggregate("count(*)").fetchone()[0]
            res_a = a.project("h3_resolution").distinct().order("h3_resolution").fetchall()
            lines.append(f"A: {count_a} feat, res {[r[0] for r in res_a]}")

        if b is not None:
            count_b = b.aggregate("count(*)").fetchone()[0]
            res_b = b.project("h3_resolution").distinct().order("h3_resolution").fetchall()
            lines.append(f"B: {count_b} feat, res {[r[0] for r in res_b]}")

        if op_res is not None:
            if op_res["type"] == "bool":
                lines.append(f"{op_res['label']}: {op_res['value']}")
            elif op_res["type"] == "cells":
                lines.append(f"{op_res['label']}: {op_res['area']:.2f} km2")

        if op_time is not None:
            lines.append(f"Time: {op_time:.3f}s")

        # Determine background color based on result type
        if op_res and op_res["type"] == "bool":
            # Green for True, Red for False
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
        # Prefer result session if a spatial operation was run
        session_id = result_session_id.get() or display_session_id.get()

        # Determine which layers to show based on checkboxes
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
            # Show empty map with instructions
            html = """
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="utf-8">
                <script src="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"></script>
                <link href="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css" rel="stylesheet">
                <style>
                    body { margin: 0; padding: 0; }
                    #map { position: absolute; top: 0; bottom: 0; width: 100%; }
                    .info-box {
                        position: absolute;
                        top: 50%;
                        left: 50%;
                        transform: translate(-50%, -50%);
                        background: white;
                        padding: 20px 30px;
                        border-radius: 8px;
                        box-shadow: 0 2px 10px rgba(0,0,0,0.2);
                        font-family: sans-serif;
                        text-align: center;
                    }
                </style>
            </head>
            <body>
                <div id="map"></div>
                <div class="info-box">
                    <h3>H3 Spatial Sandbox</h3>
                    <p>Enter a query and click "Load Queries" to visualize H3 hexagons.</p>
                </div>
                <script>
                    const map = new maplibregl.Map({
                        container: 'map',
                        style: {
                            version: 8,
                            sources: {
                                'carto-light': {
                                    type: 'raster',
                                    tiles: ['https://basemaps.cartocdn.com/light_all/{z}/{x}/{y}@2x.png'],
                                    tileSize: 256
                                }
                            },
                            layers: [{
                                id: 'carto-light-layer',
                                type: 'raster',
                                source: 'carto-light'
                            }]
                        },
                        center: [8.2, 46.8],
                        zoom: 7
                    });
                    map.addControl(new maplibregl.NavigationControl(), 'top-right');
                </script>
            </body>
            </html>
            """

        # Encode HTML as base64 to avoid quote escaping issues
        html_b64 = base64.b64encode(html.encode()).decode()
        return ui.HTML(f'''
            <iframe
                src="data:text/html;base64,{html_b64}"
                style="width:100%;height:calc(100vh - 20px);border:none;">
            </iframe>
        ''')


app = App(app_ui, server)
