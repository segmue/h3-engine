"""
MapLibre GL JS HTML Generation.

Generates HTML with MapLibre GL JS for vector tile visualization.
"""

from dashboard.config import TILE_SERVER_URL


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
                    const features = map.queryRenderedFeatures(e.point, {{
                        layers: interactiveLayers.filter(l => map.getLayer(l))
                    }});

                    if (features.length === 0) {{
                        map.getCanvas().style.cursor = '';
                        popup.remove();
                        return;
                    }}

                    map.getCanvas().style.cursor = 'pointer';

                    const byLayer = {{}};
                    features.forEach(f => {{
                        if (!byLayer[f.layer.id]) byLayer[f.layer.id] = [];
                        byLayer[f.layer.id].push(f);
                    }});

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

            map.on('zoomend', updateZoomInfo);
            map.on('load', updateZoomInfo);
        </script>
    </body>
    </html>
    """


def create_empty_map_html() -> str:
    """Generate empty map HTML with info box."""
    return """
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
            <h3>H3 Engine Sandbox</h3>
            <p>Enter a query and click "Run" to visualize H3 hexagons.</p>
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
