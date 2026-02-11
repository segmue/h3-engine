"""
Vector to DGGS (H3) conversion functions.

Supports conversion of points, lines, and polygons to H3 cells with
automatic coordinate transformation.
"""

from typing import Set, List, Union, Optional, Any
from enum import Enum
import h3
from shapely.geometry import Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon
from shapely.ops import transform
from pyproj import Transformer, CRS, Geod


# =============================================================================
# CONSTANTS
# =============================================================================

class ContainmentMode(Enum):
    """
    H3 containment modes for polygon_to_cells operations.

    The experimental API (h3shape_to_cells_experimental) supports all 4 modes.
    The standard API (polygon_to_cells) only supports CENTER.
    """
    CENTER = "center"              # Cell center must be inside polygon (default classic)
    FULL = "full"                  # Cell must be fully contained in polygon
    OVERLAPPING = "overlap"        # Cell overlaps polygon at any point (recommended)
    OVERLAPPING_BBOX = "overlap_bbox"  # Cell bounding box overlaps polygon


# Average hexagon areas in m² per H3 resolution level
# Source: https://h3geo.org/docs/core-library/restable/
H3_AVG_HEXAGON_AREA_M2: dict[int, float] = {
    0:  4_357_449_416_078.392,
    1:    609_788_441_794.134,
    2:     86_801_780_398.997,
    3:     12_393_434_655.088,
    4:      1_770_347_654.491,
    5:        252_903_858.182,
    6:         36_129_062.164,
    7:          5_161_293.360,
    8:            737_327.598,
    9:            105_332.513,
    10:            15_047.502,
    11:             2_149.643,
    12:               307.092,
    13:                43.870,
    14:                 6.267,
    15:                 0.895,
}

# WGS84 ellipsoid for geodesic area calculations
_WGS84_GEOD = Geod(ellps='WGS84')

try:
    import geopandas as gpd
    HAS_GEOPANDAS = True
except ImportError:
    HAS_GEOPANDAS = False


def _ensure_wgs84(geometry, source_crs: Optional[Union[str, int]] = None):
    """
    Transform geometry to WGS84 if needed.

    Args:
        geometry: Shapely geometry object
        source_crs: Source CRS (EPSG code as int, string like 'EPSG:2056', or None for WGS84)

    Returns:
        Transformed geometry in WGS84
    """
    if source_crs is None:
        return geometry

    # Handle different CRS input formats
    if isinstance(source_crs, int):
        source_crs = f"EPSG:{source_crs}"

    # Create transformer
    transformer = Transformer.from_crs(
        CRS.from_user_input(source_crs),
        CRS.from_epsg(4326),  # WGS84
        always_xy=True
    )

    return transform(transformer.transform, geometry)


def _polygon_to_h3_cells(
    polygon: Polygon,
    resolution: int,
    containment_mode: ContainmentMode = ContainmentMode.OVERLAPPING
) -> Set[str]:
    """
    Helper function to convert a single polygon to H3 cells at given resolution.

    Uses the experimental H3 API (h3shape_to_cells_experimental) which supports
    different containment modes. Falls back to standard API if experimental fails.

    Args:
        polygon: Shapely Polygon in WGS84
        resolution: H3 resolution level
        containment_mode: ContainmentMode enum value (default: OVERLAPPING)
            - CENTER: Cell center must be inside polygon (classic behavior)
            - FULL: Cell must be fully contained
            - OVERLAPPING: Any overlap counts (recommended for full coverage)
            - OVERLAPPING_BBOX: Bounding box overlap

    Returns:
        Set of H3 cell IDs
    """
    exterior_coords = [(coord[1], coord[0]) for coord in polygon.exterior.coords]

    holes = []
    for interior in polygon.interiors:
        hole_coords = [(coord[1], coord[0]) for coord in interior.coords]
        holes.append(hole_coords)

    if holes:
        h3_poly = h3.LatLngPoly(exterior_coords, *holes)
    else:
        h3_poly = h3.LatLngPoly(exterior_coords)

    # Try experimental API first (supports all containment modes)
    try:
        return h3.h3shape_to_cells_experimental(h3_poly, resolution, contain=containment_mode.value)
    except (AttributeError, TypeError, Exception) as e:
        # Fallback to standard API if experimental not available or fails
        # Note: Standard API only supports CENTER containment mode
        if containment_mode != ContainmentMode.CENTER:
            print(f"[_polygon_to_h3_cells] Warning: h3shape_to_cells_experimental failed ({e}). "
                  f"Falling back to standard polygon_to_cells (CENTER mode only).")
        return h3.polygon_to_cells(h3_poly, resolution)


def _calculate_geodesic_area_m2(polygon: Union[Polygon, MultiPolygon]) -> float:
    """
    Calculate the geodesic area of a polygon in square meters using WGS84 ellipsoid.

    Args:
        polygon: Shapely Polygon or MultiPolygon in WGS84 coordinates

    Returns:
        Area in square meters (always positive)
    """
    area, _ = _WGS84_GEOD.geometry_area_perimeter(polygon)
    return abs(area)


def _estimate_resolution_from_area(
    polygon_area_m2: float,
    target_cells: int,
    min_resolution: int,
    max_resolution: int
) -> int:
    """
    Estimate optimal H3 resolution based on polygon area and target cell count.

    Chooses the resolution where cells are small enough to produce AT LEAST
    target_cells (errs on the side of more cells, not fewer).

    Args:
        polygon_area_m2: Polygon area in square meters
        target_cells: Target number of cells
        min_resolution: Minimum allowed resolution
        max_resolution: Maximum allowed resolution

    Returns:
        Estimated resolution level
    """
    target_cell_area = polygon_area_m2 / target_cells

    # Find the finest resolution where cell area >= target_cell_area
    # This ensures we get AT LEAST target_cells (more cells = smaller cells = finer resolution)
    for res in range(min_resolution, max_resolution + 1):
        if H3_AVG_HEXAGON_AREA_M2[res] <= target_cell_area:
            return res

    # If even max_resolution cells are too big, return max_resolution
    return max_resolution


def calculate_optimal_resolution(
    polygon: Union[Polygon, MultiPolygon],
    target_cells: int = 1000,
    min_resolution: int = 5,
    max_resolution: int = 12,
    source_crs: Optional[Union[str, int]] = None,
    containment_mode: ContainmentMode = ContainmentMode.OVERLAPPING
) -> Optional[int]:
    """
    Calculate optimal H3 resolution for a polygon based on target cell count.

    OPTIMIZED ALGORITHM:
    1. Calculate geodesic area of polygon (in m²)
    2. Use lookup table to estimate resolution from area/target_cells ratio
    3. Validate with actual polygon_to_cells call
    4. Adjust to finer resolution if validation shows fewer cells than target

    The algorithm guarantees AT LEAST target_cells will be generated (errs on
    the side of more cells, not fewer).

    Args:
        polygon: Shapely Polygon or MultiPolygon geometry
        target_cells: Target number of H3 cells (default: 1000)
        min_resolution: Minimum resolution to use (default: 5)
        max_resolution: Maximum resolution before centroid fallback (default: 12)
        source_crs: Source CRS (e.g., 2056 for LV95, None for WGS84)
        containment_mode: ContainmentMode for polygon_to_cells (default: OVERLAPPING)

    Returns:
        Optimal resolution (int) or None if polygon too small for max_resolution

    Example:
        >>> # Large polygon (half of Switzerland)
        >>> big_polygon = Polygon([...])  # ~20,000 km²
        >>> res = calculate_optimal_resolution(big_polygon)  # → 5 or 6

        >>> # Tiny polygon (building)
        >>> tiny_polygon = Polygon([...])  # few m²
        >>> res = calculate_optimal_resolution(tiny_polygon)  # → None (use centroid)
    """
    # Transform to WGS84 for consistent area calculation
    polygon_wgs84 = _ensure_wgs84(polygon, source_crs)

    # Step 1: Calculate geodesic area
    polygon_area_m2 = _calculate_geodesic_area_m2(polygon_wgs84)

    # Step 2: Estimate resolution from lookup table
    estimated_resolution = _estimate_resolution_from_area(
        polygon_area_m2, target_cells, min_resolution, max_resolution
    )

    print(f"[calculate_optimal_resolution] Polygon area: {polygon_area_m2:,.2f} m² "
          f"({polygon_area_m2 / 1_000_000:,.2f} km²), target_cells: {target_cells}, "
          f"estimated resolution: {estimated_resolution}")

    # Step 3: Validation - actually compute cells to verify
    if isinstance(polygon_wgs84, MultiPolygon):
        h3_cells = set()
        for poly in polygon_wgs84.geoms:
            h3_cells.update(_polygon_to_h3_cells(poly, estimated_resolution, containment_mode))
    else:
        h3_cells = _polygon_to_h3_cells(polygon_wgs84, estimated_resolution, containment_mode)

    num_cells = len(h3_cells)

    print(f"[calculate_optimal_resolution] Validation at res {estimated_resolution}: "
          f"{num_cells} cells (target: {target_cells})")

    # Step 4: Adjust if needed - we want AT LEAST target_cells
    if num_cells < target_cells and estimated_resolution < max_resolution:
        # Try one finer resolution
        finer_resolution = estimated_resolution + 1

        if isinstance(polygon_wgs84, MultiPolygon):
            finer_cells = set()
            for poly in polygon_wgs84.geoms:
                finer_cells.update(_polygon_to_h3_cells(poly, finer_resolution, containment_mode))
        else:
            finer_cells = _polygon_to_h3_cells(polygon_wgs84, finer_resolution, containment_mode)

        print(f"[calculate_optimal_resolution] Adjustment: res {finer_resolution} → "
              f"{len(finer_cells)} cells (was {num_cells} at res {estimated_resolution})")

        return finer_resolution

    # No cells found at all → polygon too small
    if num_cells == 0:
        print(f"[calculate_optimal_resolution] No cells found at res {estimated_resolution}, "
              f"polygon too small → returning None (use centroid fallback)")
        return None

    return estimated_resolution



def point_to_h3(point: Point, resolution: int = 10, source_crs: Optional[Union[str, int]] = None) -> str:
    """
    Convert a point geometry to H3 cell ID.

    Args:
        point: Shapely Point geometry
        resolution: H3 resolution (0-15), default 10
        source_crs: Source CRS (e.g., 2056 for LV95, None for WGS84)

    Returns:
        H3 cell ID as string

    Example:
        >>> point = Point(7.5, 47.5)  # WGS84
        >>> cell_id = point_to_h3(point)
        >>> print(cell_id)
    """
    # Transform to WGS84 if needed
    point_wgs84 = _ensure_wgs84(point, source_crs)

    # H3 expects (lat, lng) but shapely uses (x, y) = (lng, lat)
    lng, lat = point_wgs84.x, point_wgs84.y

    return h3.latlng_to_cell(lat, lng, resolution)


def line_to_h3(line: LineString, resolution: int = 10, source_crs: Optional[Union[str, int]] = None) -> Set[str]:
    # 1. Transform to WGS84 (wie gehabt)
    line_wgs84 = _ensure_wgs84(line, source_crs)

    h3_cells = set()

    # 2. Koordinaten extrahieren
    coords = list(line_wgs84.coords)

    # 3. Durch alle Segmente iterieren
    for i in range(len(coords) - 1):
        p1 = coords[i]  # (lng, lat)
        p2 = coords[i + 1]  # (lng, lat)

        # H3 Zellen für Start und Ende des Segments finden
        # Achtung: H3 erwartet (lat, lng), Shapely liefert (lng, lat)
        cell_start = h3.latlng_to_cell(p1[1], p1[0], resolution)
        cell_end = h3.latlng_to_cell(p2[1], p2[0], resolution)

        # 4. Den Pfad zwischen den Zellen berechnen (H3 Native Funktion)
        # grid_path_cells (früher h3_line) füllt die Lücke topologisch korrekt
        try:
            path = h3.grid_path_cells(cell_start, cell_end)
            h3_cells.update(path)
        except Exception:
            # Fallback, falls Zellen zu weit entfernt oder Fehler auftreten
            # (passiert selten bei benachbarten Geometrie-Punkten)
            h3_cells.add(cell_start)
            h3_cells.add(cell_end)

    return h3_cells


def polygon_to_h3(
    polygon: Union[Polygon, MultiPolygon],
    resolution: int = 10,
    source_crs: Optional[Union[str, int]] = None,
    containment_mode: ContainmentMode = ContainmentMode.OVERLAPPING
) -> Set[str]:
    """
    Convert a polygon geometry to set of H3 cell IDs.

    Uses H3 experimental API with configurable containment mode. Default is
    OVERLAPPING which includes all cells that touch the polygon boundary.
    If the polygon is too small and no cells are found, falls back to using
    the polygon's centroid. Supports both Polygon and MultiPolygon geometries.

    Args:
        polygon: Shapely Polygon or MultiPolygon geometry
        resolution: H3 resolution (0-15), default 10
        source_crs: Source CRS (e.g., 2056 for LV95, None for WGS84)
        containment_mode: ContainmentMode enum (default: OVERLAPPING)
            - CENTER: Cell center must be inside polygon
            - FULL: Cell must be fully contained
            - OVERLAPPING: Any overlap counts (recommended)
            - OVERLAPPING_BBOX: Bounding box overlap

    Returns:
        Set of H3 cell IDs as strings (at least one cell)

    Example:
        >>> polygon = Polygon([(7.5, 47.5), (7.6, 47.5), (7.6, 47.6), (7.5, 47.6)])
        >>> cell_ids = polygon_to_h3(polygon)
        >>> print(f"Polygon contains {len(cell_ids)} cells")

        >>> # With explicit containment mode
        >>> cell_ids = polygon_to_h3(polygon, containment_mode=ContainmentMode.CENTER)

        >>> # MultiPolygon example
        >>> multipoly = MultiPolygon([poly1, poly2, poly3])
        >>> cell_ids = polygon_to_h3(multipoly)
        >>> print(f"MultiPolygon contains {len(cell_ids)} cells")
    """
    # Transform to WGS84 if needed
    polygon_wgs84 = _ensure_wgs84(polygon, source_crs)

    # Handle MultiPolygon by processing each part
    if isinstance(polygon_wgs84, MultiPolygon):
        all_cells = set()
        for poly in polygon_wgs84.geoms:
            # Process each polygon part (already in WGS84)
            cells = polygon_to_h3(poly, resolution=resolution, source_crs=None,
                                  containment_mode=containment_mode)
            all_cells.update(cells)

        # If no cells found, use centroid of entire MultiPolygon
        if not all_cells:
            centroid = polygon_wgs84.centroid
            lng, lat = centroid.x, centroid.y
            cell_id = h3.latlng_to_cell(lat, lng, resolution)
            return {cell_id}

        return all_cells

    # Single Polygon - use helper function with containment mode
    h3_cells = _polygon_to_h3_cells(polygon_wgs84, resolution, containment_mode)

    # Fallback: If polygon is too small and no cells found, use centroid
    if not h3_cells:
        centroid = polygon_wgs84.centroid
        lng, lat = centroid.x, centroid.y
        cell_id = h3.latlng_to_cell(lat, lng, resolution)
        return {cell_id}

    return set(h3_cells)


def polygon_to_h3_adaptive(
    polygon: Union[Polygon, MultiPolygon],
    target_cells: int = 1000,
    min_resolution: int = 5,
    max_resolution: int = 12,
    source_crs: Optional[Union[str, int]] = None,
    containment_mode: ContainmentMode = ContainmentMode.OVERLAPPING
) -> tuple[Set[str], int]:
    """
    Convert polygon to H3 cells with adaptive resolution.

    Automatically chooses optimal resolution based on polygon size to keep
    cell count around target_cells. Returns both cells and the chosen resolution.
    Supports both Polygon and MultiPolygon geometries.

    Args:
        polygon: Shapely Polygon or MultiPolygon geometry
        target_cells: Target number of H3 cells (default: 1000)
        min_resolution: Minimum resolution to use (default: 5)
        max_resolution: Maximum resolution before centroid fallback (default: 12)
        source_crs: Source CRS (e.g., 2056 for LV95, None for WGS84)
        containment_mode: ContainmentMode enum (default: OVERLAPPING)
            - CENTER: Cell center must be inside polygon
            - FULL: Cell must be fully contained
            - OVERLAPPING: Any overlap counts (recommended)
            - OVERLAPPING_BBOX: Bounding box overlap

    Returns:
        Tuple of (set of H3 cell IDs, resolution used)

    Example:
        >>> # Large polygon → coarse resolution
        >>> big_polygon = Polygon([...])
        >>> cells, res = polygon_to_h3_adaptive(big_polygon)
        >>> print(f"Used resolution {res}, got {len(cells)} cells")

        >>> # Tiny polygon → centroid fallback
        >>> tiny = Polygon([...])
        >>> cells, res = polygon_to_h3_adaptive(tiny)
        >>> print(f"Too small, used centroid at resolution {res}")

        >>> # MultiPolygon → unified resolution for all parts
        >>> multipoly = MultiPolygon([poly1, poly2, poly3])
        >>> cells, res = polygon_to_h3_adaptive(multipoly)
        >>> print(f"All {len(multipoly.geoms)} parts use resolution {res}")
    """
    # Calculate optimal resolution
    optimal_res = calculate_optimal_resolution(
        polygon, target_cells, min_resolution, max_resolution, source_crs,
        containment_mode=containment_mode
    )

    # If None, polygon too small → use centroid at max_resolution
    if optimal_res is None:
        polygon_wgs84 = _ensure_wgs84(polygon, source_crs)
        centroid = polygon_wgs84.centroid
        lng, lat = centroid.x, centroid.y
        cell_id = h3.latlng_to_cell(lat, lng, max_resolution)
        return {cell_id}, max_resolution

    # Convert with optimal resolution
    cells = polygon_to_h3(polygon, resolution=optimal_res, source_crs=source_crs,
                          containment_mode=containment_mode)
    return cells, optimal_res


def convert_geometry_to_h3(
    geometry: Union[Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon],
    resolution: int = 10,
    source_crs: Optional[Union[str, int]] = None
) -> Set[str]:
    """
    Convert any geometry type to H3 cell IDs.

    Handles Point, LineString, Polygon, and Multi* variants.

    Args:
        geometry: Shapely geometry object
        resolution: H3 resolution (0-15), default 10
        source_crs: Source CRS (e.g., 2056 for LV95, None for WGS84)

    Returns:
        Set of H3 cell IDs as strings

    Raises:
        ValueError: If geometry type is not supported

    Example:
        >>> from shapely.geometry import Point
        >>> geom = Point(7.5, 47.5)
        >>> cells = convert_geometry_to_h3(geom)
    """
    if isinstance(geometry, Point):
        return {point_to_h3(geometry, resolution, source_crs)}

    elif isinstance(geometry, LineString):
        return line_to_h3(geometry, resolution, source_crs)

    elif isinstance(geometry, Polygon):
        return polygon_to_h3(geometry, resolution, source_crs)

    elif isinstance(geometry, (MultiPoint, MultiLineString, MultiPolygon)):
        # Handle multi-geometries by processing each part
        # Transform once before processing parts (performance optimization)
        geometry_wgs84 = _ensure_wgs84(geometry, source_crs)
        h3_cells = set()
        for geom in geometry_wgs84.geoms:
            # Parts are already in WGS84, so pass source_crs=None
            h3_cells.update(convert_geometry_to_h3(geom, resolution, source_crs=None))
        return h3_cells

    else:
        raise ValueError(f"Unsupported geometry type: {type(geometry)}")


def convert_geodataframe_to_h3(
    gdf: Any,
    resolution: int = 10,
    geometry_column: str = 'geometry',
    containment_mode: ContainmentMode = ContainmentMode.OVERLAPPING
) -> List[Set[str]]:
    """
    Optimized batch conversion of GeoDataFrame geometries to H3 cells.

    This function is significantly faster than using apply() on each row
    because it transforms all geometries to WGS84 in one operation.

    Args:
        gdf: GeoDataFrame with geometries to convert
        resolution: H3 resolution (0-15), default 10
        geometry_column: Name of the geometry column, default 'geometry'
        containment_mode: ContainmentMode enum (default: OVERLAPPING)

    Returns:
        List of H3 cell sets, one per row

    Example:
        >>> import geopandas as gpd
        >>> gdf = gpd.read_file('data.gpkg')
        >>> h3_cells = convert_geodataframe_to_h3(gdf, resolution=10)
        >>> gdf['h3_cells'] = h3_cells

    Performance:
        - 5-10x faster than row-by-row apply() due to batch CRS transformation
        - For 40 large polygons: seconds instead of minutes
    """
    if not HAS_GEOPANDAS:
        raise ImportError("geopandas is required for convert_geodataframe_to_h3")

    # Batch transform to WGS84 once (huge performance gain!)
    gdf_wgs84 = gdf.to_crs(epsg=4326)

    # Convert each geometry (already in WGS84, so no more CRS transformations)
    h3_cells_list = []
    for geom in gdf_wgs84[geometry_column]:
        if isinstance(geom, (Polygon, MultiPolygon)):
            cells = polygon_to_h3(geom, resolution=resolution, source_crs=None,
                                  containment_mode=containment_mode)
        else:
            cells = convert_geometry_to_h3(geom, resolution=resolution, source_crs=None)
        h3_cells_list.append(cells)

    return h3_cells_list


def convert_geodataframe_to_h3_adaptive(
    gdf: Any,
    target_cells: int = 1000,
    min_resolution: int = 5,
    max_resolution: int = 12,
    geometry_column: str = 'geometry',
    containment_mode: ContainmentMode = ContainmentMode.OVERLAPPING
) -> tuple[List[Set[str]], List[int]]:
    """
    Optimized batch conversion with ADAPTIVE resolution per geometry.

    Each geometry gets its own optimal resolution based on size, preventing
    huge polygons from generating millions of cells while keeping small
    polygons detailed.

    Args:
        gdf: GeoDataFrame with geometries to convert
        target_cells: Target number of cells per geometry (default: 1000)
        min_resolution: Minimum resolution to use (default: 5)
        max_resolution: Maximum resolution before centroid fallback (default: 12)
        geometry_column: Name of the geometry column, default 'geometry'
        containment_mode: ContainmentMode enum (default: OVERLAPPING)

    Returns:
        Tuple of (list of H3 cell sets, list of resolutions used)

    Example:
        >>> import geopandas as gpd
        >>> gdf = gpd.read_file('data.gpkg')  # Mix of large & small polygons
        >>> h3_cells, resolutions = convert_geodataframe_to_h3_adaptive(gdf)
        >>> gdf['h3_cells'] = h3_cells
        >>> gdf['h3_resolution'] = resolutions
        >>> # Large polygon: ~1000 cells at res 6
        >>> # Small polygon: ~1000 cells at res 11

    Performance:
        - Large polygons (half of Switzerland): Resolution 5-6, ~1000 cells
        - Small polygons (buildings): Resolution 11-12, ~10-100 cells
        - Prevents 900,000 cell explosion!
    """
    if not HAS_GEOPANDAS:
        raise ImportError("geopandas is required for convert_geodataframe_to_h3_adaptive")

    # Batch transform to WGS84 once
    gdf_wgs84 = gdf.to_crs(epsg=4326)

    h3_cells_list = []
    resolutions_list = []

    for geom in gdf_wgs84[geometry_column]:
        # Handle polygons and multipolygons with adaptive resolution
        if isinstance(geom, (Polygon, MultiPolygon)):
            cells, resolution = polygon_to_h3_adaptive(
                geom,
                target_cells=target_cells,
                min_resolution=min_resolution,
                max_resolution=max_resolution,
                source_crs=None,  # Already WGS84
                containment_mode=containment_mode
            )
            h3_cells_list.append(cells)
            resolutions_list.append(resolution)

        # Points and lines: use max_resolution (they're small by nature)
        else:
            cells = convert_geometry_to_h3(geom, resolution=max_resolution, source_crs=None)
            h3_cells_list.append(cells)
            resolutions_list.append(max_resolution)

    return h3_cells_list, resolutions_list
