"""
H3 DGGS conversion and spatial operations module.
"""

from .converter import (
    point_to_h3,
    line_to_h3,
    polygon_to_h3,
    polygon_to_h3_adaptive,
    convert_geometry_to_h3,
    convert_geodataframe_to_h3,
    convert_geodataframe_to_h3_adaptive,
    calculate_optimal_resolution,
    ContainmentMode,
    H3_AVG_HEXAGON_AREA_M2,
)

from .predicates import (
    intersects,
    within,
    contains,
    touches,
)

from spatial_engine import SpatialPredicateEngine
from geometric_engine import GeometricPredicateEngine
from .h3_engine import H3PredicateEngine

__all__ = [
    "point_to_h3",
    "line_to_h3",
    "polygon_to_h3",
    "polygon_to_h3_adaptive",
    "convert_geometry_to_h3",
    "convert_geodataframe_to_h3",
    "convert_geodataframe_to_h3_adaptive",
    "calculate_optimal_resolution",
    "ContainmentMode",
    "H3_AVG_HEXAGON_AREA_M2",
    "intersects",
    "within",
    "contains",
    "touches",
    "SpatialPredicateEngine",
    "H3PredicateEngine",
    "GeometricPredicateEngine",
]
