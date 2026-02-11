"""
H3 DGGS conversion and spatial operations module.
"""

from .converter import (
    convert_geometry_to_h3,
    convert_geodataframe_to_h3,
    ContainmentMode,
    H3_AVG_HEXAGON_AREA_M2,
)


__all__ = [
    "convert_geometry_to_h3",
    "convert_geodataframe_to_h3",
    "ContainmentMode",
    "H3_AVG_HEXAGON_AREA_M2",
]
