"""
H3 DGGS implementation of the spatial predicate engine.

This module provides a concrete implementation of SpatialPredicateEngine
using H3 hierarchical spatial indexing.
"""

from typing import Set
from spatial_engine import SpatialPredicateEngine
from . import predicates


class H3PredicateEngine(SpatialPredicateEngine):
    """
    Spatial predicate engine using H3 DGGS.

    This engine operates on sets of H3 cell IDs and uses hierarchical
    normalization for efficient cross-resolution queries.

    Expected input: Sets of H3 cell IDs (strings)
    """

    def intersects(self, a: Set[str], b: Set[str]) -> bool:
        """
        Test if two H3 cell sets intersect.

        Uses hierarchical normalization to handle different resolutions.

        Args:
            a: Set of H3 cell IDs
            b: Set of H3 cell IDs

        Returns:
            True if cell sets intersect, False otherwise
        """
        return predicates.intersects(a, b)

    def within(self, a: Set[str], b: Set[str]) -> bool:
        """
        Test if H3 cell set a is completely within cell set b.

        Uses hierarchical normalization to handle different resolutions.

        Args:
            a: Set of H3 cell IDs
            b: Set of H3 cell IDs

        Returns:
            True if a is within b, False otherwise
        """
        return predicates.within(a, b)

    def contains(self, a: Set[str], b: Set[str]) -> bool:
        """
        Test if H3 cell set a completely contains cell set b.

        Uses hierarchical normalization to handle different resolutions.

        Args:
            a: Set of H3 cell IDs
            b: Set of H3 cell IDs

        Returns:
            True if a contains b, False otherwise
        """
        return predicates.contains(a, b)

    def touches(self, a: Set[str], b: Set[str]) -> bool:
        """
        Test if two H3 cell sets touch (adjacent but not overlapping).

        Uses hierarchical normalization to handle different resolutions.

        Args:
            a: Set of H3 cell IDs
            b: Set of H3 cell IDs

        Returns:
            True if cell sets touch, False otherwise
        """
        return predicates.touches(a, b)

    def get_name(self) -> str:
        """Return the name of this engine."""
        return "H3 DGGS Engine"
