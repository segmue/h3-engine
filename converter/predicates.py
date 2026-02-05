"""
Spatial predicates for DGGS (H3) geometries.

Provides functions to test spatial relationships between sets of H3 cells,
analogous to traditional vector spatial predicates.

HIERARCHICAL SUPPORT:
All predicates support mixed resolutions efficiently using H3's hierarchical
structure. When cells have different resolutions, they are normalized to
the coarser resolution for comparison
"""

from typing import Set, List, Union
import h3


def _normalize_to_coarser_resolution(cells_a: Set[str], cells_b: Set[str]) -> tuple[Set[str], Set[str], int]:
    """
    Normalize two cell sets to their coarser resolution using H3 hierarchy.

    Returns:
        Tuple of (normalized_a, normalized_b, target_resolution)
    """
    # Get resolutions (assuming all cells in a set have same resolution)
    if not cells_a or not cells_b:
        return cells_a, cells_b, None

    try:
        res_a = h3.get_resolution(next(iter(cells_a)))
        res_b = h3.get_resolution(next(iter(cells_b)))
    except (h3.H3CellInvalidError, Exception):
        # If cells are invalid (e.g., test data), skip normalization
        return cells_a, cells_b, None

    # Same resolution → no normalization needed
    if res_a == res_b:
        return cells_a, cells_b, res_a

    # Normalize to coarser (lower number) resolution
    target_res = min(res_a, res_b)

    # Normalize A if needed
    if res_a > target_res:
        cells_a = {h3.cell_to_parent(cell, target_res) for cell in cells_a}

    # Normalize B if needed
    if res_b > target_res:
        cells_b = {h3.cell_to_parent(cell, target_res) for cell in cells_b}

    return cells_a, cells_b, target_res


def intersects(cells_a: Union[Set[str], List[str]], cells_b: Union[Set[str], List[str]]) -> bool:
    """
    Test if two H3 cell sets intersect (hierarchical-aware).

    Supports mixed resolutions using H3's hierarchical structure.

    Args:
        cells_a: Set or list of H3 cell IDs (can be any resolution)
        cells_b: Set or list of H3 cell IDs (can be any resolution)

    Returns:
        True if the sets share at least one cell (or hierarchical parent), False otherwise

    Example:
        >>> # Same resolution
        >>> cells_a = {'8a1234567890abc', '8a1234567890abd'}
        >>> cells_b = {'8a1234567890abd', '8a1234567890abe'}
        >>> intersects(cells_a, cells_b)
        True

        >>> # Mixed resolutions (A is coarse, B is fine)
        >>> cells_a = {'851234567ffffff'}  # Resolution 5
        >>> cells_b = {'8a1234567890abc'}  # Resolution 10, child of A
        >>> intersects(cells_a, cells_b)
        True  # Hierarchical match!
    """
    set_a = set(cells_a) if not isinstance(cells_a, set) else cells_a
    set_b = set(cells_b) if not isinstance(cells_b, set) else cells_b

    # Normalize to coarser resolution
    norm_a, norm_b, _ = _normalize_to_coarser_resolution(set_a, set_b)

    return len(norm_a & norm_b) > 0


def within(cells_a: Union[Set[str], List[str]], cells_b: Union[Set[str], List[str]]) -> bool:
    """
    Test if all cells in A are contained within B (hierarchical-aware).

    Args:
        cells_a: Set or list of H3 cell IDs (can be any resolution)
        cells_b: Set or list of H3 cell IDs (can be any resolution)

    Returns:
        True if all cells in A are contained within B, False otherwise

    Example:
        >>> # Same resolution
        >>> cells_a = {'8a1234567890abc'}
        >>> cells_b = {'8a1234567890abc', '8a1234567890abd'}
        >>> within(cells_a, cells_b)
        True

        >>> # Mixed resolution: fine cells within coarse cell
        >>> cells_a = {'8a1234567890abc', '8a1234567890abd'}  # Res 10
        >>> cells_b = {'851234567ffffff'}  # Res 5, parent of both
        >>> within(cells_a, cells_b)
        True  # All children within parent!
    """
    set_a = set(cells_a) if not isinstance(cells_a, set) else cells_a
    set_b = set(cells_b) if not isinstance(cells_b, set) else cells_b

    # Normalize to coarser resolution
    norm_a, norm_b, _ = _normalize_to_coarser_resolution(set_a, set_b)

    return norm_a.issubset(norm_b)


def contains(cells_a: Union[Set[str], List[str]], cells_b: Union[Set[str], List[str]]) -> bool:
    """
    Test if A contains all cells in B (hierarchical-aware).

    This is the inverse of within: A contains B if B is within A.

    Args:
        cells_a: Set or list of H3 cell IDs (can be any resolution)
        cells_b: Set or list of H3 cell IDs (can be any resolution)

    Returns:
        True if all cells in B are contained within A, False otherwise

    Example:
        >>> # Same resolution
        >>> cells_a = {'8a1234567890abc', '8a1234567890abd'}
        >>> cells_b = {'8a1234567890abc'}
        >>> contains(cells_a, cells_b)
        True

        >>> # Mixed resolution: coarse cell contains fine cells
        >>> cells_a = {'851234567ffffff'}  # Res 5, parent
        >>> cells_b = {'8a1234567890abc', '8a1234567890abd'}  # Res 10, children
        >>> contains(cells_a, cells_b)
        True  # Parent contains all children!
    """
    set_a = set(cells_a) if not isinstance(cells_a, set) else cells_a
    set_b = set(cells_b) if not isinstance(cells_b, set) else cells_b

    # Normalize to coarser resolution
    norm_a, norm_b, _ = _normalize_to_coarser_resolution(set_a, set_b)

    return norm_b.issubset(norm_a)


def touches(cells_a: Union[Set[str], List[str]], cells_b: Union[Set[str], List[str]]) -> bool:
    """
    Test if two H3 cell sets touch (hierarchical-aware).

    Two cell sets touch if they have no cells in common (don't intersect)
    but at least one cell in A is a neighbor of at least one cell in B.

    Args:
        cells_a: Set or list of H3 cell IDs (can be any resolution)
        cells_b: Set or list of H3 cell IDs (can be any resolution)

    Returns:
        True if the sets touch but don't intersect, False otherwise

    Example:
        >>> cells_a = {'8a1234567890abc'}
        >>> cells_b = {'8a1234567890abd'}  # Adjacent cell
        >>> touches(cells_a, cells_b)
        True  # (if they are neighbors)
    """
    set_a = set(cells_a) if not isinstance(cells_a, set) else cells_a
    set_b = set(cells_b) if not isinstance(cells_b, set) else cells_b

    # First check: they must not intersect (uses hierarchical intersects)
    if intersects(set_a, set_b):
        return False

    # Normalize to same resolution for neighbor checks
    norm_a, norm_b, _ = _normalize_to_coarser_resolution(set_a, set_b)

    # Get all neighbors of cells in A (at normalized resolution)
    neighbors_a = set()
    for cell in norm_a:
        try:
            # Get direct neighbors (k=1)
            neighbors = set(h3.grid_disk(cell, k=1))
            neighbors_a.update(neighbors)
        except Exception:
            # Skip invalid cells
            continue

    # Remove cells from A itself (we only want the neighboring cells)
    neighbors_a -= norm_a

    # Check if any neighbor of A is in B
    return len(neighbors_a & norm_b) > 0


def get_neighbors(cells: Union[Set[str], List[str]], k: int = 1) -> Set[str]:
    """
    Get all neighboring cells for a set of H3 cells.

    Args:
        cells: Set or list of H3 cell IDs
        k: Distance (ring number) for neighbors, default 1 for direct neighbors

    Returns:
        Set of neighboring H3 cell IDs (excluding input cells)

    Example:
        >>> cells = {'8a1234567890abc'}
        >>> neighbors = get_neighbors(cells)
        >>> len(neighbors)  # Typically 6 for a single hexagon
    """
    cell_set = set(cells) if not isinstance(cells, set) else cells
    all_neighbors = set()

    for cell in cell_set:
        try:
            neighbors = h3.grid_disk(cell, k=k)
            all_neighbors.update(neighbors)
        except Exception:
            # Skip invalid cells
            continue

    # Remove original cells
    all_neighbors -= cell_set

    return all_neighbors
