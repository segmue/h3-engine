"""
Dashboard Configuration.

Central configuration for paths, URLs, and shared utilities.
"""

import os
import time
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "swissNAMES3D_combined_h3.duckdb"
ASSOCIATION_RESULTS_PATH = PROJECT_ROOT / "data" / "association_results"

# External services
TILE_SERVER_URL = os.environ.get("TILE_SERVER_URL", "http://localhost:8001")


def log(message: str, level: str = "info") -> None:
    """Log a message with timestamp."""
    timestamp = time.strftime("%H:%M:%S")
    prefix = {
        "info": "[i]",
        "success": "[+]",
        "warning": "[!]",
        "error": "[x]"
    }.get(level, "[i]")
    print(f"{prefix} {timestamp} {message}")
