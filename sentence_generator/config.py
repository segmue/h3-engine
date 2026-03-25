"""
Konfiguration fuer den CandidateSentenceGenerator.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class SentenceGeneratorConfig:
    """Konfiguration fuer die Satzgenerierung.

    Attributes:
        assoc_threshold: Minimaler B1-Wert fuer relevante Kategorien (default 0.05)
        max_instances: Maximale Anzahl Instanzen im Satz gesamt (default 8)
        max_instances_per_category: Maximale Instanzen pro Kategorie (default 3)
        max_categories: Maximale Anzahl Kategorien zu beruecksichtigen (default 6)
        db_path: Pfad zur DuckDB Datenbank (optional, wird von H3Engine uebernommen)
        matrix_path: Pfad zur B1 Matrix CSV (optional, default: data/association_results/b1_matrix.csv)
        category_separator: Trennzeichen zwischen Kategorien im Satz
        instance_separator: Trennzeichen zwischen Instanzen einer Kategorie
    """

    # Association thresholds
    assoc_threshold: float = 0.0

    # Slot allocation
    max_instances: int = 10
    max_instances_per_category: int = 5
    max_categories: int = 6

    # Paths
    db_path: Optional[Path] = None
    matrix_path: Optional[Path] = None

    # Template settings
    category_separator: str = "; "
    instance_separator: str = ", "

    def get_matrix_path(self) -> Path:
        """Gibt den Pfad zur B1 Matrix zurueck (mit Default-Fallback)."""
        if self.matrix_path:
            return self.matrix_path
        # Default: relativ zum Projekt-Root
        project_root = Path(__file__).parent.parent
        return project_root / "data" / "association_results" / "b1_matrix.csv"
