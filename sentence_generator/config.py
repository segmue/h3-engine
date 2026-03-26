"""
Konfiguration fuer den CandidateSentenceGenerator.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


@dataclass
class StaticDatasetConfig:
    """Konfiguration fuer ein statisches Kontext-Dataset.

    Attributes:
        name: Dataset-Name in der DuckDB (z.B. 'gemeinden')
        slots: Maximale Anzahl Features im Satz
        label: Anzeige-Label im Satz (z.B. 'Gemeinde')
    """
    name: str
    slots: int
    label: str


@dataclass
class SentenceGeneratorConfig:
    """Konfiguration fuer die Satzgenerierung.

    Attributes:
        assoc_threshold: Minimaler B1-Wert fuer relevante Kategorien
        max_instances: Maximale Anzahl Instanzen im Satz gesamt
        max_instances_per_category: Maximale Instanzen pro Kategorie
        max_categories: Maximale Anzahl Kategorien zu beruecksichtigen
        target_dataset: Dataset-Name des Target-Datensatzes
        static_datasets: Liste von statischen Kontext-Datasets
        db_path: Pfad zur DuckDB Datenbank (optional)
        matrix_path: Pfad zur B1 Matrix CSV (optional)
        category_separator: Trennzeichen zwischen Kategorien im Satz
        instance_separator: Trennzeichen zwischen Instanzen einer Kategorie
    """

    # Association thresholds
    assoc_threshold: float = 0.0

    # Slot allocation
    max_instances: int = 10
    max_instances_per_category: int = 5
    max_categories: int = 6
    max_filler_slots: int = 2

    # Dataset settings
    target_dataset: str = "swissnames3d"
    static_datasets: List[StaticDatasetConfig] = field(default_factory=list)

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

    @classmethod
    def from_config_yaml(cls, config_path: Optional[Path] = None) -> "SentenceGeneratorConfig":
        """Erstellt Config aus config.yaml (liest static_datasets automatisch).

        Args:
            config_path: Pfad zur config.yaml. Default: {project_root}/config.yaml
        """
        import yaml

        if config_path is None:
            config_path = Path(__file__).parent.parent / "config.yaml"

        if not config_path.exists():
            return cls()

        with open(config_path) as f:
            raw = yaml.safe_load(f)

        static_datasets = []
        target_dataset = "swissnames3d"

        for ds in raw.get("datasets", []):
            role = ds.get("role", "target")
            if role == "target":
                target_dataset = ds["name"]
            elif role in ("static_context", "dynamic_context"):
                if "slots" in ds and "label" in ds:
                    static_datasets.append(StaticDatasetConfig(
                        name=ds["name"],
                        slots=ds["slots"],
                        label=ds["label"],
                    ))

        return cls(
            target_dataset=target_dataset,
            static_datasets=static_datasets,
        )
