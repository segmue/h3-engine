"""
CandidateSentenceGenerator - Generiert beschreibende Saetze fuer Gazetteer-Features.

Verwendet H3 Spatial Intersection und die B1 Association Matrix um fuer
jedes Feature relevante Kontext-Instanzen zu finden und einen semantisch
reichhaltigen Beschreibungssatz zu generieren.

Beispiel-Output:
    Alpiner Gipfel "Matterhorn" bei LP 123 (Landesgrenzstein); Theodulstrasse (Strasse)
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple, TYPE_CHECKING

from .config import SentenceGeneratorConfig
from .association_loader import AssociationMatrixLoader
from .templates import SentenceTemplate

if TYPE_CHECKING:
    from engine import H3Engine, CellSet


@dataclass
class FeatureInput:
    """Input-Feature fuer die Satzgenerierung.

    Attributes:
        feature_id: Eindeutige ID des Features in der DuckDB
        name: Name des Features (kann None sein)
        objektart: OBJEKTART/Kategorie des Features
    """
    feature_id: int
    name: Optional[str]
    objektart: str


@dataclass
class GeneratedSentence:
    """Ergebnis der Satzgenerierung.

    Attributes:
        feature_id: ID des Quell-Features
        sentence: Der generierte Beschreibungssatz
        context_by_category: Dict von {OBJEKTART: [Namen]} der gefundenen Kontext-Instanzen
        categories_used: Liste der verwendeten Kategorien (in Reihenfolge)
    """
    feature_id: int
    sentence: str
    context_by_category: Dict[str, List[str]]
    categories_used: List[str]


class CandidateSentenceGenerator:
    """Generiert beschreibende Saetze fuer Gazetteer-Features.

    Verwendet:
    - B1 Association Matrix: Welche OBJEKTART-Kategorien sind assoziiert
    - H3Engine.intersects_predicate(): Welche Instanzen intersecten raeumlich

    Algorithmus:
    1. Relevante Kategorien aus B1-Matrix holen (threshold > 0.05)
    2. Slots proportional nach Assoziationsstaerke verteilen
    3. Fuer jede Kategorie: intersecting Instanzen via H3Engine finden
    4. Satz aus Template bauen

    Example:
        from engine import H3Engine
        from sentence_generator import CandidateSentenceGenerator, FeatureInput

        engine = H3Engine("data/swissNAMES3D_combined_h3.duckdb")
        generator = CandidateSentenceGenerator(engine)

        feature = FeatureInput(feature_id=123, name="Matterhorn", objektart="Alpiner Gipfel")
        result = generator.generate(feature)
        print(result.sentence)
    """

    def __init__(
        self,
        engine: "H3Engine",
        config: Optional[SentenceGeneratorConfig] = None
    ):
        """
        Initialisiert den Generator.

        Args:
            engine: H3Engine Instanz (bereits verbunden zur DuckDB)
            config: Optionale Konfiguration (sonst Defaults)
        """
        self.engine = engine
        self.config = config or SentenceGeneratorConfig()

        # Association Matrix laden
        matrix_path = self.config.get_matrix_path()
        self._assoc_loader = AssociationMatrixLoader(matrix_path)

        # Template-Handler
        self._template = SentenceTemplate(self.config)

        # Cache fuer Feature-CellSets (beschleunigt Batch-Verarbeitung)
        self._cell_cache: Dict[int, "CellSet"] = {}

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def generate(self, feature: FeatureInput) -> GeneratedSentence:
        """Generiert einen beschreibenden Satz fuer ein einzelnes Feature.

        Args:
            feature: Das Quell-Feature

        Returns:
            GeneratedSentence mit Satz und Kontext-Informationen
        """
        # 1. Assoziierte Kategorien aus B1-Matrix holen
        associated = self._assoc_loader.get_associated_categories(
            source_objektart=feature.objektart,
            threshold=self.config.assoc_threshold,
            max_categories=self.config.max_categories
        )

        # Keine Assoziationen gefunden
        if not associated:
            return GeneratedSentence(
                feature_id=feature.feature_id,
                sentence=self._template.format_feature(feature.name, feature.objektart),
                context_by_category={},
                categories_used=[]
            )

        # 2. Slots proportional verteilen
        slot_allocation = self._allocate_slots(associated)

        # 3. CellSet fuer das Feature holen
        feature_cells = self._get_feature_cells(feature.feature_id)

        # 4. Intersecting Instanzen pro Kategorie finden
        context_by_category: Dict[str, List[str]] = {}

        for objektart, slots in slot_allocation.items():
            if slots == 0:
                continue

            names = self._find_intersecting_names(
                feature_cells=feature_cells,
                target_objektart=objektart,
                max_instances=slots,
                exclude_feature_id=feature.feature_id
            )

            if names:
                context_by_category[objektart] = names

        # 5. Satz bauen
        sentence = self._template.build_sentence(
            name=feature.name,
            objektart=feature.objektart,
            context_by_category=context_by_category
        )

        return GeneratedSentence(
            feature_id=feature.feature_id,
            sentence=sentence,
            context_by_category=context_by_category,
            categories_used=list(context_by_category.keys())
        )

    def generate_batch(
        self,
        features: List[FeatureInput]
    ) -> List[GeneratedSentence]:
        """Generiert Saetze fuer mehrere Features (optimiert).

        Nutzt Caching fuer bessere Performance bei vielen Features.

        Args:
            features: Liste von Quell-Features

        Returns:
            Liste von GeneratedSentence in gleicher Reihenfolge
        """
        # Cache leeren fuer frischen Batch
        self._cell_cache.clear()

        results = []
        for feature in features:
            results.append(self.generate(feature))

        return results

    # -------------------------------------------------------------------------
    # Slot Allocation
    # -------------------------------------------------------------------------

    def _allocate_slots(
        self,
        associated: List[Tuple[str, float]]
    ) -> Dict[str, int]:
        """Verteilt Instanz-Slots proportional nach Assoziationsstaerke.

        Args:
            associated: Liste von (OBJEKTART, B1-Wert) sortiert nach B1 desc

        Returns:
            Dict von {OBJEKTART: Anzahl_Slots}
        """
        if not associated:
            return {}

        total_slots = self.config.max_instances
        max_per_cat = self.config.max_instances_per_category

        # Gesamtgewicht berechnen
        total_weight = sum(b1 for _, b1 in associated)
        if total_weight <= 0:
            return {}

        # Proportionale Verteilung
        allocation: Dict[str, int] = {}
        remaining_slots = total_slots

        for objektart, b1_value in associated:
            if remaining_slots <= 0:
                break

            # Proportionale Slots (mindestens 1 wenn ueber Schwelle)
            raw_slots = (b1_value / total_weight) * total_slots
            slots = max(1, min(int(round(raw_slots)), max_per_cat))
            slots = min(slots, remaining_slots)

            allocation[objektart] = slots
            remaining_slots -= slots

        return allocation

    # -------------------------------------------------------------------------
    # H3 Intersection
    # -------------------------------------------------------------------------

    def _get_feature_cells(self, feature_id: int) -> "CellSet":
        """Holt oder cached das CellSet fuer ein Feature."""
        if feature_id not in self._cell_cache:
            self._cell_cache[feature_id] = self.engine.union(
                f"feature_id = {feature_id}"
            )
        return self._cell_cache[feature_id]

    def _find_intersecting_names(
        self,
        feature_cells: "CellSet",
        target_objektart: str,
        max_instances: int,
        exclude_feature_id: int
    ) -> List[str]:
        """Findet Namen von Features die mit dem CellSet intersecten.

        Verwendet H3Engine.intersects_predicate() fuer die raeumliche Abfrage.

        Args:
            feature_cells: CellSet des Quell-Features
            target_objektart: OBJEKTART der Ziel-Features
            max_instances: Maximale Anzahl zurueckzugebender Namen
            exclude_feature_id: Feature-ID die ausgeschlossen werden soll

        Returns:
            Liste von Feature-Namen (nur benannte Features)
        """
        # Predicate fuer Intersection bauen
        predicate = self.engine.intersects_predicate(feature_cells)

        # OBJEKTART escapen (fuer Single Quotes)
        safe_objektart = target_objektart.replace("'", "''")

        # Query: Features der Kategorie die intersecten und einen Namen haben
        try:
            result = self.engine.features.filter(
                f"OBJEKTART = '{safe_objektart}' "
                f"AND NAME IS NOT NULL "
                f"AND feature_id != {exclude_feature_id} "
                f"AND {predicate}"
            ).limit(max_instances).df()

            if result.empty:
                return []

            return result['NAME'].tolist()

        except Exception:
            # Bei Fehlern leere Liste zurueckgeben
            return []

    # -------------------------------------------------------------------------
    # Utility
    # -------------------------------------------------------------------------

    def clear_cache(self) -> None:
        """Leert den CellSet-Cache."""
        self._cell_cache.clear()

    def get_available_categories(self) -> List[str]:
        """Gibt alle verfuegbaren OBJEKTART-Kategorien zurueck."""
        return self._assoc_loader.get_all_categories()
