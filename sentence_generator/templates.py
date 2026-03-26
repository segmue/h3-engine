"""
Satz-Templates und Formatierung.

Format mit Static Context:
    {OBJEKTART} "{NAME}" in {Static1} ({Label1}), {Static2} ({Label2}). Bei {Inst1} ({Kat1}); ...

Beispiel:
    Alpiner Gipfel "Matterhorn" in Zermatt (Gemeinde), Wallis (Kanton). Bei Zmuttgrat (Grat); Theodulstrasse (Strasse)
"""

from typing import Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .config import SentenceGeneratorConfig


class SentenceTemplate:
    """Formatiert Features und Kontext zu beschreibenden Saetzen."""

    def __init__(self, config: "SentenceGeneratorConfig"):
        self.config = config

    def format_feature(self, name: str, objektart: str) -> str:
        """Formatiert das Haupt-Feature.

        Returns:
            Formatierter String wie 'Alpiner Gipfel "Matterhorn"'
        """
        if name:
            return f'{objektart} "{name}"'
        return objektart

    def format_category_group(
        self,
        objektart: str,
        instance_names: List[str]
    ) -> str:
        """Formatiert eine Gruppe von Instanzen einer Kategorie.

        Returns:
            Formatierter String wie 'Zmuttgrat, Hoernligrat (Grat)'
        """
        if not instance_names:
            return ""

        names_str = self.config.instance_separator.join(instance_names)
        return f"{names_str} ({objektart})"

    def build_sentence(
        self,
        name: str,
        objektart: str,
        static_context: Optional[Dict[str, List[str]]] = None,
        context_by_category: Optional[Dict[str, List[str]]] = None,
    ) -> str:
        """Baut den kompletten beschreibenden Satz.

        Args:
            name: Name des Haupt-Features
            objektart: OBJEKTART des Haupt-Features
            static_context: Dict von {Label: [Namen]} fuer statischen Kontext
                            z.B. {'Gemeinde': ['Zermatt'], 'Kanton': ['Wallis']}
            context_by_category: Dict von {OBJEKTART: [Namen]} fuer dynamischen Kontext

        Returns:
            Kompletter Satz, z.B.:
            'Alpiner Gipfel "Matterhorn" in Zermatt (Gemeinde). Bei Zmuttgrat (Grat)'
        """
        feature_part = self.format_feature(name, objektart)

        parts = []

        # Static Context: "in Zermatt (Gemeinde), Wallis (Kanton)"
        if static_context:
            static_items = []
            for label, names in static_context.items():
                for n in names:
                    static_items.append(f"{n} ({label})")
            if static_items:
                parts.append("in " + self.config.instance_separator.join(static_items))

        # Dynamic Context: "bei Zmuttgrat (Grat); Theodulstrasse (Strasse)"
        if context_by_category:
            dynamic_items = []
            for cat_objektart, names in context_by_category.items():
                if names:
                    formatted = self.format_category_group(cat_objektart, names)
                    if formatted:
                        dynamic_items.append(formatted)
            if dynamic_items:
                parts.append("bei " + self.config.category_separator.join(dynamic_items))

        if not parts:
            return feature_part

        return feature_part + " " + ". ".join(parts)
