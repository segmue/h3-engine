"""
Satz-Templates und Formatierung.
"""

from typing import Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from .config import SentenceGeneratorConfig


class SentenceTemplate:
    """Formatiert Features und Kontext zu beschreibenden Saetzen.

    Format:
        {OBJEKTART} "{NAME}" bei {Instanz1}, {Instanz2} ({Kategorie1}); {Instanz3} ({Kategorie2}); ...

    Beispiel:
        Alpiner Gipfel "Matterhorn" bei Zmuttgrat, Hoernligrat (Grat); Theodulstrasse (Strasse)
    """

    def __init__(self, config: "SentenceGeneratorConfig"):
        self.config = config

    def format_feature(self, name: str, objektart: str) -> str:
        """Formatiert das Haupt-Feature.

        Args:
            name: Feature-Name (kann None sein)
            objektart: OBJEKTART des Features

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

        Args:
            objektart: OBJEKTART der Instanzen
            instance_names: Liste der Instanz-Namen

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
        context_by_category: Dict[str, List[str]]
    ) -> str:
        """Baut den kompletten beschreibenden Satz.

        Args:
            name: Name des Haupt-Features
            objektart: OBJEKTART des Haupt-Features
            context_by_category: Dict von {OBJEKTART: [Namen]} fuer Kontext

        Returns:
            Kompletter Satz wie 'Alpiner Gipfel "Matterhorn" bei Zmuttgrat (Grat); ...'
        """
        # Feature selbst
        feature_part = self.format_feature(name, objektart)

        # Kontext-Teile sammeln
        context_parts = []
        for cat_objektart, names in context_by_category.items():
            if names:
                formatted = self.format_category_group(cat_objektart, names)
                if formatted:
                    context_parts.append(formatted)

        # Zusammenbauen
        if not context_parts:
            return feature_part

        context_str = self.config.category_separator.join(context_parts)
        return f"{feature_part} bei {context_str}"
