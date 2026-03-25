"""
Sentence Generator Module.

Generiert beschreibende Saetze fuer geografische Features basierend auf
H3 Spatial Intersection und der B1 Association Matrix.

Beispiel:
    from engine import H3Engine
    from sentence_generator import CandidateSentenceGenerator, FeatureInput

    engine = H3Engine("data/swissNAMES3D_combined_h3.duckdb")
    generator = CandidateSentenceGenerator(engine)

    # Einzelnes Feature
    feature = FeatureInput(feature_id=123, name="Matterhorn", objektart="Alpiner Gipfel")
    result = generator.generate(feature)
    print(result.sentence)
    # -> Alpiner Gipfel "Matterhorn" bei Zmuttgrat, Hoernligrat (Grat); Theodulstrasse (Strasse)

    # Batch-Verarbeitung
    features = [...]
    results = generator.generate_batch(features)
"""

from .sentence_generator import (
    CandidateSentenceGenerator,
    FeatureInput,
    GeneratedSentence,
)
from .config import SentenceGeneratorConfig
from .association_loader import AssociationMatrixLoader

__all__ = [
    "CandidateSentenceGenerator",
    "FeatureInput",
    "GeneratedSentence",
    "SentenceGeneratorConfig",
    "AssociationMatrixLoader",
]
