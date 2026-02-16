"""
Text normalization package for chemical name processing.

This package provides robust text normalization capabilities for chemical names,
handling variants in notation, qualifiers, CAS numbers, and petroleum hydrocarbons.
"""

from .text_normalizer import TextNormalizer, normalize_text
from .qualifier_handler import QualifierHandler
from .cas_extractor import CASExtractor
from .petroleum_handler import PetroleumHandler

__all__ = [
    'TextNormalizer',
    'normalize_text',
    'QualifierHandler',
    'CASExtractor',
    'PetroleumHandler',
]
