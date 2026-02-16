"""
Demonstrate chemical name parsing for Ontario lab-specific notations.

Shows how the parser handles:
- Trailing locants: "Methylnaphthalene 1-" vs "1-Methylnaphthalene"
- Position descriptors: "ortho-" vs "1,2-"
- Qualifier text: "(hot water soluble)", "(total)"
- Abbreviated forms: "p-dichlorobenzene" vs "para-dichlorobenzene"
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.normalization.chemical_parser import ChemicalNameParser


def main():
    """Demonstrate parsing capabilities."""
    parser = ChemicalNameParser()
    
    # Test cases showing Ontario lab variations
    test_cases = [
        # Trailing locants (Ontario style)
        "Methylnaphthalene 1-",
        "1-Methylnaphthalene",
        "Methylnaphthalene 2-",
        "2-Methylnaphthalene",
        
        # Aromatic position descriptors
        "para-dichlorobenzene",
        "p-dichlorobenzene",
        "1,4-Dichlorobenzene",
        
        # Complex PAHs with brackets
        "Benz[a]anthracene",
        "Benzo[a]pyrene",
        "Benzo[g,h,i]perylene",
        
        # Qualifier text
        "Boron (hot water soluble)",
        "Boron (total)",
        "Chromium (total)",
        "Chromium VI",
        
        # Trailing notation
        "Hexachlorocyclohexane gamma-",
        "Dimethylphenol 2,4-",
        "Dichlorobenzidine 3,3'-",
        
        # Multiple locants
        "1,2-Dichloroethane",
        "1,2,3-Trichlorobenzene",
        "2,4,6-Trichlorophenol",
    ]
    
    print("=" * 80)
    print("CHEMICAL NAME PARSING DEMONSTRATION")
    print("=" * 80)
    print()
    
    for name in test_cases:
        print("-" * 80)
        print(parser.explain_parse(name))
        print()
        
        # Show variants
        variants = parser.generate_variants(name)
        if len(variants) > 1:
            print("  Name Variants Generated:")
            for variant in sorted(variants):
                if variant != name:
                    print(f"    → {variant}")
            print()
    
    print("=" * 80)
    print("NORMALIZATION COMPARISON")
    print("=" * 80)
    print()
    
    # Show how different forms normalize to the same thing
    equivalence_groups = [
        [
            "1-Methylnaphthalene",
            "Methylnaphthalene 1-",
            "1-methyl-naphthalene",
        ],
        [
            "1,4-Dichlorobenzene",
            "para-dichlorobenzene",
            "p-dichlorobenzene",
        ],
        [
            "2,4-Dimethylphenol",
            "Dimethylphenol 2,4-",
            "2,4-dimethyl-phenol",
        ],
    ]
    
    for group in equivalence_groups:
        print(f"Equivalence Group:")
        normalized_forms = set()
        for variant in group:
            components = parser.parse(variant)
            normalized_forms.add(components.normalized_form)
            print(f"  '{variant}' → '{components.normalized_form}'")
        
        if len(normalized_forms) == 1:
            print(f"  ✓ All normalize to: '{normalized_forms.pop()}'")
        else:
            print(f"  ⚠ Different normalized forms: {normalized_forms}")
        print()


if __name__ == "__main__":
    main()
