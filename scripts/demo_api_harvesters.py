"""
Example script demonstrating API harvester usage.

This script shows how to use individual harvesters and quality filters
for testing and validation purposes.
"""
import sys
from pathlib import Path

from loguru import logger

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.bootstrap import (
    PubChemHarvester,
    ChemicalResolverHarvester,
    filter_synonyms,
    validate_cas_format,
)


def demo_pubchem():
    """Demonstrate PubChem harvester."""
    logger.info("=" * 80)
    logger.info("PubChem Harvester Demo")
    logger.info("=" * 80)

    # Test CAS numbers
    test_chemicals = [
        ("71-43-2", "Benzene"),
        ("108-88-3", "Toluene"),
        ("50-00-0", "Formaldehyde"),
    ]

    with PubChemHarvester() as harvester:
        for cas, name in test_chemicals:
            logger.info(f"\nQuerying: {name} ({cas})")

            # Harvest synonyms
            synonyms = harvester.harvest_synonyms(cas, name)
            logger.info(f"Found {len(synonyms)} raw synonyms")

            # Show first 10
            if synonyms:
                logger.info("Sample synonyms:")
                for syn in synonyms[:10]:
                    logger.info(f"  - {syn}")

            # Get properties
            properties = harvester.get_properties(cas)
            if properties:
                logger.info(f"Molecular Formula: {properties.get('MolecularFormula')}")
                logger.info(f"IUPAC Name: {properties.get('IUPACName')}")


def demo_chemical_resolver():
    """Demonstrate Chemical Resolver harvester."""
    logger.info("\n" + "=" * 80)
    logger.info("Chemical Identifier Resolver Demo")
    logger.info("=" * 80)

    test_chemicals = [
        ("71-43-2", "Benzene"),
        ("108-88-3", "Toluene"),
    ]

    with ChemicalResolverHarvester() as harvester:
        for cas, name in test_chemicals:
            logger.info(f"\nQuerying: {name} ({cas})")

            # Get synonyms
            synonyms = harvester.harvest_synonyms(cas, name)
            logger.info(f"Found {len(synonyms)} names from NCI")

            # Get structure identifiers
            smiles = harvester.get_smiles(cas)
            inchi = harvester.get_inchi(cas)
            inchi_key = harvester.get_inchi_key(cas)

            if smiles:
                logger.info(f"SMILES: {smiles}")
            if inchi_key:
                logger.info(f"InChIKey: {inchi_key}")


def demo_quality_filters():
    """Demonstrate quality filtering."""
    logger.info("\n" + "=" * 80)
    logger.info("Quality Filter Demo")
    logger.info("=" * 80)

    # Mock synonym list with various quality issues
    test_synonyms = [
        "Benzene",  # Good
        "benzene",  # Duplicate (case-insensitive)
        "Benzol",  # Good
        "Benzene solution",  # Mixture term
        "Benzene standard",  # Generic term
        "BenzPro®",  # Trade name
        "B",  # Too short (invalid abbreviation)
        "Cyclohexatriene",  # Good
        "Phenyl hydride",  # Good
        "A" * 150,  # Too long
        "Benzène",  # Non-ASCII
    ]

    logger.info(f"\nInput synonyms ({len(test_synonyms)}):")
    for syn in test_synonyms:
        logger.info(f"  - {syn[:50]}{'...' if len(syn) > 50 else ''}")

    # Apply filters
    filtered = filter_synonyms(test_synonyms, "single_substance")

    logger.info(f"\nFiltered synonyms ({len(filtered)}):")
    for syn in filtered:
        logger.info(f"  - {syn}")

    logger.info(f"\nRetention rate: {len(filtered)}/{len(test_synonyms)} = {len(filtered)/len(test_synonyms)*100:.1f}%")


def demo_cas_validation():
    """Demonstrate CAS validation."""
    logger.info("\n" + "=" * 80)
    logger.info("CAS Number Validation Demo")
    logger.info("=" * 80)

    test_cas = [
        ("71-43-2", True),
        ("108-88-3", True),
        ("50-00-0", True),
        ("71-43-3", False),  # Wrong check digit
        ("12345-67-8", False),  # Invalid check digit
        ("abc-de-f", False),  # Non-numeric
        ("", False),  # Empty
    ]

    for cas, expected_valid in test_cas:
        is_valid = validate_cas_format(cas)
        status = "✓" if is_valid == expected_valid else "✗"
        logger.info(f"{status} {cas:20s} -> {'Valid' if is_valid else 'Invalid'}")


def main():
    """Run all demos."""
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    logger.info("API Harvester Demo Script")
    logger.info("This script demonstrates the bootstrap API harvesters")
    logger.info("")

    try:
        demo_cas_validation()
        demo_quality_filters()
        demo_pubchem()
        demo_chemical_resolver()

        logger.info("\n" + "=" * 80)
        logger.info("Demo Complete!")
        logger.info("=" * 80)
        logger.info("\nNote: Network-dependent demos may fail without internet connection")

    except KeyboardInterrupt:
        logger.warning("\nDemo interrupted by user")
    except Exception as e:
        logger.error(f"\nDemo failed: {e}")
        raise


if __name__ == "__main__":
    main()
