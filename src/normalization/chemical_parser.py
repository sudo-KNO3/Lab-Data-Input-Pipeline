"""
Chemical name parser based on IUPAC naming conventions.

Breaks down chemical names into structural components (parent chain, substituents,
locants, functional groups) to enable better matching and normalization of
variant naming forms.

This addresses Ontario lab-specific notations like:
- Trailing vs leading locants: "Methylnaphthalene 1-" vs "1-Methylnaphthalene"
- Position descriptor formats: "ortho-" vs "1,2-"
- Abbreviated vs full forms: "p-dichlorobenzene" vs "para-dichlorobenzene"
"""
import re
from dataclasses import dataclass
from typing import List, Optional, Set, Tuple

from loguru import logger


@dataclass
class ChemicalNameComponents:
    """
    Parsed components of a chemical name.
    
    Attributes:
        raw_name: Original input name
        parent_chain: Root carbon backbone (meth, eth, prop, benz, etc)
        bond_type: Primary suffix (-ane, -ene, -yne)
        functional_group: Priority suffix (-ol, -al, -one, -oic acid)
        substituents: List of substituent groups
        locants: Position numbers for substituents
        multiplicity: di-, tri-, tetra-, etc
        aromatic_position: ortho/meta/para descriptors
        stereochemistry: R/S, E/Z, cis/trans descriptors
        normalized_form: Canonical ordered form
    """
    raw_name: str
    parent_chain: Optional[str] = None
    bond_type: Optional[str] = None
    functional_group: Optional[str] = None
    substituents: List[str] = None
    locants: List[int] = None
    multiplicity: Optional[str] = None
    aromatic_position: Optional[str] = None
    stereochemistry: Optional[str] = None
    normalized_form: Optional[str] = None
    
    def __post_init__(self):
        if self.substituents is None:
            self.substituents = []
        if self.locants is None:
            self.locants = []


class ChemicalNameParser:
    """
    Parse chemical names into IUPAC structural components.
    
    Handles:
    - Parent chain identification (meth-, eth-, prop-, benz-, etc)
    - Bond type suffixes (-ane, -ene, -yne)
    - Functional groups (-ol, -al, -one, -oic acid, -amine)
    - Substituents (chloro-, bromo-, methyl-, nitro-, etc)
    - Locants (position numbers)
    - Multiplicity prefixes (di-, tri-, tetra-)
    - Aromatic positions (ortho/meta/para ↔ 1,2- / 1,3- / 1,4-)
    """
    
    # Parent chain roots (carbon backbones)
    PARENT_CHAINS = {
        'meth', 'eth', 'prop', 'but', 'pent', 'hex', 'hept', 'oct', 'non', 'dec',
        'undec', 'dodec', 'tridec', 'tetradec', 'pentadec', 'hexadec',
        'benz', 'naphthal', 'anthrac', 'phenanthr', 'pyren', 'chrysen',
        'fluoranth', 'fluoren', 'acenaph',
    }
    
    # Bond type suffixes
    BOND_TYPES = {
        'ane': 'single',
        'ene': 'double',
        'yne': 'triple',
        'adiene': 'two_double',
        'atriene': 'three_double',
    }
    
    # Functional group suffixes (priority order)
    FUNCTIONAL_GROUPS = {
        'oic acid': 'carboxylic_acid',
        'al': 'aldehyde',
        'one': 'ketone',
        'ol': 'alcohol',
        'amine': 'amine',
        'ether': 'ether',
        'thiol': 'thiol',
        'nitrile': 'nitrile',
    }
    
    # Common substituents
    SUBSTITUENTS = {
        'methyl', 'ethyl', 'propyl', 'butyl', 'pentyl', 'hexyl',
        'chloro', 'bromo', 'iodo', 'fluoro',
        'nitro', 'amino', 'hydroxy', 'carboxy',
        'phenyl', 'benzyl', 'tolyl',
        'cyano', 'acetyl', 'formyl',
    }
    
    # Multiplicity prefixes
    MULTIPLICITIES = {
        'di': 2, 'tri': 3, 'tetra': 4, 'penta': 5,
        'hexa': 6, 'hepta': 7, 'octa': 8, 'nona': 9, 'deca': 10,
    }
    
    # Aromatic position descriptors
    AROMATIC_POSITIONS = {
        'ortho': '1,2', 'o': '1,2',
        'meta': '1,3', 'm': '1,3',
        'para': '1,4', 'p': '1,4',
    }
    
    # Stereochemistry descriptors
    STEREO_DESCRIPTORS = {
        'cis', 'trans', 'E', 'Z', 'R', 'S', '+', '-', 'D', 'L',
        'alpha', 'beta', 'gamma', 'delta',
    }
    
    def __init__(self):
        """Initialize parser with compiled regex patterns."""
        # Pattern for locants: numbers with optional separators
        self.locant_pattern = re.compile(r'\b(\d+(?:[,\-]\d+)*)[\']*\s*-')
        
        # Pattern for trailing locants (Ontario lab style)
        self.trailing_locant_pattern = re.compile(r'\s+(\d+(?:[,\-]\d+)*)\s*-?\s*$')
        
        # Pattern for multiplicity prefixes
        mult_pattern = '|'.join(self.MULTIPLICITIES.keys())
        self.multiplicity_pattern = re.compile(rf'\b({mult_pattern})-?', re.IGNORECASE)
        
        # Pattern for aromatic positions
        aromatic_pattern = '|'.join(self.AROMATIC_POSITIONS.keys())
        self.aromatic_pattern = re.compile(rf'\b({aromatic_pattern})-', re.IGNORECASE)
        
        # Pattern for stereochemistry
        stereo_pattern = '|'.join(re.escape(s) for s in self.STEREO_DESCRIPTORS)
        self.stereo_pattern = re.compile(rf'\(({stereo_pattern})\)', re.IGNORECASE)
    
    def parse(self, name: str) -> ChemicalNameComponents:
        """
        Parse a chemical name into components.
        
        Args:
            name: Chemical name to parse
            
        Returns:
            ChemicalNameComponents with identified structural parts
        """
        if not name:
            return ChemicalNameComponents(raw_name=name)
        
        # Normalize case for pattern matching
        name_lower = name.lower().strip()
        
        components = ChemicalNameComponents(raw_name=name)
        
        # Extract stereochemistry (remove from working string)
        stereo_matches = self.stereo_pattern.findall(name)
        if stereo_matches:
            components.stereochemistry = ','.join(stereo_matches)
            name_lower = self.stereo_pattern.sub('', name_lower).strip()
        
        # Extract locants (leading position)
        locant_match = self.locant_pattern.search(name_lower)
        if locant_match:
            locant_str = locant_match.group(1)
            components.locants = self._parse_locants(locant_str)
        
        # Check for trailing locants (Ontario lab style)
        trailing_match = self.trailing_locant_pattern.search(name_lower)
        if trailing_match and not components.locants:
            locant_str = trailing_match.group(1)
            components.locants = self._parse_locants(locant_str)
        
        # Extract aromatic position descriptors
        aromatic_match = self.aromatic_pattern.search(name_lower)
        if aromatic_match:
            aromatic_desc = aromatic_match.group(1).lower()
            components.aromatic_position = aromatic_desc
            # Convert to numeric locant
            if aromatic_desc in self.AROMATIC_POSITIONS and not components.locants:
                numeric = self.AROMATIC_POSITIONS[aromatic_desc]
                components.locants = self._parse_locants(numeric)
        
        # Extract multiplicity
        mult_match = self.multiplicity_pattern.search(name_lower)
        if mult_match:
            components.multiplicity = mult_match.group(1).lower()
        
        # Extract substituents
        for substituent in self.SUBSTITUENTS:
            if substituent in name_lower:
                components.substituents.append(substituent)
        
        # Extract parent chain
        for chain in self.PARENT_CHAINS:
            if chain in name_lower:
                components.parent_chain = chain
                break
        
        # Extract bond type
        for bond_suffix, bond_type in self.BOND_TYPES.items():
            if bond_suffix in name_lower:
                components.bond_type = bond_suffix
                break
        
        # Extract functional group
        for func_suffix, func_type in self.FUNCTIONAL_GROUPS.items():
            if func_suffix in name_lower:
                components.functional_group = func_suffix
                break
        
        # Generate normalized form
        components.normalized_form = self._generate_normalized_form(components)
        
        return components
    
    def _parse_locants(self, locant_str: str) -> List[int]:
        """
        Parse locant string into list of position numbers.
        
        Args:
            locant_str: String like "1,2" or "1-2" or "1,2,4"
            
        Returns:
            List of integer positions
        """
        # Replace hyphens with commas for consistent splitting
        locant_str = locant_str.replace('-', ',')
        
        try:
            locants = [int(num.strip()) for num in locant_str.split(',') if num.strip()]
            return sorted(locants)
        except ValueError:
            return []
    
    def _generate_normalized_form(self, components: ChemicalNameComponents) -> str:
        """
        Generate canonical normalized form from components.
        
        Applies consistent ordering:
        1. Locants (sorted, comma-separated)
        2. Multiplicity prefix
        3. Substituents (alphabetical)
        4. Parent chain
        5. Bond type/functional group
        
        Args:
            components: Parsed components
            
        Returns:
            Normalized chemical name
        """
        parts = []
        
        # Locants first (if present)
        if components.locants:
            locant_str = ','.join(str(n) for n in sorted(components.locants))
            parts.append(locant_str + '-')
        
        # Multiplicity
        if components.multiplicity:
            parts.append(components.multiplicity)
        
        # Substituents (alphabetical)
        if components.substituents:
            subs = sorted(components.substituents)
            parts.append(''.join(subs))
        
        # Parent chain
        if components.parent_chain:
            parts.append(components.parent_chain)
        
        # Functional group or bond type
        if components.functional_group:
            parts.append(components.functional_group)
        elif components.bond_type:
            parts.append(components.bond_type)
        
        normalized = ''.join(parts)
        
        # If we couldn't build a normalized form, return lowercase raw name
        if not normalized or normalized == '':
            normalized = components.raw_name.lower().strip()
        
        return normalized
    
    def generate_variants(self, name: str) -> Set[str]:
        """
        Generate naming variants for better matching.
        
        Creates alternative forms:
        - Locants at beginning vs end
        - Numeric vs aromatic position descriptors
        - With/without hyphens
        - Abbreviated vs full aromatic descriptors
        
        Args:
            name: Chemical name
            
        Returns:
            Set of name variants
        """
        components = self.parse(name)
        variants = {name, name.lower(), components.normalized_form}
        
        # Add variant with leading locants
        if components.locants and not name.startswith(str(components.locants[0])):
            locant_str = ','.join(str(n) for n in components.locants)
            # Remove trailing locants
            base = self.trailing_locant_pattern.sub('', name)
            variants.add(f"{locant_str}-{base}")
        
        # Add variant with trailing locants (Ontario style)
        if components.locants:
            locant_str = ' '.join(str(n) for n in components.locants)
            # Remove leading locants
            base = self.locant_pattern.sub('', name)
            variants.add(f"{base} {locant_str}-")
        
        # Add aromatic position variants
        if components.aromatic_position and components.aromatic_position in self.AROMATIC_POSITIONS:
            numeric = self.AROMATIC_POSITIONS[components.aromatic_position]
            base = self.aromatic_pattern.sub('', name)
            variants.add(f"{numeric}-{base}")
            
            # Add all aromatic descriptors for this position
            for desc, num in self.AROMATIC_POSITIONS.items():
                if num == numeric:
                    variants.add(f"{desc}-{base}")
        
        # Add hyphen variants
        if '-' in name:
            variants.add(name.replace('-', ''))
        if ' ' in name:
            variants.add(name.replace(' ', '-'))
        
        return {v.strip() for v in variants if v.strip()}
    
    def explain_parse(self, name: str) -> str:
        """
        Generate human-readable explanation of parsed components.
        
        Args:
            name: Chemical name to explain
            
        Returns:
            Multi-line explanation string
        """
        components = self.parse(name)
        
        lines = [
            f"Chemical Name: {components.raw_name}",
            f"Normalized: {components.normalized_form}",
            "",
        ]
        
        if components.locants:
            lines.append(f"  Locants (positions): {', '.join(str(n) for n in components.locants)}")
        
        if components.multiplicity:
            count = self.MULTIPLICITIES.get(components.multiplicity, '?')
            lines.append(f"  Multiplicity: {components.multiplicity} ({count} instances)")
        
        if components.substituents:
            lines.append(f"  Substituents: {', '.join(components.substituents)}")
        
        if components.parent_chain:
            lines.append(f"  Parent Chain: {components.parent_chain}")
        
        if components.bond_type:
            bond_desc = self.BOND_TYPES.get(components.bond_type, components.bond_type)
            lines.append(f"  Bond Type: {components.bond_type} ({bond_desc})")
        
        if components.functional_group:
            func_desc = self.FUNCTIONAL_GROUPS.get(components.functional_group, components.functional_group)
            lines.append(f"  Functional Group: {components.functional_group} ({func_desc})")
        
        if components.aromatic_position:
            lines.append(f"  Aromatic Position: {components.aromatic_position}")
        
        if components.stereochemistry:
            lines.append(f"  Stereochemistry: {components.stereochemistry}")
        
        return '\n'.join(lines)


def parse_chemical_name(name: str) -> ChemicalNameComponents:
    """
    Convenience function to parse a chemical name.
    
    Args:
        name: Chemical name to parse
        
    Returns:
        Parsed components
    """
    parser = ChemicalNameParser()
    return parser.parse(name)


def generate_name_variants(name: str) -> Set[str]:
    """
    Convenience function to generate name variants.
    
    Args:
        name: Chemical name
        
    Returns:
        Set of variant forms
    """
    parser = ChemicalNameParser()
    return parser.generate_variants(name)


class ExhaustiveVariantGenerator:
    """
    Generate exhaustive synonym variants for chemical names.
    
    Produces 20-50 unique variants per compound by systematically exploring:
    - Qualifier handling (total, HWS, oxidation states)
    - Locant positions (leading, trailing, separators)
    - Aromatic descriptors (ortho/meta/para vs numeric)
    - Stereochemistry (cis/trans/E/Z)
    - Hyphen/space/concatenation variants
    - Case variations
    
    Handles special qualifier logic:
    - "Chromium (total)" -> strip qualifier, use "Chromium" base
    - "Chromium VI" -> keep oxidation state, generate Cr(VI), Cr6+, hexavalent variants
    - "Boron (hot water soluble)" -> keep HWS qualifier in variants
    """
    
    # Equivalent qualifiers that should be stripped (represent the same as base name)
    EQUIVALENT_QUALIFIERS = {
        'total', '(total)', 'totals',
    }
    
    # Distinct qualifiers that represent different species (keep in variants)
    DISTINCT_QUALIFIERS = {
        'hot water soluble', 'hws', '(hot water soluble)', '(hws)',
        'cold water soluble', 'cws',
        'extractable', 'available', 'dissolved', 'soluble',
    }
    
    # Roman numeral oxidation states
    ROMAN_NUMERALS = {
        'I': '1', 'II': '2', 'III': '3', 'IV': '4', 'V': '5', 
        'VI': '6', 'VII': '7', 'VIII': '8', 'IX': '9', 'X': '10',
    }
    
    # Oxidation state name mappings
    OXIDATION_NAMES = {
        '6': ['hexavalent', 'VI', '(VI)', 'Cr6+', 'Cr+6', 'Cr(6+)'],
        '3': ['trivalent', 'III', '(III)', 'Cr3+', 'Cr+3', 'Cr(3+)'],
        '2': ['divalent', 'II', '(II)'],
        '4': ['tetravalent', 'IV', '(IV)'],
        '5': ['pentavalent', 'V', '(V)'],
    }
    
    # Locant separators for variants
    LOCANT_SEPARATORS = [',', '-', ', ', ' ', '']
    
    # Aromatic position mappings
    AROMATIC_FULL = {
        'o': 'ortho', 'm': 'meta', 'p': 'para',
        'ortho': '1,2', 'meta': '1,3', 'para': '1,4',
    }
    
    def __init__(self, parser: ChemicalNameParser):
        """
        Initialize with a ChemicalNameParser instance.
        
        Args:
            parser: ChemicalNameParser to use for component extraction
        """
        self.parser = parser
        
        # Precompile regex patterns
        self.qualifier_pattern = re.compile(
            r'\s*\(([^)]+)\)\s*|\s+(total|hws|cws|hot water soluble|cold water soluble|extractable|available|dissolved|soluble)\s*',
            re.IGNORECASE
        )
        self.roman_pattern = re.compile(
            r'\b([IVX]+)\b$',  # Roman numerals at end
            re.IGNORECASE
        )
        self.oxidation_pattern = re.compile(
            r'\(([IVX]+|[0-9]+\+?)\)',  # (VI), (6+), etc.
            re.IGNORECASE
        )
    
    def generate_all_variants(self, name: str) -> Set[str]:
        """
        Generate exhaustive variant set (20-50 variants) for a chemical name.
        
        Args:
            name: Chemical name to generate variants for
            
        Returns:
            Set of 20-50 unique variant strings
        """
        if not name or not name.strip():
            return set()
        
        variants = set()
        name = name.strip()
        
        # Parse components using the parser
        components = self.parser.parse(name)
        
        # Step 1: Classify and handle qualifiers
        base_name, qualifier, is_equivalent = self._classify_qualifier(name)
        
        if is_equivalent:
            # Equivalent qualifier: strip it and use base name only
            working_names = {base_name, base_name.lower(), base_name.title()}
        else:
            # Distinct qualifier or oxidation state: keep in variants
            working_names = {name, base_name}
            if qualifier:
                working_names.add(f"{base_name} {qualifier}")
                working_names.add(f"{base_name} ({qualifier})")
                working_names.add(f"{qualifier} {base_name}")
        
        # Step 2: Generate variants from components
        for working_name in list(working_names):
            # Parse this variant
            comp = self.parser.parse(working_name)
            
            # Add the working name itself
            variants.add(working_name)
            
            # Component recombination variants
            variants.update(self._generate_from_components(comp))
            
            # Locant position variants (only if locants are present)
            if comp.locants:
                variants.update(self._generate_locant_variants(working_name, comp))
            
            # Aromatic descriptor variants (only if aromatic position is present)
            if comp.aromatic_position or self._has_aromatic_descriptor(working_name):
                variants.update(self._generate_aromatic_variants(working_name, comp))
            
            # Stereochemistry variants (only if stereochemistry is present)
            if comp.stereochemistry:
                variants.update(self._generate_stereo_variants(working_name, comp))
        
        # Step 3: Generate hyphen/space/concatenation variants
        variants = self._generate_hyphen_variants(variants)
        
        # Step 4: Add selective case variations (not all 4 cases for every variant)
        case_variants = set()
        for v in variants:
            case_variants.add(v)
            case_variants.add(v.lower())
            # Only add title case for shorter names
            if len(v) < 30:
                case_variants.add(v.title())
        
        variants = case_variants
        
        # Step 5: Clean up and deduplicate
        cleaned = {v.strip() for v in variants if v and v.strip() and len(v.strip()) > 1}
        
        # Add original name
        cleaned.add(name)
        cleaned.add(name.lower())
        
        # Step 6: Filter spurious variants and cap at reasonable size
        filtered = self._filter_and_rank_variants(cleaned, name)
        
        return filtered
    
    def _filter_and_rank_variants(self, variants: Set[str], original_name: str) -> Set[str]:
        """
        Filter spurious variants and rank by quality, keeping best variants.
        
        Args:
            variants: Raw variant set
            original_name: Original chemical name
            
        Returns:
            Filtered and capped variant set
        """
        MAX_VARIANTS = 50
        
        # Filter out clearly spurious variants
        valid_variants = []
        spurious_patterns = [
            r'^[ompe]$',  # Single aromatic position letters
            r'^[ivxIVX]+$',  # Standalone Roman numerals
            r'\s[ompe]\s',  # Isolated aromatic letters in middle
        ]
        
        for v in variants:
            # Skip if matches spurious pattern
            is_spurious = any(re.search(pattern, v) for pattern in spurious_patterns)
            if is_spurious:
                continue
            
            # Skip if too short (unless it's a simple name)
            if len(v) < 3 and len(original_name) > 5:
                continue
            
            # Calculate quality score
            score = self._variant_quality_score(v, original_name)
            valid_variants.append((score, v))
        
        # Sort by score (descending) and take top variants
        valid_variants.sort(reverse=True, key=lambda x: x[0])
        
        # Always include original name at highest priority
        result = {original_name, original_name.lower()}
        
        # Add ranked variants up to limit
        for score, variant in valid_variants:
            if len(result) >= MAX_VARIANTS:
                break
            result.add(variant)
        
        return result
    
    def _variant_quality_score(self, variant: str, original: str) -> float:
        """
        Calculate quality score for a variant (higher is better).
        
        Args:
            variant: Variant string
            original: Original name
            
        Returns:
            Quality score
        """
        score = 0.0
        
        # Exact match bonus
        if variant.lower() == original.lower():
            score += 100.0
        
        # Length similarity bonus (prefer similar length)
        len_ratio = min(len(variant), len(original)) / max(len(variant), len(original))
        score += len_ratio * 10.0
        
        # Penalize very long variants
        if len(variant) > 40:
            score -= 5.0
        
        # Penalize variants with many spaces
        if variant.count(' ') > 3:
            score -= 2.0
        
        # Bonus for proper chemical structure indicators
        if re.search(r'\d+,\d+', variant):  # Has locants
            score += 3.0
        
        if re.search(r'(ortho|meta|para)', variant, re.IGNORECASE):  # Has aromatic
            score += 2.0
        
        # Penalize all-caps (less readable)
        if variant.isupper() and len(variant) > 5:
            score -= 1.0
        
        # Bonus for lowercase (most common form)
        if variant.islower():
            score += 1.0
        
        return score
    
    def _has_aromatic_descriptor(self, name: str) -> bool:
        """
        Check if name contains aromatic position descriptors.
        
        Args:
            name: Chemical name
            
        Returns:
            True if aromatic descriptors are present
        """
        name_lower = name.lower()
        aromatic_terms = ['ortho', 'meta', 'para', 'o-', 'm-', 'p-']
        return any(term in name_lower for term in aromatic_terms)
    
    def _classify_qualifier(self, name: str) -> Tuple[str, str, bool]:
        """
        Classify qualifier as equivalent or distinct.
        
        Args:
            name: Chemical name with potential qualifier
            
        Returns:
            Tuple of (base_name, qualifier, is_equivalent)
            - base_name: Name without qualifier
            - qualifier: Extracted qualifier text (empty if none)
            - is_equivalent: True if qualifier is equivalent (strip it), False if distinct (keep it)
        """
        # Check for oxidation states (Roman numerals or numeric)
        roman_match = self.roman_pattern.search(name)
        if roman_match:
            roman = roman_match.group(1).upper()
            if roman in self.ROMAN_NUMERALS:
                base = name[:roman_match.start()].strip()
                return (base, roman, False)  # Oxidation states are distinct
        
        oxidation_match = self.oxidation_pattern.search(name)
        if oxidation_match:
            base = self.oxidation_pattern.sub('', name).strip()
            qualifier = oxidation_match.group(1)
            return (base, qualifier, False)  # Oxidation states are distinct
        
        # Check for parenthetical or trailing qualifiers
        qualifier_match = self.qualifier_pattern.search(name)
        if qualifier_match:
            # Extract qualifier text
            qualifier = qualifier_match.group(1) or qualifier_match.group(2)
            qualifier = qualifier.strip().lower()
            
            # Remove qualifier from name to get base
            base = self.qualifier_pattern.sub('', name).strip()
            
            # Classify as equivalent or distinct
            is_equivalent = qualifier in [q.lower().strip('()') for q in self.EQUIVALENT_QUALIFIERS]
            
            return (base, qualifier, is_equivalent)
        
        # No qualifier found
        return (name, '', False)
    
    def _remove_qualifiers(self, name: str) -> str:
        """
        Remove all qualifier text from name.
        
        Args:
            name: Chemical name
            
        Returns:
            Name with qualifiers removed
        """
        # Remove parenthetical qualifiers
        name = self.qualifier_pattern.sub('', name)
        
        # Remove Roman numeral oxidation states
        name = self.roman_pattern.sub('', name)
        
        # Remove oxidation state notations
        name = self.oxidation_pattern.sub('', name)
        
        return name.strip()
    
    def _generate_from_components(self, components: ChemicalNameComponents) -> Set[str]:
        """
        Generate variants by recombining parsed components.
        
        Args:
            components: Parsed chemical name components
            
        Returns:
            Set of recombined variants
        """
        variants = set()
        
        if not components.parent_chain:
            # If no components parsed, return normalized form
            if components.normalized_form and len(components.normalized_form) > 2:
                variants.add(components.normalized_form)
            return variants
        
        # Build base parts
        parts = []
        
        if components.substituents:
            # Combine substituents
            subs_str = ''.join(components.substituents)
            
            # With multiplicity
            if components.multiplicity:
                parts.append(f"{components.multiplicity}-{subs_str}")
            else:
                parts.append(subs_str)
        
        if components.parent_chain:
            parts.append(components.parent_chain)
        
        if components.functional_group:
            parts.append(components.functional_group)
        elif components.bond_type:
            parts.append(components.bond_type)
        
        # Combine parts (only meaningful combinations)
        if parts and len(parts) >= 2:
            # Concatenated form
            variants.add(''.join(parts))
            # Hyphenated form
            variants.add('-'.join(parts))
        
        # Add locants if present (only to base forms, not all combinations)
        if components.locants and len(variants) > 0:
            locant_str = ','.join(str(n) for n in components.locants)
            base_variant = list(variants)[0]  # Pick one base form
            variants.add(f"{locant_str}-{base_variant}")
        
        return variants
    
    def _generate_locant_variants(self, name: str, components: ChemicalNameComponents) -> Set[str]:
        """
        Generate all locant position and separator variants.
        
        Args:
            name: Chemical name
            components: Parsed components
            
        Returns:
            Set of locant variants
        """
        variants = set()
        
        if not components.locants:
            return variants
        
        # Get base name without locants
        base = self.parser.locant_pattern.sub('', name)
        base = self.parser.trailing_locant_pattern.sub('', base).strip()
        
        # Generate key separator variants (not all combinations)
        key_seps = [',', '-', '']  # Reduced from all separators
        for sep in key_seps:
            locant_str = sep.join(str(n) for n in components.locants)
            
            # Leading locants
            variants.add(f"{locant_str}-{base}")
            if sep == ',':
                variants.add(f"{locant_str} {base}")  # Only add space variant for comma separator
            
            # Trailing locants (Ontario style)
            variants.add(f"{base} {locant_str}")
            if sep == '-':
                variants.add(f"{base}-{locant_str}")
        
        return variants
    
    def _generate_aromatic_variants(self, name: str, components: ChemicalNameComponents) -> Set[str]:
        """
        Generate aromatic position variants (ortho/meta/para vs 1,2/1,3/1,4).
        
        Args:
            name: Chemical name
            components: Parsed components
            
        Returns:
            Set of aromatic variants
        """
        variants = set()
        
        # Check if name contains aromatic descriptors
        name_lower = name.lower()
        aromatic_found = False
        
        for abbr in ['o-', 'm-', 'p-', 'ortho-', 'meta-', 'para-']:
            if abbr in name_lower:
                aromatic_found = True
                # Get base without descriptor
                base = re.sub(rf'{re.escape(abbr)}', '', name_lower, flags=re.IGNORECASE).strip()
                
                # Determine which position this is
                if abbr.startswith('o'):
                    numeric = '1,2'
                    descriptors = ['o', 'ortho']
                elif abbr.startswith('m'):
                    numeric = '1,3'
                    descriptors = ['m', 'meta']
                else:  # para
                    numeric = '1,4'
                    descriptors = ['p', 'para']
                
                # Generate key variants (not all combinations)
                for desc in descriptors:
                    variants.add(f"{desc}-{base}")
                    variants.add(f"{base} {desc}")
                
                # Add numeric form
                variants.add(f"{numeric}-{base}")
                variants.add(f"{base} {numeric}")
                
                break  # Only process first match
        
        # If parsed aromatic position and not already handled
        if components.aromatic_position and not aromatic_found:
            aromatic_desc = components.aromatic_position.lower()
            base = self.parser.aromatic_pattern.sub('', name).strip()
            numeric = self.parser.AROMATIC_POSITIONS.get(aromatic_desc, '')
            
            if numeric:
                # Add just the key variants
                variants.add(f"{aromatic_desc}-{base}")
                variants.add(f"{numeric}-{base}")
        
        return variants
    
    def _generate_stereo_variants(self, name: str, components: ChemicalNameComponents) -> Set[str]:
        """
        Generate stereochemistry variants (cis/trans/E/Z).
        
        Args:
            name: Chemical name
            components: Parsed components
            
        Returns:
            Set of stereochemistry variants
        """
        variants = set()
        
        if not components.stereochemistry:
            return variants
        
        # Get base name without stereochemistry
        base = self.parser.stereo_pattern.sub('', name).strip()
        
        # Parse stereochemistry descriptors
        stereo_list = components.stereochemistry.split(',')
        
        for stereo in stereo_list:
            stereo = stereo.strip()
            
            # Generate variants with different notations
            if stereo.lower() in ['cis', 'trans']:
                opposite = 'trans' if stereo.lower() == 'cis' else 'cis'
                
                # Leading
                variants.add(f"{stereo}-{base}")
                variants.add(f"({stereo})-{base}")
                
                # Trailing
                variants.add(f"{base} {stereo}")
                variants.add(f"{base} ({stereo})")
                
            elif stereo.upper() in ['E', 'Z']:
                # E/Z notation
                variants.add(f"({stereo})-{base}")
                variants.add(f"{stereo}-{base}")
                variants.add(f"{base} ({stereo})")
            
            elif stereo.upper() in ['R', 'S']:
                # R/S notation
                variants.add(f"({stereo})-{base}")
                variants.add(f"{stereo}-{base}")
                variants.add(f"{base} ({stereo})")
        
        # Add base without stereochemistry
        variants.add(base)
        
        return variants
    
    def _generate_hyphen_variants(self, variants: Set[str]) -> Set[str]:
        """
        Generate hyphen, space, and concatenation variants.
        
        Args:
            variants: Input variant set
            
        Returns:
            Expanded set with hyphen/space/concatenation variants
        """
        expanded = set(variants)
        
        for v in list(variants):
            # Only generate key hyphen/space variants
            if '-' in v:
                expanded.add(v.replace('-', ''))  # Remove hyphens
                # Only add space variant for short names
                if len(v) < 25:
                    expanded.add(v.replace('-', ' '))  # Replace with spaces
            
            if ' ' in v:
                expanded.add(v.replace(' ', ''))  # Remove spaces
                # Only add hyphen variant for names without multiple spaces
                if v.count(' ') <= 2:
                    expanded.add(v.replace(' ', '-'))  # Replace with hyphens
        
        return expanded
