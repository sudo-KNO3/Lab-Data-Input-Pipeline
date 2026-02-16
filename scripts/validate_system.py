"""
System validation script for Reg 153 Chemical Matcher.

Comprehensive validation of:
- Database integrity
- Configuration validity
- Model files and embeddings
- Matching against known good examples
- Performance benchmarks
- Generate validation report
"""

import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Tuple
import argparse

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.database.models import Base, Analyte, Synonym
from src.database import crud_new as crud
from src.normalization.text_normalizer import TextNormalizer
from src.normalization.cas_extractor import CASExtractor
from src.matching.exact_matcher import ExactMatcher
from src.matching.fuzzy_matcher import FuzzyMatcher
from src.matching.resolution_engine import ResolutionEngine


# ============================================================================
# VALIDATION CONSTANTS
# ============================================================================

KNOWN_GOOD_EXAMPLES = [
    # Exact matches
    {'input': 'Benzene', 'expected_id': 'REG153_VOCS_001', 'expected_confidence': 1.0},
    {'input': 'Toluene', 'expected_id': 'REG153_VOCS_002', 'expected_confidence': 1.0},
    {'input': 'Ethylbenzene', 'expected_id': 'REG153_VOCS_003', 'expected_confidence': 1.0},
    
    # CAS numbers
    {'input': '71-43-2', 'expected_id': 'REG153_VOCS_001', 'expected_confidence': 1.0},
    {'input': '108-88-3', 'expected_id': 'REG153_VOCS_002', 'expected_confidence': 1.0},
    
    # PHC fractions
    {'input': 'PHC F1', 'expected_id': 'REG153_PHCS_001', 'expected_confidence': 1.0},
    {'input': 'PHC F2', 'expected_id': 'REG153_PHCS_002', 'expected_confidence': 1.0},
    
    # Metals
    {'input': 'Arsenic', 'expected_id': 'REG153_METALS_001', 'expected_confidence': 1.0},
    {'input': 'Lead', 'expected_id': 'REG153_METALS_002', 'expected_confidence': 1.0},
]


# ============================================================================
# VALIDATION RESULT CLASS
# ============================================================================

class ValidationResult:
    """Container for validation results."""
    
    def __init__(self):
        self.timestamp = datetime.now()
        self.checks: Dict[str, Dict[str, Any]] = {}
        self.overall_status = "UNKNOWN"
        self.errors: List[str] = []
        self.warnings: List[str] = []
    
    def add_check(self, name: str, passed: bool, details: Dict[str, Any] = None):
        """Add a validation check result."""
        self.checks[name] = {
            'passed': passed,
            'details': details or {},
            'timestamp': datetime.now().isoformat()
        }
    
    def add_error(self, message: str):
        """Add an error message."""
        self.errors.append(message)
    
    def add_warning(self, message: str):
        """Add a warning message."""
        self.warnings.append(message)
    
    def finalize(self):
        """Finalize validation and determine overall status."""
        if self.errors:
            self.overall_status = "FAILED"
        elif self.warnings:
            self.overall_status = "WARNING"
        elif all(check['passed'] for check in self.checks.values()):
            self.overall_status = "PASSED"
        else:
            self.overall_status = "FAILED"


# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

def validate_database_connection(db_path: str) -> Tuple[bool, Dict[str, Any]]:
    """
    Validate database connection and structure.
    
    Returns:
        (success, details_dict)
    """
    try:
        engine = create_engine(f"sqlite:///{db_path}")
        
        # Test connection
        with engine.connect() as conn:
            pass
        
        # Check if tables exist
        from sqlalchemy import inspect
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        expected_tables = ['analytes', 'synonyms', 'lab_variants', 'match_decisions']
        missing_tables = [t for t in expected_tables if t not in tables]
        
        if missing_tables:
            return False, {
                'error': f"Missing tables: {missing_tables}",
                'existing_tables': tables
            }
        
        # Get row counts
        Session = sessionmaker(bind=engine)
        session = Session()
        
        analyte_count = session.query(Analyte).count()
        synonym_count = session.query(Synonym).count()
        
        session.close()
        engine.dispose()
        
        return True, {
            'tables': tables,
            'analyte_count': analyte_count,
            'synonym_count': synonym_count,
        }
    
    except Exception as e:
        return False, {'error': str(e)}


def validate_configuration(config_path: str = None) -> Tuple[bool, Dict[str, Any]]:
    """
    Validate configuration files.
    
    Returns:
        (success, details_dict)
    """
    try:
        # Check for config files
        config_dir = project_root / 'config'
        
        if not config_dir.exists():
            return False, {'error': 'Config directory not found'}
        
        config_files = list(config_dir.glob('*.yaml'))
        
        if not config_files:
            return False, {'error': 'No config files found'}
        
        # Try to load a config file
        try:
            import yaml
            
            sample_config = config_files[0]
            with open(sample_config, 'r') as f:
                config_data = yaml.safe_load(f)
            
            return True, {
                'config_files': [f.name for f in config_files],
                'sample_config': config_data
            }
        
        except Exception as e:
            return False, {'error': f"Failed to load config: {e}"}
    
    except Exception as e:
        return False, {'error': str(e)}


def validate_model_files() -> Tuple[bool, Dict[str, Any]]:
    """
    Validate model files and embeddings.
    
    Returns:
        (success, details_dict)
    """
    try:
        models_dir = project_root / 'models'
        embeddings_dir = project_root / 'data' / 'embeddings'
        
        details = {
            'models_dir_exists': models_dir.exists(),
            'embeddings_dir_exists': embeddings_dir.exists(),
        }
        
        if models_dir.exists():
            model_files = list(models_dir.glob('*'))
            details['model_files'] = [f.name for f in model_files if f.is_file()]
        
        if embeddings_dir.exists():
            embedding_files = list(embeddings_dir.glob('*'))
            details['embedding_files'] = [f.name for f in embedding_files if f.is_file()]
        
        # Models directory should exist (may be empty initially)
        if not models_dir.exists():
            return False, details
        
        return True, details
    
    except Exception as e:
        return False, {'error': str(e)}


def validate_matching_engine(db_session) -> Tuple[bool, Dict[str, Any]]:
    """
    Validate matching engine against known good examples.
    
    Returns:
        (success, details_dict)
    """
    try:
        # Initialize matching components
        normalizer = TextNormalizer()
        cas_extractor = CASExtractor()
        exact_matcher = ExactMatcher(normalizer, cas_extractor)
        fuzzy_matcher = FuzzyMatcher(normalizer)
        resolution_engine = ResolutionEngine(
            db_session,
            normalizer,
            cas_extractor,
            exact_matcher,
            fuzzy_matcher
        )
        
        # Test known good examples
        results = []
        passed = 0
        failed = 0
        
        for example in KNOWN_GOOD_EXAMPLES:
            result = resolution_engine.resolve(example['input'])
            
            test_passed = (
                result.is_resolved and
                result.best_match.analyte_id == example['expected_id']
            )
            
            results.append({
                'input': example['input'],
                'expected': example['expected_id'],
                'actual': result.best_match.analyte_id if result.is_resolved else None,
                'confidence': result.confidence if result.is_resolved else 0.0,
                'passed': test_passed
            })
            
            if test_passed:
                passed += 1
            else:
                failed += 1
        
        overall_passed = failed == 0
        
        return overall_passed, {
            'passed': passed,
            'failed': failed,
            'total': len(KNOWN_GOOD_EXAMPLES),
            'pass_rate': passed / len(KNOWN_GOOD_EXAMPLES) * 100,
            'results': results
        }
    
    except Exception as e:
        return False, {'error': str(e)}


def validate_performance(db_session) -> Tuple[bool, Dict[str, Any]]:
    """
    Run performance benchmarks.
    
    Returns:
        (success, details_dict)
    """
    import time
    
    try:
        # Initialize components
        normalizer = TextNormalizer()
        cas_extractor = CASExtractor()
        exact_matcher = ExactMatcher(normalizer, cas_extractor)
        resolution_engine = ResolutionEngine(db_session, normalizer, cas_extractor, exact_matcher)
        
        # Benchmark 1: Exact matching
        iterations = 100
        start = time.perf_counter()
        for _ in range(iterations):
            exact_matcher.match("Benzene", db_session)
        exact_time_ms = (time.perf_counter() - start) * 1000 / iterations
        
        # Benchmark 2: Resolution
        start = time.perf_counter()
        for _ in range(iterations):
            resolution_engine.resolve("Benzene")
        resolution_time_ms = (time.perf_counter() - start) * 1000 / iterations
        
        # Benchmark 3: Batch processing
        batch_inputs = ["Benzene", "Toluene", "Ethylbenzene", "Lead"] * 25  # 100 items
        start = time.perf_counter()
        for text in batch_inputs:
            resolution_engine.resolve(text)
        batch_time_ms = (time.perf_counter() - start) * 1000
        batch_avg_ms = batch_time_ms / len(batch_inputs)
        
        # Check against targets
        exact_ok = exact_time_ms < 10
        resolution_ok = resolution_time_ms < 20
        batch_ok = batch_avg_ms < 50
        
        overall_passed = exact_ok and resolution_ok and batch_ok
        
        return overall_passed, {
            'exact_match_ms': round(exact_time_ms, 2),
            'exact_target_ok': exact_ok,
            'resolution_ms': round(resolution_time_ms, 2),
            'resolution_target_ok': resolution_ok,
            'batch_avg_ms': round(batch_avg_ms, 2),
            'batch_target_ok': batch_ok,
        }
    
    except Exception as e:
        return False, {'error': str(e)}


def validate_data_integrity(db_session) -> Tuple[bool, Dict[str, Any]]:
    """
    Validate data integrity in database.
    
    Returns:
        (success, details_dict)
    """
    try:
        # Check for orphaned synonyms
        orphaned = db_session.query(Synonym).filter(
            ~Synonym.analyte_id.in_(
                db_session.query(Analyte.analyte_id)
            )
        ).count()
        
        # Check for analytes without synonyms
        analytes_no_synonyms = db_session.query(Analyte).filter(
            ~Analyte.analyte_id.in_(
                db_session.query(Synonym.analyte_id).distinct()
            )
        ).count()
        
        # Get analyte counts by type
        from sqlalchemy import func
        from src.database.models import AnalyteType
        
        type_counts = db_session.query(
            Analyte.analyte_type,
            func.count(Analyte.analyte_id)
        ).group_by(Analyte.analyte_type).all()
        
        # Basic integrity checks
        issues = []
        if orphaned > 0:
            issues.append(f"{orphaned} orphaned synonyms")
        
        warnings = []
        if analytes_no_synonyms > 0:
            warnings.append(f"{analytes_no_synonyms} analytes without synonyms")
        
        overall_passed = len(issues) == 0
        
        return overall_passed, {
            'orphaned_synonyms': orphaned,
            'analytes_no_synonyms': analytes_no_synonyms,
            'type_counts': {str(t): c for t, c in type_counts},
            'issues': issues,
            'warnings': warnings,
        }
    
    except Exception as e:
        return False, {'error': str(e)}


# ============================================================================
# MAIN VALIDATION FUNCTION
# ============================================================================

def run_validation(db_path: str = None, verbose: bool = True) -> ValidationResult:
    """
    Run comprehensive system validation.
    
    Args:
        db_path: Path to database file (default: data/processed/canonical/master.db)
        verbose: Print detailed output
    
    Returns:
        ValidationResult object
    """
    result = ValidationResult()
    
    # Default database path
    if db_path is None:
        db_path = project_root / 'data' / 'processed' / 'canonical' / 'master.db'
    
    if verbose:
        print("="*70)
        print("REG 153 CHEMICAL MATCHER - SYSTEM VALIDATION")
        print("="*70)
        print(f"\nTimestamp: {result.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Database: {db_path}")
        print()
    
    # 1. Database Connection
    if verbose:
        print("1. Validating database connection...")
    
    db_ok, db_details = validate_database_connection(str(db_path))
    result.add_check('database_connection', db_ok, db_details)
    
    if verbose:
        if db_ok:
            print(f"   ✓ Database OK ({db_details.get('analyte_count', 0)} analytes, "
                  f"{db_details.get('synonym_count', 0)} synonyms)")
        else:
            print(f"   ✗ Database FAILED: {db_details.get('error', 'Unknown error')}")
            result.add_error(f"Database validation failed: {db_details.get('error')}")
    
    # 2. Configuration
    if verbose:
        print("\n2. Validating configuration...")
    
    config_ok, config_details = validate_configuration()
    result.add_check('configuration', config_ok, config_details)
    
    if verbose:
        if config_ok:
            print(f"   ✓ Configuration OK ({len(config_details.get('config_files', []))} files)")
        else:
            print(f"   ✗ Configuration FAILED: {config_details.get('error', 'Unknown error')}")
            result.add_warning(f"Configuration issue: {config_details.get('error')}")
    
    # 3. Model Files
    if verbose:
        print("\n3. Validating model files...")
    
    models_ok, models_details = validate_model_files()
    result.add_check('model_files', models_ok, models_details)
    
    if verbose:
        if models_ok:
            print(f"   ✓ Models directory OK")
        else:
            print(f"   ⚠ Models directory not found (may be empty for new installation)")
            result.add_warning("Models directory not found")
    
    # Continue only if database is OK
    if db_ok:
        # Create database session
        engine = create_engine(f"sqlite:///{db_path}")
        Session = sessionmaker(bind=engine)
        db_session = Session()
        
        # 4. Data Integrity
        if verbose:
            print("\n4. Validating data integrity...")
        
        integrity_ok, integrity_details = validate_data_integrity(db_session)
        result.add_check('data_integrity', integrity_ok, integrity_details)
        
        if verbose:
            if integrity_ok:
                print(f"   ✓ Data integrity OK")
            else:
                print(f"   ✗ Data integrity issues found:")
                for issue in integrity_details.get('issues', []):
                    print(f"      - {issue}")
                    result.add_error(f"Data integrity: {issue}")
            
            for warning in integrity_details.get('warnings', []):
                print(f"   ⚠ {warning}")
                result.add_warning(f"Data: {warning}")
        
        # 5. Matching Engine
        if verbose:
            print("\n5. Validating matching engine...")
        
        matching_ok, matching_details = validate_matching_engine(db_session)
        result.add_check('matching_engine', matching_ok, matching_details)
        
        if verbose:
            if matching_ok:
                print(f"   ✓ Matching engine OK "
                      f"({matching_details['passed']}/{matching_details['total']} tests passed)")
            else:
                print(f"   ✗ Matching engine FAILED "
                      f"({matching_details['passed']}/{matching_details['total']} tests passed)")
                for test_result in matching_details.get('results', []):
                    if not test_result['passed']:
                        print(f"      - {test_result['input']}: expected {test_result['expected']}, "
                              f"got {test_result['actual']}")
                        result.add_error(f"Matching: {test_result['input']} failed")
        
        # 6. Performance Benchmarks
        if verbose:
            print("\n6. Running performance benchmarks...")
        
        perf_ok, perf_details = validate_performance(db_session)
        result.add_check('performance', perf_ok, perf_details)
        
        if verbose:
            if perf_ok:
                print(f"   ✓ Performance OK")
            else:
                print(f"   ⚠ Performance below targets:")
            
            print(f"      Exact match: {perf_details['exact_match_ms']}ms "
                  f"({'✓' if perf_details['exact_target_ok'] else '✗'} < 10ms)")
            print(f"      Resolution: {perf_details['resolution_ms']}ms "
                  f"({'✓' if perf_details['resolution_target_ok'] else '✗'} < 20ms)")
            print(f"      Batch avg: {perf_details['batch_avg_ms']}ms "
                  f"({'✓' if perf_details['batch_target_ok'] else '✗'} < 50ms)")
            
            if not perf_ok:
                result.add_warning("Performance below targets")
        
        # Cleanup
        db_session.close()
        engine.dispose()
    
    else:
        result.add_error("Skipping further tests due to database connection failure")
    
    # Finalize
    result.finalize()
    
    if verbose:
        print("\n" + "="*70)
        print(f"OVERALL STATUS: {result.overall_status}")
        print("="*70)
        
        if result.errors:
            print(f"\nErrors ({len(result.errors)}):")
            for error in result.errors:
                print(f"  ✗ {error}")
        
        if result.warnings:
            print(f"\nWarnings ({len(result.warnings)}):")
            for warning in result.warnings:
                print(f"  ⚠ {warning}")
        
        print()
    
    return result


def generate_report(result: ValidationResult, output_path: str = None):
    """
    Generate validation report file.
    
    Args:
        result: ValidationResult object
        output_path: Output file path (default: reports/validation_report.txt)
    """
    if output_path is None:
        output_path = project_root / 'reports' / 'validation_report.txt'
    
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        f.write("="*70 + "\n")
        f.write("REG 153 CHEMICAL MATCHER - VALIDATION REPORT\n")
        f.write("="*70 + "\n\n")
        
        f.write(f"Generated: {result.timestamp.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Overall Status: {result.overall_status}\n\n")
        
        f.write("VALIDATION CHECKS:\n")
        f.write("-"*70 + "\n")
        
        for check_name, check_data in result.checks.items():
            status = "PASSED" if check_data['passed'] else "FAILED"
            f.write(f"\n{check_name.upper()}: {status}\n")
            
            if check_data['details']:
                for key, value in check_data['details'].items():
                    if key != 'error' and key !='results':
                        f.write(f"  {key}: {value}\n")
        
        if result.errors:
            f.write("\n" + "-"*70 + "\n")
            f.write("ERRORS:\n")
            for error in result.errors:
                f.write(f"  - {error}\n")
        
        if result.warnings:
            f.write("\n" + "-"*70 + "\n")
            f.write("WARNINGS:\n")
            for warning in result.warnings:
                f.write(f"  - {warning}\n")
        
        f.write("\n" + "="*70 + "\n")
    
    print(f"\nValidation report saved to: {output_path}")


# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================

def main():
    """Main entry point for validation script."""
    parser = argparse.ArgumentParser(
        description="Validate Reg 153 Chemical Matcher system"
    )
    parser.add_argument(
        '--db',
        type=str,
        help='Path to database file',
        default=None
    )
    parser.add_argument(
        '--report',
        type=str,
        help='Output path for validation report',
        default=None
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Suppress detailed output'
    )
    
    args = parser.parse_args()
    
    # Run validation
    result = run_validation(db_path=args.db, verbose=not args.quiet)
    
    # Generate report
    if args.report or result.overall_status != "PASSED":
        generate_report(result, args.report)
    
    # Exit with appropriate code
    if result.overall_status == "PASSED":
        return 0
    elif result.overall_status == "WARNING":
        return 1
    else:
        return 2


if __name__ == "__main__":
    sys.exit(main())
