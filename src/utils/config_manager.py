"""
Configuration management for the matching system.

Handles loading, updating, and persisting configuration including
thresholds, learning parameters, and system settings.
"""

import logging
from pathlib import Path
from typing import Any, Optional
import yaml

logger = logging.getLogger(__name__)


class ConfigManager:
    """
    Manages system configuration including thresholds and learning parameters.
    
    Provides methods to load, update, and persist configuration with
    support for dynamic threshold adjustment.
    """
    
    DEFAULT_CONFIG = {
        'thresholds': {
            'auto_accept': 0.93,
            'review': 0.75,
            'unknown': 0.75,
            'disagreement_cap': 0.84,
            'exact_match': 1.0,
            'fuzzy_high': 0.95,
            'semantic_high': 0.90
        },
        'learning': {
            'retraining_trigger_count': 2000,
            'incremental_save_frequency': 100,
            'calibration_period_days': 30,
            'min_decisions_for_calibration': 100
        },
        'matching': {
            'fuzzy_algorithm': 'token_set_ratio',
            'semantic_model': 'all-MiniLM-L6-v2',
            'top_k_candidates': 5,
            'enable_cas_extraction': True
        },
        'clustering': {
            'similarity_threshold': 0.85,
            'min_cluster_size': 2
        },
        'database': {
            'batch_size': 1000,
            'connection_pool_size': 5
        }
    }
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize the configuration manager.
        
        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = config_path
        self.config: dict[str, Any] = {}
        
        if config_path and config_path.exists():
            self.load_config(config_path)
        else:
            logger.info("No config file found, using defaults")
            self.config = self._deep_copy_dict(self.DEFAULT_CONFIG)
    
    def load_config(self, path: Path) -> dict[str, Any]:
        """
        Load configuration from YAML file.
        
        Args:
            path: Path to configuration file
        
        Returns:
            Loaded configuration dictionary
        
        Raises:
            FileNotFoundError: If config file does not exist
            yaml.YAMLError: If config file is invalid
        """
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                loaded_config = yaml.safe_load(f)
            
            if not loaded_config:
                logger.warning(f"Empty config file at {path}, using defaults")
                self.config = self._deep_copy_dict(self.DEFAULT_CONFIG)
            else:
                # Merge with defaults to ensure all keys exist
                self.config = self._merge_with_defaults(loaded_config)
            
            self.config_path = path
            logger.info(f"Loaded configuration from {path}")
            
            return self.config
            
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse YAML config: {e}")
            raise
    
    def get_threshold(self, name: str) -> float:
        """
        Get a threshold value by name.
        
        Args:
            name: Threshold name (e.g., 'auto_accept', 'review')
        
        Returns:
            Threshold value
        
        Raises:
            KeyError: If threshold name not found
        """
        if name not in self.config.get('thresholds', {}):
            raise KeyError(f"Threshold '{name}' not found in configuration")
        
        return float(self.config['thresholds'][name])
    
    def update_threshold(self, name: str, value: float) -> None:
        """
        Update a threshold value.
        
        Args:
            name: Threshold name
            value: New threshold value (0-1)
        
        Raises:
            ValueError: If value is out of range
        """
        if not 0.0 <= value <= 1.0:
            raise ValueError(f"Threshold value must be between 0 and 1, got {value}")
        
        if 'thresholds' not in self.config:
            self.config['thresholds'] = {}
        
        old_value = self.config['thresholds'].get(name)
        self.config['thresholds'][name] = value
        
        logger.info(f"Updated threshold '{name}': {old_value} -> {value}")
    
    def update_thresholds_bulk(self, thresholds: dict[str, float]) -> None:
        """
        Update multiple thresholds at once.
        
        Args:
            thresholds: Dictionary of threshold name -> value pairs
        
        Raises:
            ValueError: If any value is out of range
        """
        for name, value in thresholds.items():
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"Threshold '{name}' value must be between 0 and 1, got {value}")
        
        if 'thresholds' not in self.config:
            self.config['thresholds'] = {}
        
        for name, value in thresholds.items():
            old_value = self.config['thresholds'].get(name)
            self.config['thresholds'][name] = value
            logger.debug(f"Updated threshold '{name}': {old_value} -> {value}")
        
        logger.info(f"Bulk updated {len(thresholds)} thresholds")
    
    def get_learning_param(self, name: str) -> Any:
        """
        Get a learning parameter by name.
        
        Args:
            name: Parameter name
        
        Returns:
            Parameter value
        
        Raises:
            KeyError: If parameter not found
        """
        if name not in self.config.get('learning', {}):
            raise KeyError(f"Learning parameter '{name}' not found in configuration")
        
        return self.config['learning'][name]
    
    def get_matching_param(self, name: str) -> Any:
        """
        Get a matching parameter by name.
        
        Args:
            name: Parameter name
        
        Returns:
            Parameter value
        
        Raises:
            KeyError: If parameter not found
        """
        if name not in self.config.get('matching', {}):
            raise KeyError(f"Matching parameter '{name}' not found in configuration")
        
        return self.config['matching'][name]
    
    def save_config(self, path: Optional[Path] = None) -> None:
        """
        Save configuration to YAML file.
        
        Args:
            path: Path to save to (uses self.config_path if not provided)
        
        Raises:
            ValueError: If no path provided and no config_path set
        """
        save_path = path or self.config_path
        
        if not save_path:
            raise ValueError("No path provided and no config_path set")
        
        try:
            # Ensure parent directory exists
            save_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(save_path, 'w', encoding='utf-8') as f:
                yaml.safe_dump(
                    self.config,
                    f,
                    default_flow_style=False,
                    sort_keys=False,
                    indent=2
                )
            
            logger.info(f"Saved configuration to {save_path}")
            
        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")
            raise
    
    def get_all_config(self) -> dict[str, Any]:
        """
        Get the complete configuration dictionary.
        
        Returns:
            Full configuration dictionary
        """
        return self._deep_copy_dict(self.config)
    
    def reset_to_defaults(self) -> None:
        """Reset configuration to default values."""
        self.config = self._deep_copy_dict(self.DEFAULT_CONFIG)
        logger.info("Configuration reset to defaults")
    
    def _merge_with_defaults(self, loaded_config: dict) -> dict:
        """Merge loaded config with defaults to ensure all keys exist."""
        merged = self._deep_copy_dict(self.DEFAULT_CONFIG)
        
        for section, values in loaded_config.items():
            if section in merged and isinstance(values, dict):
                merged[section].update(values)
            else:
                merged[section] = values
        
        return merged
    
    def _deep_copy_dict(self, d: dict) -> dict:
        """Deep copy a dictionary."""
        import copy
        return copy.deepcopy(d)
    
    def validate_config(self) -> list[str]:
        """
        Validate the current configuration.
        
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Validate thresholds
        thresholds = self.config.get('thresholds', {})
        for name, value in thresholds.items():
            if not isinstance(value, (int, float)):
                errors.append(f"Threshold '{name}' must be numeric, got {type(value)}")
            elif not 0.0 <= value <= 1.0:
                errors.append(f"Threshold '{name}' must be between 0 and 1, got {value}")
        
        # Validate learning parameters
        learning = self.config.get('learning', {})
        if 'retraining_trigger_count' in learning:
            if not isinstance(learning['retraining_trigger_count'], int) or learning['retraining_trigger_count'] < 1:
                errors.append("retraining_trigger_count must be a positive integer")
        
        if 'incremental_save_frequency' in learning:
            if not isinstance(learning['incremental_save_frequency'], int) or learning['incremental_save_frequency'] < 1:
                errors.append("incremental_save_frequency must be a positive integer")
        
        return errors
