"""
Configuration management for AI-ETL framework
"""
import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from dotenv import load_dotenv

from src.common.exceptions import ConfigurationError


class Config:
    """Configuration manager"""

    def __init__(self, config_file: Optional[str] = None, env_file: Optional[str] = None):
        """
        Initialize configuration

        Args:
            config_file: Path to YAML configuration file
            env_file: Path to .env file
        """
        # Load environment variables
        if env_file:
            load_dotenv(env_file)
        else:
            load_dotenv()  # Load from .env in current directory

        # Load YAML configuration
        self._config = {}
        if config_file:
            self._load_yaml(config_file)

    def _load_yaml(self, config_file: str) -> None:
        """Load configuration from YAML file"""
        try:
            with open(config_file, 'r') as f:
                self._config = yaml.safe_load(f) or {}
        except FileNotFoundError:
            raise ConfigurationError(f"Config file not found: {config_file}")
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Error parsing YAML: {e}")

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value

        Supports dot notation for nested values (e.g., 'execution.batch_size')
        Checks environment variables first, then YAML config

        Args:
            key: Configuration key
            default: Default value if key not found

        Returns:
            Configuration value
        """
        # Check environment variable first (with uppercase and underscores)
        env_key = key.upper().replace('.', '_')
        env_value = os.getenv(env_key)
        if env_value is not None:
            return env_value

        # Check YAML config with dot notation support
        keys = key.split('.')
        value = self._config
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    def get_int(self, key: str, default: int = 0) -> int:
        """Get integer configuration value"""
        value = self.get(key, default)
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def get_float(self, key: str, default: float = 0.0) -> float:
        """Get float configuration value"""
        value = self.get(key, default)
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def get_bool(self, key: str, default: bool = False) -> bool:
        """Get boolean configuration value"""
        value = self.get(key, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'on')
        return default

    def get_all(self) -> Dict[str, Any]:
        """Get all configuration as dictionary"""
        return self._config.copy()


# Global configuration instance
_global_config: Optional[Config] = None


def init_config(config_file: Optional[str] = None, env_file: Optional[str] = None) -> Config:
    """
    Initialize global configuration

    Args:
        config_file: Path to YAML configuration file
        env_file: Path to .env file

    Returns:
        Config instance
    """
    global _global_config
    _global_config = Config(config_file, env_file)
    return _global_config


def get_config() -> Config:
    """
    Get global configuration instance

    Returns:
        Config instance

    Raises:
        ConfigurationError: If config not initialized
    """
    if _global_config is None:
        # Auto-initialize with defaults
        return init_config()
    return _global_config
