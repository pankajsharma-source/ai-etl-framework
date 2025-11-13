"""
Custom exceptions for the AI-ETL framework
"""


class ETLError(Exception):
    """Base exception for all ETL errors"""
    pass


class ConnectionError(ETLError):
    """Connection to source/destination failed"""
    pass


class SchemaError(ETLError):
    """Schema-related errors"""
    pass


class ReadError(ETLError):
    """Error reading from source"""
    pass


class WriteError(ETLError):
    """Error writing to destination"""
    pass


class TransformError(ETLError):
    """Error during transformation"""
    pass


class ConfigurationError(ETLError):
    """Invalid configuration"""
    pass


class StateError(ETLError):
    """State management error"""
    pass


class ValidationError(ETLError):
    """Data validation error"""
    pass


class PipelineError(ETLError):
    """Pipeline execution error"""
    pass


class StorageError(ETLError):
    """Storage operation error"""
    pass
