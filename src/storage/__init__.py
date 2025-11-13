"""
Storage module for intermediate data persistence
"""
from src.storage.base import IntermediateStorage
from src.storage.file_storage import FileStorage

__all__ = ['IntermediateStorage', 'FileStorage']
