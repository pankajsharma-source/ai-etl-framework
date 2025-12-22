"""
Authentication module for AI ETL Framework
Handles password hashing and JWT token management
"""
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from passlib.hash import argon2
from jose import JWTError, jwt

# Configuration
SECRET_KEY = os.getenv('JWT_SECRET_KEY', 'dev-secret-key-change-in-production')
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 24
MAX_PASSWORD_LENGTH = 128  # Characters


def hash_password(password: str) -> str:
    """Hash a password using argon2 (modern, no length limits)"""
    return argon2.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash"""
    try:
        return argon2.verify(plain_password, hashed_password)
    except Exception:
        return False


def validate_password(password: str) -> tuple:
    """
    Validate password meets requirements.
    Returns (is_valid, error_message)
    """
    if len(password) < 6:
        return False, "Password must be at least 6 characters"
    if len(password) > MAX_PASSWORD_LENGTH:
        return False, f"Password must be less than {MAX_PASSWORD_LENGTH} characters"
    return True, ""


def create_access_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    """
    Create a JWT access token

    Args:
        data: Payload to encode in the token
        expires_delta: Optional custom expiration time

    Returns:
        Encoded JWT token string
    """
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def verify_token(token: str) -> Optional[Dict[str, Any]]:
    """
    Verify and decode a JWT token

    Args:
        token: JWT token string

    Returns:
        Decoded payload if valid, None if invalid/expired
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None


def extract_token_from_header(authorization: Optional[str]) -> Optional[str]:
    """
    Extract JWT token from Authorization header

    Args:
        authorization: Authorization header value (e.g., "Bearer <token>")

    Returns:
        Token string if found, None otherwise
    """
    if not authorization:
        return None

    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != 'bearer':
        return None

    return parts[1]


def generate_slug(name: str) -> str:
    """
    Generate a URL-friendly slug from a name

    Args:
        name: Organization or user name

    Returns:
        Lowercase slug with spaces replaced by hyphens
    """
    import re
    # Convert to lowercase, replace spaces with hyphens
    slug = name.lower().strip()
    # Remove special characters, keep only alphanumeric and hyphens
    slug = re.sub(r'[^a-z0-9\s-]', '', slug)
    # Replace spaces with hyphens
    slug = re.sub(r'\s+', '-', slug)
    # Remove multiple consecutive hyphens
    slug = re.sub(r'-+', '-', slug)
    # Remove leading/trailing hyphens
    slug = slug.strip('-')
    return slug or 'organization'
