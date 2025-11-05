"""Validation utilities for input data."""
import re
from typing import Optional


def validate_password_strength(password: str) -> Optional[str]:
    """
    Validate password meets complexity requirements.

    Password must contain:
    - At least 8 characters
    - At least 1 uppercase letter (A-Z)
    - At least 1 lowercase letter (a-z)
    - At least 1 digit (0-9)
    - At least 1 special character (!@#$%^&*(),.?":{}|<>)

    Args:
        password: The password to validate

    Returns:
        Error message if invalid, None if valid

    Examples:
        >>> validate_password_strength("weak")
        'Password must be at least 8 characters'

        >>> validate_password_strength("Admin123!@#")
        None
    """
    if len(password) < 8:
        return "Password must be at least 8 characters"

    if not re.search(r'[A-Z]', password):
        return "Password must contain at least one uppercase letter"

    if not re.search(r'[a-z]', password):
        return "Password must contain at least one lowercase letter"

    if not re.search(r'\d', password):
        return "Password must contain at least one digit"

    if not re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
        return "Password must contain at least one special character (!@#$%^&*(),.?\":{}|<>)"

    return None
