"""Rate limiter configuration."""
from slowapi import Limiter
from slowapi.util import get_remote_address

# Global rate limiter instance
# Used across the application for rate limiting endpoints
limiter = Limiter(key_func=get_remote_address)
