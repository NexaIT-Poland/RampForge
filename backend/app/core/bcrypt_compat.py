"""Compatibility helpers for the ``bcrypt`` package.

The pre-built ``bcrypt`` wheels published for Python 3.13 currently miss the
``__about__`` module that older versions of :mod:`passlib` expect. When that
attribute is unavailable ``passlib`` raises an ``AttributeError`` during
application start up which prevents the API server and setup scripts from
running.

This module patches the imported :mod:`bcrypt` package so that
``bcrypt.__about__.__version__`` is always available and mirrors the legacy
behaviour of silently truncating passwords longer than 72 bytes. Import this
module before initialising the password hashing context to keep authentication
working across platforms.
"""
from __future__ import annotations

from importlib import metadata
from types import SimpleNamespace
import logging

try:
    import bcrypt
except Exception:  # pragma: no cover - bcrypt is an optional dependency at runtime
    bcrypt = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

if bcrypt is not None:
    if not hasattr(bcrypt, "__about__"):
        try:
            version = getattr(bcrypt, "__version__", None) or metadata.version("bcrypt")
        except metadata.PackageNotFoundError:
            version = "unknown"

        bcrypt.__about__ = SimpleNamespace(__version__=version)  # type: ignore[attr-defined]
        logger.debug("Injected missing bcrypt.__about__.__version__ = %s", version)

    _original_hashpw = getattr(bcrypt, "hashpw", None)

    if callable(_original_hashpw):
        def _hashpw_with_truncation(password: bytes, salt: bytes) -> bytes:
            """Mirror legacy behaviour for long passwords so passlib stays compatible."""
            try:
                return _original_hashpw(password, salt)
            except ValueError as exc:  # pragma: no cover - only triggered on buggy wheels
                message = str(exc)
                if "password cannot be longer than 72 bytes" in message:
                    logger.debug("Truncating bcrypt password to 72 bytes for compatibility")
                    return _original_hashpw(password[:72], salt)
                raise

        bcrypt.hashpw = _hashpw_with_truncation  # type: ignore[assignment]
