"""Compatibility helpers for the ``bcrypt`` package.

The pre-built ``bcrypt`` wheels published for Python 3.13 currently miss the
``__about__`` module that older versions of :mod:`passlib` expect. When that
attribute is unavailable ``passlib`` raises an ``AttributeError`` during
application start up which prevents the API server and setup scripts from
running.

This module patches the imported :mod:`bcrypt` package so that
``bcrypt.__about__.__version__`` is always available. Import this module before
initialising the password hashing context to keep authentication working across
platforms.
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

if bcrypt is not None and not hasattr(bcrypt, "__about__"):
    try:
        version = getattr(bcrypt, "__version__", None) or metadata.version("bcrypt")
    except metadata.PackageNotFoundError:
        version = "unknown"

    bcrypt.__about__ = SimpleNamespace(__version__=version)  # type: ignore[attr-defined]
    logger.debug("Injected missing bcrypt.__about__.__version__ = %s", version)
