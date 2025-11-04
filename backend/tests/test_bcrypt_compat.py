"""Tests for the bcrypt compatibility shim."""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

import app.core.bcrypt_compat as bcrypt_compat


@pytest.mark.skipif(bcrypt_compat.bcrypt is None, reason="bcrypt is not installed")
def test_missing_about_module_is_restored(monkeypatch):
    """Reloading the shim should recreate ``bcrypt.__about__`` when it is absent."""
    bcrypt = bcrypt_compat.bcrypt
    assert bcrypt is not None  # for type checking

    # Simulate the broken wheel by removing the attribute before reloading.
    monkeypatch.delattr(bcrypt, "__about__", raising=False)
    assert not hasattr(bcrypt, "__about__")

    reloaded = importlib.reload(bcrypt_compat)

    assert reloaded.bcrypt is bcrypt
    assert hasattr(bcrypt, "__about__")
    assert hasattr(bcrypt.__about__, "__version__")
    assert isinstance(bcrypt.__about__.__version__, str)
