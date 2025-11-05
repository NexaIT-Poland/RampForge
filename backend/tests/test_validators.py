"""Tests for validation utilities."""
import pytest

from app.core.validators import validate_password_strength


class TestPasswordValidation:
    """Test password strength validation."""

    def test_valid_password(self):
        """Test that valid passwords pass validation."""
        valid_passwords = [
            "Admin123!@#",
            "Operator123!@#",
            "MyP@ssw0rd",
            "Complex1!Pass",
            "Str0ng!Password",
            "Test1234!@#$",
        ]

        for password in valid_passwords:
            result = validate_password_strength(password)
            assert result is None, f"Password '{password}' should be valid but got error: {result}"

    def test_password_too_short(self):
        """Test that passwords shorter than 8 characters are rejected."""
        short_passwords = [
            "Ab1!",
            "Test1!",
            "Abc123!",
        ]

        for password in short_passwords:
            result = validate_password_strength(password)
            assert result is not None
            assert "8 characters" in result

    def test_password_missing_uppercase(self):
        """Test that passwords without uppercase letters are rejected."""
        passwords = [
            "password123!",
            "test1234!@#",
            "myp@ssw0rd",
        ]

        for password in passwords:
            result = validate_password_strength(password)
            assert result is not None
            assert "uppercase" in result.lower()

    def test_password_missing_lowercase(self):
        """Test that passwords without lowercase letters are rejected."""
        passwords = [
            "PASSWORD123!",
            "TEST1234!@#",
            "MYP@SSW0RD",
        ]

        for password in passwords:
            result = validate_password_strength(password)
            assert result is not None
            assert "lowercase" in result.lower()

    def test_password_missing_digit(self):
        """Test that passwords without digits are rejected."""
        passwords = [
            "Password!@#",
            "TestPass!@#",
            "MyP@ssword",
        ]

        for password in passwords:
            result = validate_password_strength(password)
            assert result is not None
            assert "digit" in result.lower()

    def test_password_missing_special_char(self):
        """Test that passwords without special characters are rejected."""
        passwords = [
            "Password123",
            "TestPass1234",
            "MyPassword1",
        ]

        for password in passwords:
            result = validate_password_strength(password)
            assert result is not None
            assert "special character" in result.lower()

    def test_password_all_special_chars_accepted(self):
        """Test that all documented special characters are accepted."""
        special_chars = "!@#$%^&*(),.?\":{}|<>"

        for char in special_chars:
            password = f"Test123{char}"
            result = validate_password_strength(password)
            assert result is None, f"Password with special char '{char}' should be valid but got: {result}"

    def test_password_whitespace_not_special_char(self):
        """Test that whitespace is not considered a special character."""
        passwords = [
            "Password 123",
            "Test Pass123",
            "MyPass word1",
        ]

        for password in passwords:
            result = validate_password_strength(password)
            assert result is not None
            assert "special character" in result.lower()
