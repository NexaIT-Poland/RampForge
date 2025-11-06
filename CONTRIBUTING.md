# Contributing to RampForge

Thank you for considering contributing to RampForge! 🎉

## 🤝 How to Contribute

We welcome contributions from the community. Here are some ways you can help:

### 🐛 Reporting Bugs

If you find a bug, please create an issue with:
- Clear title and description
- Steps to reproduce the bug
- Expected vs actual behavior
- Your environment (OS, Python version, etc.)
- Screenshots if applicable

### 💡 Suggesting Features

Have an idea? Open an issue with:
- Clear description of the feature
- Use case and benefits
- Any implementation thoughts

### 🔧 Pull Requests

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/amazing-feature`
3. **Make your changes**
4. **Test your changes**: Run tests and ensure everything works
5. **Commit your changes**: Use clear commit messages
6. **Push to your fork**: `git push origin feature/amazing-feature`
7. **Open a Pull Request**

## 📝 Development Guidelines

### Code Style

**Python:**
- Follow PEP 8
- Use type hints
- Add docstrings to functions and classes
- Keep functions focused and small

**Backend:**
- Use async/await for database operations
- Add proper error handling
- Write tests for new features

**Frontend (TUI):**
- Follow Textual best practices
- Keep UI responsive
- Test with different terminal sizes

### Commit Messages

Use clear, descriptive commit messages:

```
feat: Add dock sorting by priority
fix: Resolve WebSocket reconnection issue
docs: Update deployment guide
refactor: Simplify authentication logic
test: Add unit tests for ramp service
```

### Testing

Before submitting a PR:

```bash
# Backend tests
cd backend
pytest

# Type checking
mypy app/

# Linting
ruff check app/
```

### Documentation

- Update README.md if needed
- Update relevant documentation in `docs/`
- Add inline comments for complex logic
- Update API documentation if adding endpoints

## 🏗️ Project Structure

```
RampForge/
├── backend/           # FastAPI backend
│   ├── app/
│   │   ├── api/      # API endpoints
│   │   ├── core/     # Config, security
│   │   ├── db/       # Database models
│   │   └── services/ # Business logic
│   └── tests/        # Backend tests
├── client_tui/       # Textual TUI client
│   ├── app/
│   │   ├── screens/  # TUI screens
│   │   └── services/ # API client
│   └── tests/        # TUI tests
└── docs/             # Documentation
```

## 🎯 Areas Where We Need Help

- **Testing**: More unit and integration tests
- **Documentation**: Improving guides and API docs
- **Translations**: Translating docs to other languages
- **UI/UX**: Improving the TUI interface
- **Performance**: Optimizing database queries
- **Features**: See [GitHub Issues](https://github.com/TMMCx2/RampForge/issues) for ideas

## 🚀 Development Setup

```bash
# Clone repository
git clone https://github.com/TMMCx2/RampForge.git
cd RampForge

# Run setup
./setup.sh

# Start backend (Terminal 1)
./start_backend.sh

# Start client (Terminal 2)
./start_client.sh
```

## 📧 Questions?

- **Email**: office@nexait.pl
- **Issues**: https://github.com/TMMCx2/RampForge/issues
- **Discussions**: https://github.com/TMMCx2/RampForge/discussions

## 📜 License

By contributing, you agree that your contributions will be licensed under the MIT License with Commons Clause.

## 🙏 Thank You!

Every contribution, no matter how small, helps make RampForge better. We appreciate your time and effort!

---

**Made with ❤️ by NEXAIT sp. z o.o.**
