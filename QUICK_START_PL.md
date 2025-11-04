# DCDock - Szybki Start (Polski)

Prosty przewodnik uruchomienia aplikacji DCDock dla początkujących.

## Czym jest DCDock?

DCDock to aplikacja do zarządzania rampami załadunkowymi w centrum dystrybucyjnym. Działa w terminalu (TUI) i pozwala wielu osobom jednocześnie śledzić i aktualizować status załadunków.

## Wymagania

- **Python 3.11 lub nowszy** (zalecany Python 3.13)
- Komputer z systemem: macOS, Linux, lub Windows
- Terminal / Wiersz poleceń

## Szybka Instalacja - 3 Kroki

### Krok 1: Pobierz Projekt

Jeśli jeszcze nie masz projektu:
```bash
git clone https://github.com/TMMCx2/DCDock.git
cd DCDock
```

Albo rozpakuj pobrany ZIP i wejdź do folderu w terminalu.

### Krok 2: Uruchom Setup (Tylko Raz)

**macOS/Linux:**
```bash
chmod +x setup.sh start_backend.sh start_client.sh
./setup.sh
```

**Windows:**
```cmd
python setup.py
```

To zajmie 2-3 minuty. Skrypt:
- Utworzy wirtualne środowiska Python (odizolowane od systemu)
- Zainstaluje wszystkie biblioteki
- Naprawi problemy z bcrypt i email-validator
- Utworzy bazę danych SQLite z przykładowymi danymi

**Przykładowy output:**
```
════════════════════════════════════════════════════════════════
  DCDock - Initial Setup
════════════════════════════════════════════════════════════════

✓ Found Python: 3.13.5

────────────────────────────────────────────────────────────────
  Setting up Backend...
────────────────────────────────────────────────────────────────
Creating virtual environment...
✓ Virtual environment created
Installing dependencies...
✓ Dependencies installed
✓ bcrypt version fixed
✓ email-validator installed
Initializing database with demo data...
✓ Database initialized

────────────────────────────────────────────────────────────────
  Setting up Client TUI...
────────────────────────────────────────────────────────────────
Creating virtual environment...
✓ Virtual environment created
Installing dependencies...
✓ Dependencies installed

════════════════════════════════════════════════════════════════
  ✓ Setup Complete!
════════════════════════════════════════════════════════════════
```

### Krok 3: Uruchom Aplikację

Potrzebujesz **dwóch okien terminala**:

**Terminal 1 - Backend (Serwer API):**
```bash
./start_backend.sh
```

Zobaczysz:
```
════════════════════════════════════════════════════════════════
  DCDock Backend Server
════════════════════════════════════════════════════════════════

Starting backend server on http://0.0.0.0:8000

Press Ctrl+C to stop the server
════════════════════════════════════════════════════════════════

INFO:     Uvicorn running on http://0.0.0.0:8000
INFO:     Application startup complete.
```

**Zostaw to okno otwarte!**

---

**Terminal 2 - Klient TUI (Interfejs):**

Otwórz **nowe okno terminala** (Cmd+T na Mac, Ctrl+Shift+T na Linux):

```bash
./start_client.sh
```

Zobaczysz ekran logowania! 🎉

## Logowanie

Użyj tych danych testowych:

**Operator (zalecane na początek):**
- Email: `operator1@dcdock.com`
- Hasło: `operator123`

**Administrator:**
- Email: `admin@dcdock.com`
- Hasło: `admin123`

**Jak się zalogować:**
1. Wpisz email i naciśnij **Enter**
2. Wpisz hasło i naciśnij **Enter**
3. Zobaczysz główny ekran z tabelą przypisań!

## Co Zobaczysz?

Po zalogowaniu zobaczysz tabelę z przypisaniami rampy:

```
┌────────────────────────────────────────────────────────────────┐
│ DCDock Board - Logged in as: John Operator (OPERATOR)         │
├────────────────────────────────────────────────────────────────┤
│ [All] [Inbound] [Outbound]                                     │
├─────┬──────┬─────────────┬───────────┬────────────┬───────────┤
│ ID  │ Ramp │ Load        │ Direction │ Status     │ Version   │
├─────┼──────┼─────────────┼───────────┼────────────┼───────────┤
│ 1   │ R5   │ IB-2024-001 │ INBOUND   │ Planned    │ 1         │
│ 2   │ R6   │ IB-2024-002 │ INBOUND   │ Arrived    │ 1         │
│ 3   │ R7   │ IB-2024-003 │ INBOUND   │ In Progress│ 1         │
│ 4   │ R1   │ OB-2024-001 │ OUTBOUND  │ Planned    │ 1         │
│ 5   │ R2   │ OB-2024-002 │ OUTBOUND  │ Arrived    │ 1         │
│ 6   │ R3   │ OB-2024-003 │ OUTBOUND  │ In Progress│ 1         │
└─────┴──────┴─────────────┴───────────┴────────────┴───────────┘
│ r: refresh | d: delete | 1: all | 2: inbound | 3: outbound  │
│ Esc: quit                                                     │
└───────────────────────────────────────────────────────────────┘
```

## Skróty Klawiszowe

- **r** - Odśwież listę
- **d** - Usuń zaznaczone przypisanie
- **1** - Pokaż wszystkie (All)
- **2** - Pokaż tylko Inbound (przychodzące)
- **3** - Pokaż tylko Outbound (wychodzące)
- **Esc** - Wyjdź z aplikacji

## Test Aktualizacji w Czasie Rzeczywistym

To najfajniejsza funkcja! Zobaczysz zmiany od razu.

### Metoda 1: Drugi Klient TUI

1. **Terminal 1**: Backend działa
2. **Terminal 2**: Twój pierwszy klient TUI (zalogowany jako operator1)
3. **Terminal 3**: Otwórz trzeci terminal i uruchom:
   ```bash
   ./start_client.sh
   ```
   Zaloguj się jako: `operator2@dcdock.com` / `operator123`

4. **Test**: W Terminal 2 usuń jakieś przypisanie (naciśnij `d`)
5. **Magia**: W Terminal 3 przypisanie zniknie automatycznie! ✨

### Metoda 2: API (dla zaawansowanych)

W trzecim terminalu możesz też testować API:

```bash
# Zaloguj się i zapisz token
TOKEN=$(curl -s -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@dcdock.com","password":"admin123"}' | jq -r '.access_token')

# Utwórz nowe przypisanie
curl -X POST http://localhost:8000/api/assignments/ \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "ramp_id": 4,
    "load_id": 4,
    "status_id": 1,
    "eta_in": "2024-01-15T10:00:00",
    "eta_out": "2024-01-15T12:00:00"
  }'
```

Nowe przypisanie pojawi się od razu w TUI! 🚀

## Dokumentacja API

Gdy backend działa, otwórz przeglądarkę:

- **Swagger UI** (interaktywna dokumentacja): http://localhost:8000/docs
- **ReDoc** (czytelna dokumentacja): http://localhost:8000/redoc

Możesz tam testować wszystkie endpointy API przez przeglądarkę.

## Zatrzymywanie Aplikacji

### Zatrzymaj Klienta TUI:
Naciśnij **Esc** lub **Ctrl+C**

### Zatrzymaj Backend:
W terminalu z backendem naciśnij **Ctrl+C**

```
^C
INFO:     Shutting down
INFO:     Finished server process
```

## Następne Uruchomienie

Nie musisz już uruchamiać `setup.sh`! Po pierwszym setupie, wystarczy:

```bash
# Terminal 1
./start_backend.sh

# Terminal 2
./start_client.sh
```

## Struktura Projektu

```
DCDock/
├── setup.sh              ← Instalacja (uruchom raz)
├── start_backend.sh      ← Start serwera
├── start_client.sh       ← Start klienta TUI
├── backend/
│   ├── venv/            ← Wirtualne środowisko Python (automatyczne)
│   ├── dcdock.db        ← Baza danych SQLite (automatyczna)
│   ├── .env             ← Konfiguracja (automatyczna)
│   └── app/             ← Kod backendu
└── client_tui/
    ├── venv/            ← Wirtualne środowisko Python (automatyczne)
    └── app/             ← Kod TUI
```

## Rozwiązywanie Problemów

### Problem: "command not found: pip"

**macOS/Homebrew:**
Edytuj skrypty i zamień wszystkie `pip` na `pip3`:
```bash
nano setup.sh
# Zamień: pip install
# Na:     pip3 install
```

### Problem: "bcrypt error" lub "password cannot be longer than 72 bytes"

Setup automatycznie to naprawia. Jeśli wystąpi ręcznie:
```bash
cd backend
source venv/bin/activate
pip install --force-reinstall "bcrypt>=4.0.0,<5.0.0"
```

### Problem: "email-validator is not installed"

Setup automatycznie to naprawia. Jeśli wystąpi ręcznie:
```bash
cd backend
source venv/bin/activate
pip install email-validator
```

### Problem: "externally-managed-environment" (Python 3.13 na macOS)

Używaj skryptów! One tworzą wirtualne środowiska automatycznie. Nigdy nie instaluj pakietów globalnie (`--break-system-packages`).

### Problem: Backend nie startuje

1. Sprawdź czy Python 3.11+ jest zainstalowany: `python3 --version`
2. Sprawdź czy `.env` istnieje: `ls -la backend/.env`
3. Usuń bazę i zrób setup ponownie:
   ```bash
   rm backend/dcdock.db
   ./setup.sh
   ```

### Problem: Nie mogę się połączyć z backendem

1. Sprawdź czy backend działa: `curl http://localhost:8000/docs`
2. Jeśli nie działa, sprawdź logi w terminalu z backendem
3. Sprawdź czy port 8000 nie jest zajęty: `lsof -i :8000` (macOS/Linux)

## Więcej Użytkowników Testowych

Oprócz operator1, są też:

- `operator2@dcdock.com` / `operator123` (Jane Operator)
- `operator3@dcdock.com` / `operator123` (Bob Operator)
- `operator4@dcdock.com` / `operator123` (Alice Operator)
- `admin2@dcdock.com` / `admin123` (Admin Two)

Możesz zalogować wiele klientów jednocześnie!

## Czyszczenie i Reset

Jeśli chcesz zacząć od nowa:

```bash
# Usuń wirtualne środowiska i bazę
rm -rf backend/venv client_tui/venv backend/dcdock.db

# Uruchom setup ponownie
./setup.sh
```

## Pomoc i Wsparcie

- **Dokumentacja projektu**: [README.md](README.md)
- **WebSocket API**: [docs/WEBSOCKET.md](docs/WEBSOCKET.md)
- **Deployment produkcyjny**: [docs/PRODUCTION.md](docs/PRODUCTION.md)
- **GitHub Issues**: https://github.com/TMMCx2/DCDock/issues

## Gratulacje! 🎉

Teraz możesz:
- ✅ Uruchomić backend i klienta TUI
- ✅ Zalogować się i przeglądać przypisania
- ✅ Testować aktualizacje w czasie rzeczywistym
- ✅ Korzystać z API przez Swagger UI
- ✅ Zarządzać wieloma klientami jednocześnie

Miłego korzystania z DCDock! 🚀
