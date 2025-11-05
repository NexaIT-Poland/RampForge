# DCDock Client - Installation Guide for Operators

**Made by NEXAIT sp. z o.o.**
📧 office@nexait.pl | 🌐 https://nexait.pl/

---

## 📋 System Requirements

- **Operating System:** Windows 10/11, macOS 10.15+, or Linux
- **Python:** 3.11 or higher
- **Internet Connection:** Required for connecting to DCDock server
- **Terminal:** Command Prompt (Windows) or Terminal (Mac/Linux)

---

## 🚀 Quick Start

### Windows

1. **Download and Install Python:**
   - Go to https://www.python.org/downloads/
   - Download Python 3.11+ for Windows
   - **IMPORTANT:** Check "Add Python to PATH" during installation

2. **Download DCDock Client:**
   - Download ZIP from: https://github.com/TMMCx2/DCDock
   - Extract to a folder (e.g., `C:\DCDock`)

3. **Run the Client:**
   - Open the `client_tui` folder
   - **Double-click** `START_CLIENT_WINDOWS.bat`
   - First run will automatically install dependencies (takes 1-2 minutes)

4. **Configure Connection:**
   - On first run, script will create `config.yaml`
   - Open `config.yaml` in Notepad
   - Change these lines:
     ```yaml
     base_url: "https://your-server.com/api/v1"
     websocket_url: "wss://your-server.com/api/v1/ws"
     ```
   - Replace `your-server.com` with your actual server address
   - Save and run `START_CLIENT_WINDOWS.bat` again

5. **Login:**
   - Enter your email and password provided by admin
   - Press Enter to login

### macOS / Linux

1. **Install Python (if not already installed):**
   ```bash
   # macOS (using Homebrew)
   brew install python@3.11

   # Ubuntu/Debian
   sudo apt update
   sudo apt install python3 python3-pip python3-venv

   # Fedora
   sudo dnf install python3 python3-pip
   ```

2. **Download DCDock Client:**
   ```bash
   cd ~
   git clone https://github.com/TMMCx2/DCDock.git
   cd DCDock/client_tui
   ```

3. **Make script executable:**
   ```bash
   chmod +x START_CLIENT_UNIX.sh
   ```

4. **Run the Client:**
   ```bash
   ./START_CLIENT_UNIX.sh
   ```
   - First run will automatically install dependencies

5. **Configure Connection:**
   - Edit `config.yaml`:
     ```bash
     nano config.yaml
     ```
   - Change these lines:
     ```yaml
     base_url: "https://your-server.com/api/v1"
     websocket_url: "wss://your-server.com/api/v1/ws"
     ```
   - Save (Ctrl+O, Enter, Ctrl+X in nano)
   - Run `./START_CLIENT_UNIX.sh` again

6. **Login:**
   - Enter your email and password provided by admin
   - Press Enter to login

---

## ⌨️ Keyboard Shortcuts

Once logged in, use these shortcuts to work with DCDock:

### Navigation
- **Arrow Keys** or **Mouse** - Select dock
- **Tab** - Switch between tables

### Actions
- **[R]** - Refresh data
- **[O]** - Occupy selected dock
- **[F]** - Free selected dock
- **[B]** - Block selected dock

### Filters
- **[1]** - Show all docks
- **[2]** - Show only Inbound (IB) docks
- **[3]** - Show only Outbound (OB) docks

### Other
- **[S]** - Toggle sorting (Priority → Name A-Z → Name Z-A)
- **[Ctrl+F]** - Focus search bar
- **[Escape]** - Logout

---

## 🔧 Troubleshooting

### "Python not found" (Windows)
**Solution:**
1. Reinstall Python from python.org
2. Make sure to check "Add Python to PATH" during installation
3. Restart your computer
4. Try running the script again

### "Connection refused" or "Cannot connect to server"
**Solution:**
1. Check your internet connection
2. Verify `config.yaml` has correct server address
3. Ask your administrator if the server is running
4. Check if your firewall is blocking the connection

### "Module not found" errors
**Solution:**
```bash
# Activate virtual environment
# Windows:
venv\Scripts\activate.bat

# Mac/Linux:
source venv/bin/activate

# Reinstall dependencies
pip install -r requirements.txt
```

### "Invalid credentials"
**Solution:**
- Check your email and password with administrator
- Make sure Caps Lock is OFF
- Try resetting your password through admin

### Client is very slow
**Solution:**
1. Check your internet speed
2. Ask administrator to check server load
3. Try closing other applications
4. Restart the client

---

## 📸 Screenshots

### Login Screen
```
┌─────────────────────────────────┐
│          DCDock                 │
│ Distribution Center Scheduling  │
│                                 │
│ Email: operator1@dcdock.com     │
│ Password: ********              │
│                                 │
│        [Login]                  │
│                                 │
│ Made by NEXAIT sp. z o.o.      │
│ office@nexait.pl               │
└─────────────────────────────────┘
```

### Main Dashboard
```
┌──────────────────────────────────────────────────────────┐
│ 👤 OPERATOR John Doe    DCDock v1.0.0 | NEXAIT         │
├──────────────────────────────────────────────────────────┤
│ [🔄 Refresh] [➕ Occupy] [🟢 Free] [🔴 Block]           │
├──────────────────────────────────────────────────────────┤
│ 🔍 Search: __________ [1]All [2]IB [3]OB                │
├──────────────────────────────────────────────────────────┤
│ 📦 PRIME DOCKS                    │ 📊 Stats            │
│ ┌────────────────────────────┐    │ UTILIZATION         │
│ │Dock Status  Direction Load │    │ 75% of 20 docks    │
│ │P-01 🔵 In Pr 📥 IB  IB-123 │    │                     │
│ │P-02 🟢 Free  -      -      │    │ ═══ PRIME ═══      │
│ └────────────────────────────┘    │ 🔵 8/12 occupied   │
│                                    │ ███████░░░░░        │
│ 📦 BUFFER DOCKS                   │                     │
│ ┌────────────────────────────┐    │ ═══ BUFFER ═══     │
│ │Dock Status  Direction Load │    │ 🔵 7/8 occupied    │
│ │B-01 🟡 Arriv 📤 OB  OB-456 │    │ ████████████░░      │
│ └────────────────────────────┘    └─────────────────────┘
├──────────────────────────────────────────────────────────┤
│ Status: ✓ Loaded 20 docks                               │
└──────────────────────────────────────────────────────────┘
```

---

## 📞 Support

If you encounter any issues:

1. **Check this guide** - Most common issues are covered above
2. **Contact your administrator** - They can help with server-side issues
3. **Contact NEXAIT:**
   - 📧 Email: office@nexait.pl
   - 🌐 Website: https://nexait.pl/

---

## 🔒 Security Notes

- **Never share your password** with anyone
- **Log out** when leaving your workstation (press ESC)
- **Report suspicious activity** to your administrator immediately
- Connection is **encrypted** (HTTPS/WSS) when using production server

---

## ℹ️ About DCDock

DCDock is a Distribution Center Dock Scheduling system designed for efficient warehouse operations. It provides real-time tracking of dock assignments, load management, and operator coordination.

**Version:** 1.0.0
**Created by:** NEXAIT sp. z o.o.
**Contact:** office@nexait.pl
**Website:** https://nexait.pl/

---

**Thank you for using DCDock!** 🚀
