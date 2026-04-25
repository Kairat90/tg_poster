@echo off
setlocal
cd /d "%~dp0"

if not exist "%~dp0venv\Scripts\python.exe" (
    echo venv\Scripts\python.exe not found
    exit /b 1
)

"%~dp0venv\Scripts\python.exe" -m pip install pyinstaller
if errorlevel 1 exit /b 1

"%~dp0venv\Scripts\python.exe" -m PyInstaller ^
    --noconfirm ^
    --clean ^
    --onefile ^
    --windowed ^
    --name tg_poster ^
    --collect-all customtkinter ^
    "%~dp0main.py"
