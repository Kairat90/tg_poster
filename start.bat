@echo off
setlocal
cd /d "%~dp0"

if exist "%~dp0frontend\package.json" (
  pushd "%~dp0frontend"
  call npm run build
  if errorlevel 1 (
    echo [ERROR] Frontend build failed.
    popd
    exit /b 1
  )
  popd
)

"%~dp0venv\Scripts\python.exe" "%~dp0main.py"
