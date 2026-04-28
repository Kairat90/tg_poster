$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
& "$PSScriptRoot\venv\Scripts\python.exe" "$PSScriptRoot\worker_main.py"
