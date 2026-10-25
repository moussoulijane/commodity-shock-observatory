$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
docker compose up -d
if ($LASTEXITCODE -ne 0) { throw 'Docker services did not start.' }
Write-Output 'Analyst workspace: http://127.0.0.1:8080'
Write-Output 'Engineering console: http://127.0.0.1:8080/ops'
