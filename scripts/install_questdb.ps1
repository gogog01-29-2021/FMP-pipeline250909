#!/bin/bash

# QuestDB Installation and Setup Script for Windows (PowerShell)

# Download and install QuestDB
echo "Installing QuestDB..."

# Create QuestDB directory
mkdir -p questdb
cd questdb

# Download QuestDB
$questdb_version = "7.3.10"
$download_url = "https://github.com/questdb/questdb/releases/download/$questdb_version/questdb-$questdb_version-rt-windows-amd64.zip"

Write-Host "Downloading QuestDB $questdb_version..."
Invoke-WebRequest -Uri $download_url -OutFile "questdb.zip"

# Extract QuestDB
Expand-Archive -Path "questdb.zip" -DestinationPath "." -Force
Remove-Item "questdb.zip"

# Create startup script
@"
@echo off
echo Starting QuestDB...
cd /d "%~dp0"
java -jar questdb-$questdb_version-rt-windows-amd64.jar
pause
"@ | Out-File -FilePath "start_questdb.bat" -Encoding ASCII

echo "QuestDB installed successfully!"
echo "To start QuestDB, run: ./questdb/start_questdb.bat"
echo "QuestDB will be available at:"
echo "  - Web Console: http://localhost:9000"
echo "  - PostgreSQL Wire Protocol: localhost:8812"
echo "  - REST API: http://localhost:9000/exec"
