# QuestDB Download and Setup Script for Windows
# This script downloads and extracts QuestDB binary

param(
    [string]$Version = "8.1.4"
)

Write-Host "🚀 QuestDB Setup Script" -ForegroundColor Green
Write-Host "========================" -ForegroundColor Green
Write-Host ""

$questdbUrl = "https://github.com/questdb/questdb/releases/download/$Version/questdb-$Version-rt-windows-amd64.zip"
$zipFile = "questdb-$Version.zip"
$extractFolder = "questdb"

Write-Host "📥 Downloading QuestDB $Version..." -ForegroundColor Cyan
Write-Host "URL: $questdbUrl" -ForegroundColor Gray

try {
    # Download with progress
    $webClient = New-Object System.Net.WebClient
    $webClient.DownloadFile($questdbUrl, $zipFile)
    
    if (Test-Path $zipFile) {
        $size = (Get-Item $zipFile).Length / 1MB
        Write-Host "✅ Downloaded: $zipFile ($([math]::Round($size,1)) MB)" -ForegroundColor Green
        
        Write-Host "📂 Extracting to $extractFolder..." -ForegroundColor Cyan
        
        # Remove existing folder if it exists
        if (Test-Path $extractFolder) {
            Remove-Item -Recurse -Force $extractFolder
        }
        
        # Extract
        Expand-Archive -Path $zipFile -DestinationPath $extractFolder -Force
        
        # Look for questdb.exe
        $questdbExe = Get-ChildItem -Path $extractFolder -Name "questdb.exe" -Recurse | Select-Object -First 1
        
        if ($questdbExe) {
            $questdbPath = Join-Path $extractFolder $questdbExe
            Write-Host "✅ QuestDB extracted successfully!" -ForegroundColor Green
            Write-Host "📍 Location: $questdbPath" -ForegroundColor Yellow
            Write-Host ""
            Write-Host "🏃 Starting QuestDB..." -ForegroundColor Cyan
            
            # Change to QuestDB directory and start
            $questdbDir = Split-Path -Parent $questdbPath
            Set-Location $questdbDir
            
            Write-Host "🌐 QuestDB will be available at: http://localhost:9000" -ForegroundColor Yellow
            Write-Host "📊 Web console: http://localhost:9000" -ForegroundColor Yellow
            Write-Host ""
            Write-Host "🚀 Starting QuestDB now..." -ForegroundColor Green
            
            # Start QuestDB
            Start-Process -FilePath ".\questdb.exe" -WorkingDirectory $questdbDir
            
            Write-Host "✅ QuestDB started!" -ForegroundColor Green
            Write-Host ""
            Write-Host "Next steps:" -ForegroundColor Cyan
            Write-Host "1. 🌐 Open http://localhost:9000 in your browser" -ForegroundColor White
            Write-Host "2. 🚀 Run your ETF analysis: python etf_fmp_processor.py" -ForegroundColor White
            
        } else {
            Write-Host "❌ questdb.exe not found in extracted files" -ForegroundColor Red
            Get-ChildItem -Path $extractFolder -Recurse | Select-Object Name, FullName | Format-Table
        }
        
    } else {
        Write-Host "❌ Download failed - file not found" -ForegroundColor Red
    }
    
} catch {
    Write-Host "❌ Error: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""
Write-Host "🔧 If QuestDB doesn't start, you can manually:" -ForegroundColor Yellow
Write-Host "   1. Navigate to the questdb folder" -ForegroundColor White
Write-Host "   2. Double-click questdb.exe" -ForegroundColor White
Write-Host "   3. Or run in command prompt: questdb.exe" -ForegroundColor White
