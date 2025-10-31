# PowerShell script to launch multiple sentinel and app instances
# Each instance runs in its own terminal window for easy monitoring

Write-Host "Starting TP-IASC distributed system..." -ForegroundColor Green
Write-Host "This will launch 3 Sentinel instances and 3 App instances in separate terminal windows" -ForegroundColor Yellow

# Get the current directory (project root)
$ProjectRoot = Get-Location

# Function to start a new PowerShell window with a specific command
function Start-NewTerminal {
    param(
        [string]$Title,
        [string]$Command,
        [string]$WorkingDirectory = $ProjectRoot
    )
    
    $ArgumentList = @(
        "-NoExit",
        "-Command",
        "cd '$WorkingDirectory'; $Command"
    )
    
    Start-Process powershell -ArgumentList $ArgumentList -WindowStyle Normal
    Write-Host "Started: $Title" -ForegroundColor Cyan
}

# Launch App instances (ports 9000, 9001, 9002)
Write-Host "`nLaunching App instances..." -ForegroundColor Yellow

Start-NewTerminal -Title "App Instance - Port 9000" -Command "uv run app/main.py 9000"
Start-Sleep -Seconds 1

Start-NewTerminal -Title "App Instance - Port 9001" -Command "uv run app/main.py 9001"
Start-Sleep -Seconds 1

Start-NewTerminal -Title "App Instance - Port 9002" -Command "uv run app/main.py 9002"
Start-Sleep -Seconds 2

# Launch Sentinel instances (using config0.yaml, config1.yaml, config2.yaml)
Write-Host "`nLaunching Sentinel instances..." -ForegroundColor Yellow

Start-NewTerminal -Title "Sentinel Instance - Config 0 (Port 8000)" -Command "uv run -m sentinel.main sentinel/configs/config0.yaml"
Start-Sleep -Seconds 1

Start-NewTerminal -Title "Sentinel Instance - Config 1 (Port 8001)" -Command "uv run -m sentinel.main sentinel/configs/config1.yaml"
Start-Sleep -Seconds 1

Start-NewTerminal -Title "Sentinel Instance - Config 2 (Port 8002)" -Command "uv run -m sentinel.main sentinel/configs/config2.yaml"

Write-Host "`nAll instances launched!" -ForegroundColor Green
Write-Host "`nApp instances running on:" -ForegroundColor White
Write-Host "  - http://localhost:9000" -ForegroundColor Cyan
Write-Host "  - http://localhost:9001" -ForegroundColor Cyan
Write-Host "  - http://localhost:9002" -ForegroundColor Cyan

Write-Host "`nSentinel instances running on:" -ForegroundColor White
Write-Host "  - http://localhost:8000 (monitoring app on 9001)" -ForegroundColor Cyan
Write-Host "  - http://localhost:8001 (monitoring app on 9000)" -ForegroundColor Cyan
Write-Host "  - http://localhost:8002 (monitoring app on 9002)" -ForegroundColor Cyan

Write-Host "`nTo stop all instances, close the individual terminal windows or press Ctrl+C in each terminal." -ForegroundColor Yellow
Write-Host "You can also check the cluster status at: http://localhost:8000/cluster-status" -ForegroundColor Green