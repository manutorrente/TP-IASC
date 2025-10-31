# PowerShell script to stop all TP-IASC instances
Write-Host "Stopping TP-IASC distributed system..." -ForegroundColor Red

# Function to kill processes by port
function Stop-ProcessByPort {
    param([int]$Port)
    
    try {
        $connections = netstat -ano | findstr ":$Port "
        if ($connections) {
            $connections | ForEach-Object {
                $line = $_.Trim()
                if ($line -match '\s+(\d+)$') {
                    $pid = $matches[1]
                    $process = Get-Process -Id $pid -ErrorAction SilentlyContinue
                    if ($process) {
                        Write-Host "Stopping process on port $Port (PID: $pid)" -ForegroundColor Yellow
                        Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
                    }
                }
            }
        } else {
            Write-Host "No process found on port $Port" -ForegroundColor Gray
        }
    } catch {
        Write-Host "Error stopping process on port $Port" -ForegroundColor Red
    }
}

# Stop all app instances
Write-Host "`nStopping App instances..." -ForegroundColor Yellow
Stop-ProcessByPort -Port 9000
Stop-ProcessByPort -Port 9001
Stop-ProcessByPort -Port 9002

# Stop all sentinel instances  
Write-Host "`nStopping Sentinel instances..." -ForegroundColor Yellow
Stop-ProcessByPort -Port 8000
Stop-ProcessByPort -Port 8001
Stop-ProcessByPort -Port 8002

# Alternative method: kill by process name
Write-Host "`nCleaning up any remaining UV/Python processes..." -ForegroundColor Yellow
$uvProcesses = Get-Process -Name "uv", "python" -ErrorAction SilentlyContinue
if ($uvProcesses) {
    $uvProcesses | ForEach-Object {
        $cmdline = (Get-CimInstance Win32_Process -Filter "ProcessId = $($_.Id)").CommandLine
        if ($cmdline -and ($cmdline -match "sentinel|app/main\.py")) {
            Write-Host "Stopping process: $($_.Id) ($($_.Name))" -ForegroundColor Yellow
            Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
        }
    }
}

Write-Host "`nAll TP-IASC instances have been stopped!" -ForegroundColor Green