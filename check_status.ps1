# PowerShell script to check the status of all TP-IASC instances
Write-Host "TP-IASC System Status Check" -ForegroundColor Green
Write-Host "=" * 50 -ForegroundColor Green

# Function to check if a port is listening
function Test-Port {
    param(
        [string]$Host = "localhost",
        [int]$Port,
        [string]$ServiceName
    )
    
    try {
        $connection = Test-NetConnection -ComputerName $Host -Port $Port -WarningAction SilentlyContinue
        if ($connection.TcpTestSucceeded) {
            Write-Host "✓ $ServiceName ($Host`:$Port) - RUNNING" -ForegroundColor Green
            return $true
        } else {
            Write-Host "✗ $ServiceName ($Host`:$Port) - NOT RUNNING" -ForegroundColor Red
            return $false
        }
    } catch {
        Write-Host "✗ $ServiceName ($Host`:$Port) - ERROR" -ForegroundColor Red
        return $false
    }
}

# Function to make HTTP health check
function Test-HealthEndpoint {
    param(
        [string]$Url,
        [string]$ServiceName
    )
    
    try {
        $response = Invoke-RestMethod -Uri "$Url/health" -Method Get -TimeoutSec 5
        if ($response.status -eq "ok") {
            Write-Host "✓ $ServiceName - HEALTHY" -ForegroundColor Green
        } else {
            Write-Host "? $ServiceName - UNKNOWN STATUS" -ForegroundColor Yellow
        }
    } catch {
        Write-Host "✗ $ServiceName - HEALTH CHECK FAILED" -ForegroundColor Red
    }
}

Write-Host "`nChecking App Instances:" -ForegroundColor Cyan
Test-Port -Port 9000 -ServiceName "App Instance 1"
Test-Port -Port 9001 -ServiceName "App Instance 2" 
Test-Port -Port 9002 -ServiceName "App Instance 3"

Write-Host "`nChecking Sentinel Instances:" -ForegroundColor Cyan
Test-Port -Port 8000 -ServiceName "Sentinel Instance 1"
Test-Port -Port 8001 -ServiceName "Sentinel Instance 2"
Test-Port -Port 8002 -ServiceName "Sentinel Instance 3"

Write-Host "`nHealth Check (if instances are running):" -ForegroundColor Cyan
Test-HealthEndpoint -Url "http://localhost:9000" -ServiceName "App 1 Health"
Test-HealthEndpoint -Url "http://localhost:9001" -ServiceName "App 2 Health"
Test-HealthEndpoint -Url "http://localhost:9002" -ServiceName "App 3 Health"
Test-HealthEndpoint -Url "http://localhost:8000" -ServiceName "Sentinel 1 Health"
Test-HealthEndpoint -Url "http://localhost:8001" -ServiceName "Sentinel 2 Health"
Test-HealthEndpoint -Url "http://localhost:8002" -ServiceName "Sentinel 3 Health"

Write-Host "`nCluster Status (from Sentinel 1):" -ForegroundColor Cyan
try {
    $clusterStatus = Invoke-RestMethod -Uri "http://localhost:8000/cluster-status" -Method Get -TimeoutSec 5
    Write-Host "Total instances in cluster: $($clusterStatus.total_instances)" -ForegroundColor White
    Write-Host "Health status details:" -ForegroundColor White
    $clusterStatus.health_status | Format-Table -AutoSize
} catch {
    Write-Host "✗ Could not retrieve cluster status from Sentinel 1" -ForegroundColor Red
}

Write-Host "`nUseful URLs:" -ForegroundColor Yellow
Write-Host "App instances:"
Write-Host "  - http://localhost:9000/health"
Write-Host "  - http://localhost:9001/health" 
Write-Host "  - http://localhost:9002/health"
Write-Host "Sentinel monitoring:"
Write-Host "  - http://localhost:8000/cluster-status"
Write-Host "  - http://localhost:8001/cluster-status"
Write-Host "  - http://localhost:8002/cluster-status"