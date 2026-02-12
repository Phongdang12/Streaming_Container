$ErrorActionPreference = "Continue" # Changed to Continue to handle SQL errors gracefully

Write-Host "=======================================================" -ForegroundColor Cyan
Write-Host "  Validating Trino Delta Lake Connection & Data"
Write-Host "=======================================================" -ForegroundColor Cyan

# Wait for Trino to be ready
Write-Host "Waiting for Trino to be ready..." -NoNewline
$trinoUrl = "http://localhost:8081/v1/info"
$maxRetries = 30
$retryCount = 0

while ($retryCount -lt $maxRetries) {
    try {
        $response = Invoke-WebRequest -Uri $trinoUrl -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop
        if ($response.StatusCode -eq 200) {
            Write-Host " Done." -ForegroundColor Green
            break
        }
    }
    catch {
        Write-Host "." -NoNewline
        Start-Sleep -Seconds 2
        $retryCount++
    }
}

if ($retryCount -eq $maxRetries) {
    Write-Host ""
    Write-Host "Trino is not ready after waiting. Check logs." -ForegroundColor Red
    exit 1
}

Write-Host "Trino is ready." -ForegroundColor Green

# Run validation SQL
Write-Host "Running validation queries..." -ForegroundColor Yellow

# Check if file exists
if (-not (Test-Path ".\scripts\validate_trino.sql")) {
    Write-Host "File .\scripts\validate_trino.sql not found!" -ForegroundColor Red
    exit 1
}

# Execute SQL using docker exec with stdin and capture output/error
$output = Get-Content ".\scripts\validate_trino.sql" -Raw | docker exec -i trino trino --catalog delta --schema lakehouse --file /dev/stdin 2>&1

# Check output for specific errors
if ($output -match "does not exist") {
    Write-Host ""
    Write-Host "⚠️  Tables are not registered yet." -ForegroundColor Yellow
    Write-Host "   This is NORMAL if you haven't run the Spark jobs yet." -ForegroundColor Gray
    Write-Host "   The tables will appear automatically after you run Step 3, 4, and 5." -ForegroundColor Gray
    Write-Host ""
    Write-Host "✅ Connection to Trino is WORKING." -ForegroundColor Green
}
elseif ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Error running validation queries:" -ForegroundColor Red
    Write-Host $output
    exit 1
}
else {
    Write-Host $output
    Write-Host "=======================================================" -ForegroundColor Cyan
    Write-Host "If counts are greater than 0, the setup is COMPLETE." -ForegroundColor Green
    Write-Host "If counts are 0, check:" -ForegroundColor Yellow
    Write-Host "  1. Spark streaming job status (is it running?)"
    Write-Host "  2. MinIO location (s3://lakehouse/gold/...)"
    Write-Host "  3. Metastore registration (trino-init logs)"
    Write-Host "=======================================================" -ForegroundColor Cyan
}
