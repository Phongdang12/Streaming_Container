# Start Infrastructure Services Only (NOT processing jobs)
# Usage: .\scripts\start-infra.ps1

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  STARTING INFRASTRUCTURE SERVICES" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "[1/2] Starting core infrastructure..." -ForegroundColor Yellow
docker-compose up -d

Write-Host ""
Write-Host "[2/2] Waiting for services to be healthy..." -ForegroundColor Yellow
Write-Host "This may take 1-2 minutes..." -ForegroundColor Gray

# Wait for critical services
$services = @("kafka", "minio", "trino", "superset", "hive-metastore", "postgres")
$maxWait = 120  # 2 minutes
$elapsed = 0
$interval = 5

while ($elapsed -lt $maxWait) {
    Start-Sleep -Seconds $interval
    $elapsed += $interval
    
    $allHealthy = $true
    foreach ($service in $services) {
        $status = docker ps --filter "name=$service" --format "{{.Status}}" 2>$null
        if ($status -notmatch "healthy") {
            $allHealthy = $false
            break
        }
    }
    
    if ($allHealthy) {
        Write-Host ""
        Write-Host "✅ All infrastructure services are healthy!" -ForegroundColor Green
        break
    }
    
    Write-Host "." -NoNewline -ForegroundColor Gray
}

Write-Host ""
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  INFRASTRUCTURE STATUS" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Show status
docker ps --format "table {{.Names}}\t{{.Status}}" | Select-String -Pattern "kafka|minio|trino|superset|spark-master|spark-worker|redis|postgres"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  NEXT STEPS (Manual Execution)" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Infrastructure is ready! Now run processing jobs manually:" -ForegroundColor Green
Write-Host ""
Write-Host "Terminal 1 - Start Producer:" -ForegroundColor Yellow
Write-Host "  docker-compose up producer-stream" -ForegroundColor White
Write-Host ""
Write-Host "Terminal 2 - Start Bronze+Silver Spark Job:" -ForegroundColor Yellow
Write-Host "  docker-compose up spark-stream-bronze-silver" -ForegroundColor White
Write-Host ""
Write-Host "Terminal 3 - Start Gold Spark Job:" -ForegroundColor Yellow
Write-Host "  docker-compose up spark-stream-gold-ops" -ForegroundColor White
Write-Host ""
Write-Host "Terminal 4 - Monitor Data Flow:" -ForegroundColor Yellow
Write-Host "  .\scripts\monitor-flow.ps1" -ForegroundColor White
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "URLs:" -ForegroundColor Cyan
Write-Host "  Superset:  http://localhost:8088 (admin/admin)" -ForegroundColor White
Write-Host "  Kafka UI:  http://localhost:8090" -ForegroundColor White
Write-Host "  MinIO:     http://localhost:9001 (minioadmin/minioadmin123)" -ForegroundColor White
Write-Host "  Trino:     http://localhost:8081" -ForegroundColor White
Write-Host "  Spark UI:  http://localhost:8080" -ForegroundColor White
Write-Host "========================================" -ForegroundColor Cyan
