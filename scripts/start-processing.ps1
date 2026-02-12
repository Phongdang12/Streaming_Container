Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  STARTING DATA PROCESSING PIPELINE" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "[1/4] Starting Producer (Generating Data)..." -ForegroundColor Yellow
docker-compose up -d producer-stream

Write-Host "[2/4] Starting Spark Bronze-Silver Stream..." -ForegroundColor Yellow
docker-compose up -d spark-stream-bronze-silver

Write-Host "[3/4] Starting Spark Gold Stream (Aggregations)..." -ForegroundColor Yellow
docker-compose up -d spark-stream-gold-ops

Write-Host "[4/4] Starting Spark KPI Batch Job..." -ForegroundColor Yellow
docker-compose up -d spark-kpi-batch

Write-Host ""
Write-Host "✅ All processing jobs started!" -ForegroundColor Green
Write-Host "⏳ Please wait 3-5 minutes for data to flow through the pipeline." -ForegroundColor Gray
Write-Host ""
Write-Host "To monitor the flow, run:" -ForegroundColor White
Write-Host "  .\scripts\monitor-flow.ps1" -ForegroundColor White
