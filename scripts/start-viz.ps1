Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  STARTING VISUALIZATION LAYER ONLY" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Stopping resource-heavy processing services..." -ForegroundColor Yellow

# Stop Spark and Kafka services to free up RAM/CPU
docker-compose stop spark-master spark-worker spark-stream-bronze-silver spark-stream-gold-ops spark-kpi-batch producer producer-stream kafka kafka-ui

Write-Host "Starting/Ensuring Visualization Services are UP..." -ForegroundColor Green

# Start only what is needed for Superset -> Trino -> MinIO
docker-compose up -d minio postgres metastore-db hive-metastore trino redis superset

Write-Host ""
Write-Host "✅ Visualization mode active!" -ForegroundColor Green
Write-Host "   - Spark & Kafka: STOPPED (Resources freed)"
Write-Host "   - Superset/Trino: RUNNING"
Write-Host ""
Write-Host "👉 Access Superset: http://localhost:8088" -ForegroundColor White
