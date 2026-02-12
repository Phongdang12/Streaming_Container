# Check Pipeline Progress
# Kiểm tra tiến độ xử lý dữ liệu giữa Bronze -> Silver -> Gold layers

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  CHECKING PIPELINE PROGRESS" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Load environment variables from .env file
$envFile = Join-Path $PSScriptRoot "..\.env"
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
            $name = $matches[1].Trim()
            $value = $matches[2].Trim()
            [Environment]::SetEnvironmentVariable($name, $value, "Process")
        }
    }
}

# Get MinIO credentials from environment or use defaults
$minioUser = $env:MINIO_ROOT_USER
$minioPassword = $env:MINIO_ROOT_PASSWORD

if (-not $minioUser) {
    $minioUser = "minioadmin"
}
if (-not $minioPassword) {
    $minioPassword = "minioadmin123"
}

Write-Host "Running pipeline progress monitor..." -ForegroundColor Yellow
Write-Host ""

# Run monitoring script in spark-master container
docker exec spark-master spark-submit `
    --master local[*] `
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension `
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog `
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 `
    --conf spark.hadoop.fs.s3a.access.key=$minioUser `
    --conf spark.hadoop.fs.s3a.secret.key=$minioPassword `
    --conf spark.hadoop.fs.s3a.path.style.access=true `
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem `
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider `
    --conf spark.hadoop.fs.s3a.connection.maximum=100 `
    --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 `
    /opt/spark-jobs/monitor_pipeline_progress.py

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Monitoring complete" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
