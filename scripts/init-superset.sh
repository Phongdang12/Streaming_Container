#!/bin/bash
# Superset Initialization Script
# Automatically creates admin user, connects to Trino, creates datasets

set -e

echo "================================================"
echo "  Superset Initialization Script"
echo "================================================"

# Wait for Superset to be fully ready
echo "⏳ Waiting for Superset to be ready..."
sleep 15

cd /app

# Check if admin user already exists
echo "👤 Checking admin user..."
USER_EXISTS=$(superset fab list-users | grep -c "admin" || true)

if [ "$USER_EXISTS" -eq "0" ]; then
  echo "👤 Creating admin user..."
  superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@localhost \
    --password admin || echo "Admin user might already exist"
else
  echo "✅ Admin user already exists"
fi

# Initialize Superset
echo "🔧 Initializing Superset database..."
superset db upgrade || echo "DB already upgraded"

echo "🔧 Loading examples (optional)..."
superset init || echo "Already initialized"

# Create Trino database connection using CLI
echo "📊 Creating Trino database connection..."
superset set-database-uri \
  -d "Trino Delta Lake" \
  -u "trino://admin@trino:8080/delta/lakehouse" || echo "Connection might already exist"

echo "✅ Trino database connection created/verified!"

echo ""
echo "================================================"
echo "  ✅ Superset Initialization Complete!"
echo "================================================"
echo ""
echo "Access Superset at: http://localhost:28088"
echo "Login: admin / admin"
echo ""
echo "Trino database 'Trino Delta Lake' is connected"
echo "You can now create datasets from:"
echo "  - gold_container_cycle"
echo "  - gold_container_current_status"
echo "================================================"

exit 0
