"""
Superset Configuration
"""
import os

# Flask App Builder configuration
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'thisISaSECRET_1234')

# Database configuration
SQLALCHEMY_DATABASE_URI = (
    f"postgresql://{os.environ.get('POSTGRES_USER', 'superset')}:"
    f"{os.environ.get('POSTGRES_PASSWORD', 'superset123')}@"
    f"postgres:5432/{os.environ.get('POSTGRES_DB', 'superset')}"
)

# Redis cache configuration
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': os.environ.get('REDIS_HOST', 'redis'),
    'CACHE_REDIS_PORT': os.environ.get('REDIS_PORT', 6379),
    'CACHE_REDIS_DB': 1
}

DATA_CACHE_CONFIG = CACHE_CONFIG

# Superset specific config
ROW_LIMIT = 5000
SUPERSET_WEBSERVER_PORT = 8088

# Enable async queries
GLOBAL_ASYNC_QUERIES_TRANSPORT = "polling"
GLOBAL_ASYNC_QUERIES_POLLING_DELAY = 500

# Security
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None

# Feature flags
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
}
