"""
Configuration Settings
"""
import os
from datetime import datetime

# Database Configuration
DB_CONFIG = {
    'sqlite': {
        'db_path': os.environ.get('DB_PATH', 'data\startup_data.db')
     }
    # Add other database configurations here (e.g., PostgreSQL, AWS RDS)
}

# Database to use (sqlite)
DB_TYPE = os.environ.get('DB_TYPE', 'sqlite')

# File paths
DATA_DIR = os.environ.get('DATA_DIR', 'data')
SCHEMA_DIR = os.environ.get('SCHEMA_DIR', 'schemas')

# Input CSV files
ORGANIZATIONS_CSV = os.path.join(DATA_DIR, os.environ.get('ORGANIZATIONS_CSV', 'organizations.csv'))
PEOPLE_CSV = os.path.join(DATA_DIR, os.environ.get('PEOPLE_CSV', 'people.csv'))
JOBS_CSV = os.path.join(DATA_DIR, os.environ.get('JOBS_CSV', 'jobs.csv'))

# API Configuration
API_BASE_URL = os.environ.get('API_BASE_URL', '')
API_KEY = os.environ.get('API_KEY', '')

# Logging Configuration
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
LOG_FILE = os.environ.get('LOG_FILE', f'logs/pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

# Pipeline Configuration
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '1000'))
CHECKPOINT_INTERVAL = int(os.environ.get('CHECKPOINT_INTERVAL', '1000'))
INCREMENTAL_MODE = os.environ.get('INCREMENTAL_MODE', 'true').lower() == 'true'
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', '4'))