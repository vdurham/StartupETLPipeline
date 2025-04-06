"""
Database connection manager
"""
import os
import sqlite3
import logging
from contextlib import contextmanager

from config import DB_CONFIG, DB_TYPE, SCHEMA_DIR

logger = logging.getLogger(__name__)

def execute_sql_file(cursor, file_path):
    """Execute SQL statements from a file."""
    try:
        with open(file_path, 'r') as sql_file:
            sql_script = sql_file.read()
            cursor.execute(sql_script)
            logger.debug(f"Executed SQL file: {file_path}")
    except Exception as e:
        logger.error(f"Error executing SQL file {file_path}: {e}")
        raise

def initialize_schema(cursor, schema_files=None):
    """Initialize database schema from SQL files in the schema directory."""
    if schema_files is None:
        # Get all .sql files in the schema directory
        schema_files = [os.path.join(SCHEMA_DIR, f) for f in os.listdir(SCHEMA_DIR) if f.endswith('.sql')]
    
    for schema_file in schema_files:
        logger.info(f"Initializing schema from {schema_file}")
        execute_sql_file(cursor, schema_file)

@contextmanager
def get_sqlite_connection():
    """Get a SQLite database connection."""
    conn = None
    try:
        db_path = DB_CONFIG['sqlite']['db_path']
        logger.info(f"Connecting to SQLite database: {db_path}")
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        conn = sqlite3.connect(db_path)
        # Enable foreign keys
        conn.execute("PRAGMA foreign_keys = 1")
        # Return dictionary-like rows
        conn.row_factory = sqlite3.Row
        
        yield conn
    except Exception as e:
        logger.error(f"Error connecting to SQLite database: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

@contextmanager
def get_connection():
    """Get a database connection based on the configured database type."""
    if DB_TYPE == 'sqlite':
        with get_sqlite_connection() as conn:
            yield conn
    # Add other database types as needed
    else:
        raise ValueError(f"Unsupported database type: {DB_TYPE}")

def init_database():
    """Initialize the database with all schema files."""
    with get_connection() as conn:
        cursor = conn.cursor()
        # Initialize all schema files
        initialize_schema(cursor)
        conn.commit()
        logger.info("Database initialized successfully")