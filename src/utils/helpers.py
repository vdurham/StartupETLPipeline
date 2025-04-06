"""
Helper functions
"""
import os
import json
import logging
import pandas as pd
from datetime import datetime
import hashlib

logger = logging.getLogger(__name__)

def get_data_checksum(file_path):
    """Calculate a checksum of a file to detect changes."""
    if not os.path.exists(file_path):
        return None
        
    h = hashlib.md5()
    
    with open(file_path, 'rb') as f:
        # Read and update in chunks to handle large files
        for chunk in iter(lambda: f.read(4096), b""):
            h.update(chunk)
            
    return h.hexdigest()

def save_pipeline_state(state, state_file='logs/pipeline_state.json'):
    """Save the pipeline state to a file."""
    try:
        # Convert datetime objects to strings
        serializable_state = {}
        for key, value in state.items():
            if isinstance(value, datetime):
                serializable_state[key] = value.isoformat()
            else:
                serializable_state[key] = value
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(state_file), exist_ok=True)
        
        # Write state to file
        with open(state_file, 'w') as f:
            json.dump(serializable_state, f, indent=2)
            
        logger.debug(f"Pipeline state saved to {state_file}")
        
    except Exception as e:
        logger.error(f"Error saving pipeline state: {e}")

def load_pipeline_state(state_file='pipeline_state.json'):
    """Load the pipeline state from a file."""
    if not os.path.exists(state_file):
        logger.debug(f"No pipeline state file found at {state_file}")
        return {}
        
    try:
        with open(state_file, 'r') as f:
            state = json.load(f)
        
        # Convert datetime strings back to datetime objects
        for key, value in state.items():
            if key.endswith('_at') or key.endswith('_time') or key.endswith('_date'):
                try:
                    state[key] = datetime.fromisoformat(value)
                except (ValueError, TypeError):
                    pass
        
        logger.debug(f"Pipeline state loaded from {state_file}")
        return state
        
    except Exception as e:
        logger.error(f"Error loading pipeline state: {e}")
        return {}

def should_run_incremental(file_path, state):
    """
    Determine if an incremental run is needed based on file checksums.
    Returns True if the file has changed since the last run.
    """
    if not os.path.exists(file_path):
        logger.warning(f"File not found: {file_path}")
        return False
        
    file_name = os.path.basename(file_path)
    current_checksum = get_data_checksum(file_path)
    
    # Get previous checksum from state
    previous_checksum = state.get(f"{file_name}_checksum")
    
    # If no previous checksum or checksums don't match, run incremental
    if not previous_checksum or current_checksum != previous_checksum:
        logger.info(f"Detected changes in {file_name}, running incremental update")
        
        # Update state with new checksum
        state[f"{file_name}_checksum"] = current_checksum
        state[f"{file_name}_last_updated"] = datetime.now()
        
        return True
    
    logger.info(f"No changes detected in {file_name}, skipping incremental update")
    return False

def get_database_stats():
    """Get statistics about the database tables."""
    from src.db.connection import get_connection
    
    stats = {}
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Get table list based on database type
            from config import DB_TYPE
            
            if DB_TYPE == 'sqlite':
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0] for row in cursor.fetchall()]
            else:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema='public'
                """)
                tables = [row[0] for row in cursor.fetchall()]
            
            # Get row counts for each table
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                stats[table] = cursor.fetchone()[0]
                
        return stats
    
    except Exception as e:
        logger.error(f"Error getting database stats: {e}")
        return {}

def validate_data_integrity():
    """Validate data integrity in the database."""
    from src.db.connection import get_connection
    
    integrity_issues = []
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Check for orphaned jobs (jobs with non-existent person)
            cursor.execute("""
                SELECT COUNT(*) 
                FROM jobs j 
                LEFT JOIN people p ON j.person_uuid = p.uuid 
                WHERE p.uuid IS NULL
            """)
            orphaned_jobs = cursor.fetchone()[0]
            
            if orphaned_jobs > 0:
                integrity_issues.append(f"Found {orphaned_jobs} jobs with missing person references")
            
            # Check for inconsistent data in people table
            cursor.execute("""
                SELECT COUNT(*) 
                FROM people 
                WHERE name IS NULL OR name = ''
            """)
            invalid_people = cursor.fetchone()[0]
            
            if invalid_people > 0:
                integrity_issues.append(f"Found {invalid_people} people with missing names")
            
            # Check for inconsistent data in organizations table
            cursor.execute("""
                SELECT COUNT(*) 
                FROM organizations 
                WHERE name IS NULL OR name = ''
            """)
            invalid_orgs = cursor.fetchone()[0]
            
            if invalid_orgs > 0:
                integrity_issues.append(f"Found {invalid_orgs} organizations with missing names")
            
        return integrity_issues
    
    except Exception as e:
        logger.error(f"Error validating data integrity: {e}")
        return [f"Error validating data integrity: {e}"]