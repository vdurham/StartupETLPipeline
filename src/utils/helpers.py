"""
Helper functions
"""
import logging


logger = logging.getLogger(__name__)

def get_database_stats():
    """Get statistics about the database tables."""
    from src.db.connection import get_connection
    
    stats = {}
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            
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