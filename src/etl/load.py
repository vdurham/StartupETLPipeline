"""
Data loading module
"""
import logging
import pandas as pd
from datetime import datetime
import json
import numpy as np
from src.etl.founder_features import process_founder_features

from config import CHECKPOINT_INTERVAL, DB_TYPE
from src.db.connection import get_connection

logger = logging.getLogger(__name__)

class DataLoader:
    """Load transformed data into the database."""
    
    def __init__(self):
        self.load_timestamp = datetime.now()
    
    def _get_table_columns(self, conn, table_name):
        """Get the columns for a table in the database."""
        cursor = conn.cursor()
        
        # Query depends on database type
        if DB_TYPE == 'sqlite':
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in cursor.fetchall()]
        else:
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
            """)
            columns = [row[0] for row in cursor.fetchall()]
        
        return columns
    
    def _prepare_row_for_insert(self, row, columns):
        """Prepare a row for insertion, handling JSON fields and None values."""
        prepared_row = {}
        
        for col in columns:
            if col in row:
                value = row[col]
                
                if isinstance(value, (list, np.ndarray)):
                    if len(value) == 0:
                        prepared_row[col] = None
                    else:
                        prepared_row[col] = json.dumps(value.tolist() if isinstance(value, np.ndarray) else value)
                elif pd.isna(value):
                    prepared_row[col] = None
                elif isinstance(value, np.integer):
                    prepared_row[col] = int(value)
                elif isinstance(value, np.floating):
                    prepared_row[col] = float(value)
                elif isinstance(value, np.bool_):
                    prepared_row[col] = bool(value)
                # Convert JSON strings to strings if needed
                elif isinstance(value, str) and (value.startswith('[') or value.startswith('{')):
                    try:
                        json.loads(value)
                        prepared_row[col] = value
                    except:
                        prepared_row[col] = str(value)
                # Convert lists/dicts to JSON strings
                elif isinstance(value, (list, dict)):
                    prepared_row[col] = json.dumps(value)
                # Convert any other numpy array or complex object to string
                elif isinstance(value, (np.ndarray, np.generic)) or not isinstance(value, (str, int, float, bool, type(None))):
                    prepared_row[col] = str(value)
                else:
                    prepared_row[col] = value
            else:
                prepared_row[col] = None
        
        return prepared_row
    
    def load_data(self, data_df, table_name, primary_key='uuid', batch_size=None):
        """
        Load data into the database in batches.
        """
        if batch_size is None:
            batch_size = CHECKPOINT_INTERVAL
            
        logger.info(f"Loading {len(data_df)} {table_name} records into the database")
        
        with get_connection() as conn:
            # Process in batches
            for i in range(0, len(data_df), batch_size):
                batch = data_df.iloc[i:i+batch_size]
                logger.info(f"Processing batch {i//batch_size + 1}/{(len(data_df)-1)//batch_size + 1} with {len(batch)} records")
                
                self._bulk_upsert(conn, table_name, batch, primary_key)
                conn.commit()
                
        logger.info(f"Successfully loaded {table_name} data")
    
    def load_all_data(self, transformed_data):
        """Load all transformed data into the database."""
        logger.info("Starting data loading")
        
        with get_connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            
            try:
                # Load organizations first (foreign key dependencies)
                self.load_data(transformed_data['organizations'], 'organizations')
                
                self.load_data(transformed_data['people'], 'people')
                
                self.load_data(transformed_data['jobs'], 'jobs')
                conn.execute("COMMIT")
                
                logger.info("Processing and loading founder features")
                process_founder_features(
                    conn=conn,
                    jobs_df=transformed_data['jobs'],
                    organizations_df=transformed_data['organizations'],
                    people_df=transformed_data['people']
                )
                                
                logger.info("Data loading completed successfully")
                
            except Exception as e:
                # Rollback on error
                logger.error(f"Error during data loading: {e}")
                conn.execute("ROLLBACK")
                raise

    def _bulk_upsert(self, conn, table_name, df, primary_key='uuid'):
        """
        Bulk upsert operation.
        """
        cursor = conn.cursor()
        columns = self._get_table_columns(conn, table_name)
        
        # Filter DataFrame to only include columns that exist in the table
        df_filtered = df[[col for col in df.columns if col in columns]]
        
        conn.execute("BEGIN TRANSACTION")
        failed_records_count = 0
        
        try:
            for _, row in df_filtered.iterrows():
                prepared_row = self._prepare_row_for_insert(row, columns)
                
                # Check if the record exists
                cursor.execute(
                    f"SELECT 1 FROM {table_name} WHERE {primary_key} = ?",
                    (prepared_row[primary_key],)
                )
                exists = cursor.fetchone() is not None
                
                if exists:
                    # Update existing record
                    update_cols = [col for col in prepared_row.keys() if col != primary_key]
                    if update_cols:
                        set_clause = ", ".join([f"{col} = ?" for col in update_cols])
                        values = [prepared_row[col] for col in update_cols]
                        values.append(prepared_row[primary_key])
                        
                        cursor.execute(
                            f"UPDATE {table_name} SET {set_clause} WHERE {primary_key} = ?",
                            values
                        )
                else:
                    # Insert new record
                    cols = ", ".join(prepared_row.keys())
                    placeholders = ", ".join(["?" for _ in prepared_row.keys()])
                    values = list(prepared_row.values())
                    
                    try:
                        cursor.execute(
                            f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})",
                            values
                        )
                    except Exception as e:
                        failed_records_count += 1
            
            conn.execute("COMMIT")
            if failed_records_count > 0:
                logger.warning(f"Failed to insert/update {failed_records_count} records")
            
        except Exception as e:
            logger.error(f"Error during bulk upsert: {e}")
            conn.execute("ROLLBACK")
            raise