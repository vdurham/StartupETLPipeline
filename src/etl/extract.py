"""
Data extraction module
"""
import os
import pandas as pd
import logging
from datetime import datetime
import json
from collections import defaultdict

from config import (
    ORGANIZATIONS_CSV, PEOPLE_CSV, JOBS_CSV, 
    BATCH_SIZE, INCREMENTAL_MODE
)
from src.api.client import ApiClient
from src.db.connection import get_connection

logger = logging.getLogger(__name__)

class DataExtractor:
    """Extract data from CSV files and enrich with API data."""
    
    def __init__(self, api_client=None):
        self.api_client = api_client
        self.extraction_timestamp = datetime.now()
    
    def extract_csv_data(self, file_path, incremental=INCREMENTAL_MODE):
        """
        Extract data from a CSV file.
        If incremental is True, only extract rows that are new or updated since last processing.
        """
        if not os.path.exists(file_path):
            logger.error(f"CSV file not found: {file_path}")
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        logger.info(f"Extracting data from {file_path}")
        
        try:
            # Read CSV file into DataFrame
            df = pd.read_csv(file_path, low_memory=False)
            
            # Add source and processing metadata
            df['source'] = 'csv'
            df['last_processed_at'] = None
            
            # Convert timestamps to datetime
            if 'created_at' in df.columns:
                df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
            if 'updated_at' in df.columns:
                df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
            
            total_rows = len(df)
            logger.info(f"Extracted {total_rows} rows from {file_path}")
            
            if incremental:
                # Filter for incremental updates if enabled
                with get_connection() as conn:
                    cursor = conn.cursor()
                    
                    # Get table name from file name
                    table_name = os.path.basename(file_path).split('.')[0]
                    
                    # Get last processed timestamp for each uuid
                    cursor.execute(f"SELECT uuid, last_processed_at FROM {table_name} WHERE source = 'csv'")
                    
                    last_processed = {}
                    for row in cursor.fetchall():
                        last_processed[row[0]] = row[1]
                    
                    # Filter rows that are new or updated since last processing
                    if last_processed:
                        filtered_df = df[
                            (~df['uuid'].isin(last_processed.keys())) |  # New rows
                            (df.apply(lambda row: row['uuid'] in last_processed and 
                                     (pd.isna(last_processed[row['uuid']]) or 
                                      pd.to_datetime(row['updated_at']) > 
                                      pd.to_datetime(last_processed[row['uuid']])), 
                                     axis=1))
                        ]
                        
                        logger.info(f"Filtered {len(filtered_df)}/{total_rows} rows for incremental processing")
                        return filtered_df
                    
                    # If no previous processing, use all rows
                    logger.info(f"No previous processing found, using all {total_rows} rows")
            
            return df
            
        except Exception as e:
            logger.error(f"Error extracting data from {file_path}: {e}")
            raise
    
    def extract_organizations(self, incremental=INCREMENTAL_MODE):
        """Extract organization data from CSV and enrich with API data."""
        # Extract from CSV
        df = self.extract_csv_data(ORGANIZATIONS_CSV, incremental)
        
        # Get unique domains for API enrichment
        domains = df['domain'].dropna().unique().tolist()

        api_data = {}
        if self.api_client is not None:
            logger.info(f"Extracting API data for {len(domains)} organizations")
        
            # Batch processing for domains
            for i in range(0, len(domains), BATCH_SIZE):
                batch = domains[i:i+BATCH_SIZE]
                logger.info(f"Processing batch {i//BATCH_SIZE + 1}/{(len(domains)-1)//BATCH_SIZE + 1} with {len(batch)} domains")
                batch_data = self.api_client.batch_get_organization_data(batch)
                api_data.update(batch_data)
            
            logger.info(f"Successfully enriched {len(api_data)}/{len(domains)} organizations with API data")
            
        # Return both DataFrames and API data
        return {
            'csv_data': df,
            'api_data': api_data
        }


    
    def extract_people(self, incremental=INCREMENTAL_MODE):
        """Extract people data from CSV and enrich with API data."""
        # Extract from CSV
        df = self.extract_csv_data(PEOPLE_CSV, incremental)

        api_data = {}
        if self.api_client is not None:
        
            # Get unique LinkedIn URLs for API enrichment
            linkedin_urls = df['linkedin_url'].dropna().unique().tolist()
            logger.info(f"Extracting API data for {len(linkedin_urls)} people")
            
            # Batch processing for LinkedIn URLs
            for i in range(0, len(linkedin_urls), BATCH_SIZE):
                batch = linkedin_urls[i:i+BATCH_SIZE]
                logger.info(f"Processing batch {i//BATCH_SIZE + 1}/{(len(linkedin_urls)-1)//BATCH_SIZE + 1} with {len(batch)} LinkedIn URLs")
                batch_data = self.api_client.batch_get_person_data(batch)
                api_data.update(batch_data)
            
            logger.info(f"Successfully enriched {len(api_data)}/{len(linkedin_urls)} people with API data")
        
        # Return both DataFrames and API data
        return {
            'csv_data': df,
            'api_data': api_data
        }

    
    
    def extract_jobs(self, incremental=INCREMENTAL_MODE):
        """Extract job data from CSV."""
        # Extract from CSV
        df = self.extract_csv_data(JOBS_CSV, incremental)
        
        # Convert date fields
        date_columns = ['started_on', 'ended_on']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Convert boolean fields
        if 'is_current' in df.columns:
            df['is_current'] = df['is_current'].map({'TRUE': True, 'FALSE': False}).fillna(False)
        
        logger.info(f"Extracted {len(df)} job records")
        
        return {'csv_data': df}
    
    def extract_all_data(self, incremental=INCREMENTAL_MODE):
        """Extract all data from CSV files and API."""
        logger.info(f"Starting data extraction (incremental={incremental})")
        
        organizations_data = self.extract_organizations(incremental)
        people_data = self.extract_people(incremental)
        jobs_data = self.extract_jobs(incremental)

        job_history = defaultdict(list)
        if self.api_client is not None:         

            # Extract job history from people API data
            for linkedin_url, person_data in people_data['api_data'].items():
                if 'employment_history' in person_data and person_data['employment_history']:
                    for job in person_data['employment_history']:
                        job_info = {
                            'person_linkedin_url': linkedin_url,
                            'organization_name': job.get('organization_name'),
                            'title': job.get('title'),
                            'start_date': job.get('start_date'),
                            'end_date': job.get('end_date'),
                            'is_current': job.get('current', False),
                            'description': job.get('description'),
                        }
                        job_history[linkedin_url].append(job_info)
            
        return {
            'organizations': organizations_data,
            'people': people_data,
            'jobs': jobs_data,
            'api_job_history': dict(job_history)
        }
