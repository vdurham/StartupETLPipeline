#!/usr/bin/env python
"""
Main script to run the Ensemble Similarity Pipeline
"""
import os
import sys
import logging
import argparse
from datetime import datetime
import time

# Add project root to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import INCREMENTAL_MODE
from src.etl.extract import DataExtractor
from src.etl.transform import DataTransformer
from src.etl.load import DataLoader
from src.db.connection import init_database
from src.api.client import ApiClient
from src.utils.logger import setup_logger, log_pipeline_start, log_pipeline_end, log_stage
from src.utils.helpers import (
    load_pipeline_state, save_pipeline_state, 
    get_database_stats, validate_data_integrity
)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run the Ensemble Similarity Pipeline')
    
    parser.add_argument(
        '--incremental', 
        action='store_true',
        help='Run in incremental mode (only process new/updated data)'
    )
    
    parser.add_argument(
        '--full', 
        action='store_true',
        help='Run in full mode (process all data)'
    )
    
    parser.add_argument(
        '--initialize-db', 
        action='store_true',
        help='Initialize the database schema before running the pipeline'
    )
    
    return parser.parse_args()

def run_pipeline(args):
    """Run the Ensemble Similarity Pipeline."""
    # Setup logger
    logger = setup_logger()
    
    # Record start time
    start_time = datetime.now()
    
    # Log pipeline start
    log_pipeline_start()
    
    # Determine if running in incremental mode
    incremental_mode = INCREMENTAL_MODE
    if args.full:
        incremental_mode = False
    elif args.incremental:
        incremental_mode = True
    
    logger.info(f"Running pipeline in {'incremental' if incremental_mode else 'full'} mode")
    
    # Load pipeline state
    pipeline_state = load_pipeline_state()
    
    # Initialize metrics collection
    pipeline_metrics = {
        'start_time': start_time,
        'end_time': None,
        'duration_seconds': None,
        'incremental_mode': incremental_mode,
        'extract_metrics': {},
        'transform_metrics': {},
        'load_metrics': {},
        'data_integrity_issues': [],
        'database_stats_before': {},
        'database_stats_after': {}
    }
    
    try:
        # Initialize database if requested
        if args.initialize_db:
            log_stage("Database Initialization")
            logger.info("Initializing database schema")
            init_database()
        
        # Get database stats before pipeline run
        pipeline_metrics['database_stats_before'] = get_database_stats()
        
        # Validate data integrity only if requested
        log_stage("Data Validation")
        logger.info("Validating data integrity")
        
        integrity_issues = validate_data_integrity()
        pipeline_metrics['data_integrity_issues'] = integrity_issues
        
        if integrity_issues:
            logger.warning(f"Found {len(integrity_issues)} data integrity issues:")
            for issue in integrity_issues:
                logger.warning(f"  - {issue}")
        else:
            logger.info("No data integrity issues found")
                
        # Initialize components
        try:
            api_client = ApiClient()
        except Exception as e:
            logger.error(f"Failed to initialize API client, continuing without: {e}")
            api_client = None

        extractor = DataExtractor(api_client)
        transformer = DataTransformer()
        loader = DataLoader()
        
        # Extract phase
        extracted_data = None

        log_stage("Extract")
        extract_start_time = time.time()
        
        logger.info(f"Extracting data (incremental={incremental_mode})")
        extracted_data = extractor.extract_all_data(incremental_mode)
        
        # Collect metrics
        extract_end_time = time.time()
        pipeline_metrics['extract_metrics'] = {
            'duration_seconds': extract_end_time - extract_start_time,
            'organizations_count': len(extracted_data['organizations']['csv_data']),
            'people_count': len(extracted_data['people']['csv_data']),
            'jobs_count': len(extracted_data['jobs']['csv_data']),
            'api_organizations_count': len(extracted_data['organizations']['api_data']),
            'api_people_count': len(extracted_data['people']['api_data']),
            'api_job_history_count': sum(len(jobs) for jobs in extracted_data.get('api_job_history', {}).values()),
        }
        
        # Update pipeline state with extraction timestamp
        pipeline_state['last_extract_time'] = datetime.now()
        save_pipeline_state(pipeline_state)
        
        # Transform phase
        transformed_data = None
        log_stage("Transform")
        transform_start_time = time.time()
        
        logger.info("Transforming extracted data")
        transformed_data = transformer.transform_all_data(extracted_data)
        
        # Collect metrics
        transform_end_time = time.time()
        pipeline_metrics['transform_metrics'] = {
            'duration_seconds': transform_end_time - transform_start_time,
            'organizations_count': len(transformed_data['organizations']),
            'people_count': len(transformed_data['people']),
            'jobs_count': len(transformed_data['jobs']),
        }
        
        # Update pipeline state with transformation timestamp
        pipeline_state['last_transform_time'] = datetime.now()
        save_pipeline_state(pipeline_state)

        
        # Load phase
        log_stage("Load")
        load_start_time = time.time()
        
        logger.info("Loading transformed data into database")
        loader.load_all_data(transformed_data)
        
        # Collect metrics
        load_end_time = time.time()
        pipeline_metrics['load_metrics'] = {
            'duration_seconds': load_end_time - load_start_time,
        }
        
        # Update pipeline state with load timestamp
        pipeline_state['last_load_time'] = datetime.now()
        save_pipeline_state(pipeline_state)
        
        # Validate data integrity after pipeline run
        log_stage("Data Validation")
        logger.info("Validating data integrity")
        
        integrity_issues = validate_data_integrity()
        pipeline_metrics['data_integrity_issues'] = integrity_issues
        
        if integrity_issues:
            logger.warning(f"Found {len(integrity_issues)} data integrity issues:")
            for issue in integrity_issues:
                logger.warning(f"  - {issue}")
        else:
            logger.info("No data integrity issues found")
        
        # Get database stats after pipeline run
        pipeline_metrics['database_stats_after'] = get_database_stats()
        
        # Log changes in database stats
        logger.info("Database statistics:")
        for table, count in pipeline_metrics['database_stats_after'].items():
            before_count = pipeline_metrics['database_stats_before'].get(table, 0)
            diff = count - before_count
            diff_str = f"(+{diff})" if diff > 0 else f"({diff})" if diff < 0 else "(no change)"
            logger.info(f"  - {table}: {count} rows {diff_str}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        pipeline_metrics['error'] = str(e)
    finally:
        # Update pipeline metrics
        pipeline_metrics['end_time'] = datetime.now()
        pipeline_metrics['duration_seconds'] = (pipeline_metrics['end_time'] - start_time).total_seconds()
        
        # Log pipeline end
        log_pipeline_end(start_time)
        
        # Update pipeline state with run timestamp
        pipeline_state['last_run_time'] = datetime.now()
        pipeline_state['last_run_status'] = 'success' if 'error' not in pipeline_metrics else 'error'
        save_pipeline_state(pipeline_state)

if __name__ == "__main__":
    args = parse_args()
    run_pipeline(args)