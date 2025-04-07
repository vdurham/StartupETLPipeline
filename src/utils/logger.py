"""
Logging utility
"""
import logging
import os
from datetime import datetime
import sys

from config import LOG_LEVEL, LOG_FILE

def setup_logger():
    """Set up and configure the logger."""
    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(LOG_FILE)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, LOG_LEVEL))
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    return logger

def log_pipeline_start():
    """Log the start of the pipeline."""
    logger = logging.getLogger()
    
    logger.info("=" * 80)
    logger.info(f"PIPELINE STARTED - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)

def log_pipeline_end(start_time):
    """Log the end of the pipeline and execution duration."""
    logger = logging.getLogger()
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    logger.info("=" * 80)
    logger.info(f"PIPELINE COMPLETED - {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Total execution time: {duration}")
    logger.info("=" * 80)

def log_stage(stage_name):
    """Log the start of a pipeline stage."""
    logger = logging.getLogger()
    
    logger.info("-" * 60)
    logger.info(f"STAGE: {stage_name} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("-" * 60)