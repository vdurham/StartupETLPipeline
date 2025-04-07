# Startup Similarity Evaluation Pipeline

## Overview
This pipeline processes and analyzes data about startups and founders from multiple sources to identify similarities between companies and founders. The system helps VC firms build quick conviction on startups being evaluated by providing accurate insights based on comprehensive data.

## Setup Instructions

### Environment Setup
Create an `.env` file in the project root with the following parameters:
```
DB_TYPE = 'sqlite'
DB_PATH = <DB PATH>
# File paths
DATA_DIR = 'data'
SCHEMA_DIR = 'schemas'
# API Configuration
API_BASE_URL = <API URL>
API_KEY = <API KEY>
# Logging Configuration
LOG_LEVEL = 'INFO'
# Parallel Processing Configuration
MAX_WORKERS = 2
```

### Directory Structure
Ensure the following directories exist:
- `data/` - For CSV input files
- `schemas/` - Contains SQL schema files
- `logs/` - For pipeline execution logs

### Data Files
Place the following CSV files in the `data/` directory:
- `organizations.csv`
- `people.csv`
- `jobs.csv`

## Running the Pipeline

### First-time Setup
To initialize the database and run the pipeline for the first time:
```
python run_pipeline.py --initialize-db
```

## Pipeline Architecture
The pipeline follows an ETL (Extract, Transform, Load) architecture:
1. **Extract**: Data is gathered from CSV files and enriched with API data
2. **Transform**: Data is cleaned, standardized, and integrated
3. **Load**: Processed data is loaded into a SQLite database
4. **Analysis**: Similarity analysis is performed on founders and companies

## Visualization
To visualize the pipeline architecture and data model diagrams included in the presentation:
- Use a web-based Mermaid diagram viewer: https://mermaid.live/
