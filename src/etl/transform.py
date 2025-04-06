"""
Data transformation module
"""
import pandas as pd
import logging
from datetime import datetime
import json
import numpy as np
import re
from collections import defaultdict

logger = logging.getLogger(__name__)

class DataTransformer:
    """Transform and clean data from CSV and API sources."""
    
    def __init__(self):
        self.transformation_timestamp = datetime.now()
    
    def _standardize_names(self, value):
        """Standardize name format."""
        if pd.isna(value):
            return None
        # Remove excess whitespace
        value = re.sub(r'\s+', ' ', str(value).strip())
        return value
    
    def _parse_list_field(self, value):
        """Parse and standardize list fields (comma-separated strings)."""
        if pd.isna(value):
            return None
            
        if isinstance(value, list):
            return json.dumps(value)
            
        # Handle string representations of lists
        if isinstance(value, str):
            # Remove brackets if present and split by commas
            value = value.strip()
            if value.startswith('[') and value.endswith(']'):
                value = value[1:-1]
            
            # Split by comma and clean values
            items = [item.strip() for item in value.split(',') if item.strip()]
            return json.dumps(items) if items else None
            
        return None
    
    def _standardize_employee_count(self, value):
        """Standardize employee count ranges."""
        if pd.isna(value):
            return None
            
        value = str(value).strip()
        
        # Handle exact values first
        if value.isdigit():
            return value
            
        # Handle ranges like "101-250"
        range_match = re.match(r'(\d+)-(\d+)', value)
        if range_match:
            return value
            
        # Handle descriptive values
        mapping = {
            '1-50': '1-50',
            '51-100': '51-100',
            '101-1000': '101-1000',
            '1001+': '1001+'
        }
        
        # Normalize different formats to standard ranges
        for standardized, variations in mapping.items():
            if value.lower() in variations.lower():
                return standardized
                
        return value
    
    def _merge_api_org_data(self, org_df, api_data):
        """Merge API organization data with CSV data."""
        if not api_data:
            return org_df
            
        # Create a new DataFrame for API data
        api_rows = []
        
        for domain, org_api_data in api_data.items():
            if not org_api_data:
                continue
                
            # Extract and flatten the API data
            api_row = {
                'domain': domain,
                'name': org_api_data.get('name'),
                'industry': org_api_data.get('industry'),
                'industries': json.dumps(org_api_data.get('industries', [])),
                'secondary_industries': json.dumps(org_api_data.get('secondary_industries', [])),
                'keywords': json.dumps(org_api_data.get('keywords', [])),
                'technology_names': json.dumps(org_api_data.get('technology_names', [])),
                'short_description': org_api_data.get('short_description'),
                'city': org_api_data.get('city'),
                'state': org_api_data.get('state'),
                'country': org_api_data.get('country'),
                'postal_code': org_api_data.get('postal_code'),
                'street_address': org_api_data.get('street_address'),
                'founded_year': org_api_data.get('founded_year'),
                'annual_revenue': org_api_data.get('annual_revenue'),
                'estimated_num_employees': org_api_data.get('estimated_num_employees'),
                'total_funding': org_api_data.get('total_funding'),
                'latest_funding_stage': org_api_data.get('latest_funding_stage'),
                'latest_funding_round_date': org_api_data.get('latest_funding_round_date'),
                'facebook_url': org_api_data.get('facebook_url'),
                'linkedin_url': org_api_data.get('linkedin_url'),
                'twitter_url': org_api_data.get('twitter_url'),
                'website_url': org_api_data.get('website_url'),
                'source': 'api'
            }
            
            api_rows.append(api_row)
        
        if not api_rows:
            return org_df
            
        # Create DataFrame from API data
        api_df = pd.DataFrame(api_rows)
        
        # Convert date fields
        if 'latest_funding_round_date' in api_df.columns:
            api_df['latest_funding_round_date'] = pd.to_datetime(api_df['latest_funding_round_date'], errors='coerce')
        
        # Merge with original DataFrame based on domain
        merged_df = pd.merge(
            org_df, 
            api_df, 
            on='domain', 
            how='left', 
            suffixes=('', '_api')
        )
        
        # For each API column, use the API value if the original is empty
        for col in api_df.columns:
            if col in merged_df.columns and f"{col}_api" in merged_df.columns:
                merged_df[col] = merged_df[col].fillna(merged_df[f"{col}_api"])
                merged_df = merged_df.drop(f"{col}_api", axis=1)
        
        # Update source to indicate if enriched with API data
        merged_df.loc[merged_df['domain'].isin(api_data.keys()), 'source'] = 'csv+api'
        
        return merged_df
    
    def _merge_api_people_data(self, people_df, api_data):
        """Merge API people data with CSV data."""
        if not api_data:
            return people_df
        
        # Create DataFrame for API data
        api_rows = []
        
        for linkedin_url, person_api_data in api_data.items():
            if not person_api_data:
                continue
                
            # Extract and flatten the API data
            api_row = {
                'linkedin_url': linkedin_url,
                'name': person_api_data.get('name'),
                'first_name': person_api_data.get('first_name'),
                'last_name': person_api_data.get('last_name'),
                'headline': person_api_data.get('headline'),
                'seniority': person_api_data.get('seniority'),
                'functions': json.dumps(person_api_data.get('functions', [])),
                'departments': json.dumps(person_api_data.get('departments', [])),
                'subdepartments': json.dumps(person_api_data.get('subdepartments', [])),
                'city': person_api_data.get('city'),
                'state': person_api_data.get('state'),
                'country': person_api_data.get('country'),
                'github_url': person_api_data.get('github_url'),
                'facebook_url': person_api_data.get('facebook_url'),
                'twitter_url': person_api_data.get('twitter_url'),
                'source': 'api'
            }
            
            api_rows.append(api_row)
        
        if not api_rows:
            return people_df
            
        # Create DataFrame from API data
        api_df = pd.DataFrame(api_rows)
        
        # Merge with original DataFrame based on LinkedIn URL
        merged_df = pd.merge(
            people_df,
            api_df,
            on='linkedin_url',
            how='left',
            suffixes=('', '_api')
        )
        
        # For each API column, use the API value if the original is empty
        for col in api_df.columns:
            if col in merged_df.columns and f"{col}_api" in merged_df.columns:
                merged_df[col] = merged_df[col].fillna(merged_df[f"{col}_api"])
                merged_df = merged_df.drop(f"{col}_api", axis=1)
        
        # Update source to indicate if enriched with API data
        merged_df.loc[merged_df['linkedin_url'].isin(api_data.keys()), 'source'] = 'csv+api'
        
        return merged_df
    
    def _enrich_jobs_with_api_data(self, jobs_df, people_df, api_job_history):
        """Enrich job data with API job history."""
        if not api_job_history:
            return jobs_df
        
        # Create a mapping from person UUID to LinkedIn URL
        uuid_to_linkedin = dict(zip(people_df['uuid'], people_df['linkedin_url']))
        
        # Create a mapping from LinkedIn URL to person UUID
        linkedin_to_uuid = {url: uuid for uuid, url in uuid_to_linkedin.items() if url}
        
        # Create new rows for API job data
        new_job_rows = []
        
        for linkedin_url, jobs in api_job_history.items():
            if linkedin_url not in linkedin_to_uuid:
                continue
                
            person_uuid = linkedin_to_uuid[linkedin_url]
            person_row = people_df[people_df['uuid'] == person_uuid].iloc[0]
            person_name = person_row['name']
            
            for job in jobs:
                # Generate a UUID for the job
                job_uuid = f"api-{person_uuid}-{len(new_job_rows)}"
                
                new_job_rows.append({
                    'uuid': job_uuid,
                    'name': f"{person_name} {job['title']} @ {job['organization_name']}",
                    'type': 'job',
                    'person_uuid': person_uuid,
                    'person_name': person_name,
                    'org_name': job['organization_name'],
                    'title': job['title'],
                    'started_on': job['start_date'],
                    'ended_on': job['end_date'],
                    'is_current': job['is_current'],
                    'description': job['description'],
                    'source': 'api',
                    'created_at': self.transformation_timestamp,
                    'updated_at': self.transformation_timestamp
                })
        
        if not new_job_rows:
            return jobs_df
            
        # Create DataFrame from new job rows
        new_jobs_df = pd.DataFrame(new_job_rows)
        
        # Convert date fields
        date_columns = ['started_on', 'ended_on']
        for col in date_columns:
            if col in new_jobs_df.columns:
                new_jobs_df[col] = pd.to_datetime(new_jobs_df[col], errors='coerce')
        
        # Append new jobs to existing jobs DataFrame
        # Only add jobs that don't already exist (based on person, org, title, dates)
        combined_df = pd.concat([jobs_df, new_jobs_df], ignore_index=True)
        
        # Remove duplicates
        # Consider jobs as duplicates if they have the same person_uuid, org_name, title, and dates
        # Keep the CSV data as the master source
        combined_df = combined_df.drop_duplicates(
            subset=['person_uuid', 'org_name', 'title', 'started_on', 'ended_on'],
            keep='first'
        )
        
        return combined_df
    
    def transform_organizations(self, data):
        """Transform organization data."""
        logger.info("Transforming organization data")
        
        df = data['csv_data'].copy()
        api_data = data['api_data']
        
        # Clean and standardize fields
        df['name'] = df['name'].apply(self._standardize_names)
        df['legal_name'] = df['legal_name'].apply(self._standardize_names)
        df['domain'] = df['domain'].str.lower().str.strip() if 'domain' in df.columns else None
       
        # Parse list fields
        list_columns = ['category_list', 'category_groups_list', 'roles']
        for col in list_columns:
            if col in df.columns:
                df[col] = df[col].apply(self._parse_list_field)
        
        # Standardize employee count
        if 'employee_count' in df.columns:
            df['employee_count'] = df['employee_count'].apply(self._standardize_employee_count)
        
        # Convert date fields
        date_columns = ['founded_on', 'last_funding_on', 'closed_on']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Set last processed timestamp
        df['last_processed_at'] = self.transformation_timestamp
        
        # Merge with API data
        enriched_df = self._merge_api_org_data(df, api_data)
        
        logger.info(f"Transformed {len(enriched_df)} organization records")
        
        return enriched_df
    
    def transform_people(self, data):
        """Transform people data."""
        logger.info("Transforming people data")
        
        df = data['csv_data'].copy()
        api_data = data['api_data']
        
        # Clean and standardize fields
        df['name'] = df['name'].apply(self._standardize_names)
        df['first_name'] = df['first_name'].apply(self._standardize_names)
        df['last_name'] = df['last_name'].apply(self._standardize_names)
        
        # Set last processed timestamp
        df['last_processed_at'] = self.transformation_timestamp
        
        # Merge with API data
        enriched_df = self._merge_api_people_data(df, api_data)
        
        logger.info(f"Transformed {len(enriched_df)} people records")
        
        return enriched_df
    
    def transform_jobs(self, data, people_df, api_job_history):
        """Transform job data."""
        logger.info("Transforming job data")
        
        df = data['csv_data'].copy()
        
        # Clean and standardize fields
        df['name'] = df['name'].apply(self._standardize_names)
        df['title'] = df['title'].apply(self._standardize_names)
        df['org_name'] = df['org_name'].apply(self._standardize_names)
        df['person_name'] = df['person_name'].apply(self._standardize_names)
        
        # Convert date fields
        date_columns = ['started_on', 'ended_on']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Set last processed timestamp
        df['last_processed_at'] = self.transformation_timestamp
        
        # Enrich with API job history
        enriched_df = self._enrich_jobs_with_api_data(df, people_df, api_job_history)
        
        logger.info(f"Transformed {len(enriched_df)} job records")
        
        return enriched_df
    
    def transform_all_data(self, extracted_data):
        """Transform all extracted data."""
        logger.info("Starting data transformation")
        
        # Transform organizations first
        organizations_df = self.transform_organizations(extracted_data['organizations'])
        
        # Transform people
        people_df = self.transform_people(extracted_data['people'])
        
        # Transform jobs, enriched with API job history
        jobs_df = self.transform_jobs(
            extracted_data['jobs'],
            people_df,
            extracted_data['api_job_history']
        )
        
        return {
            'organizations': organizations_df,
            'people': people_df,
            'jobs': jobs_df
        }