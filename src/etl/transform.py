"""
Data transformation module
"""
import pandas as pd
import logging
from datetime import datetime
import json
import re


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
            value = value.strip()
            if value.startswith('[') and value.endswith(']'):
                value = value[1:-1]
            
            # Split by comma and clean values
            items = [item.strip() for item in value.split(',') if item.strip()]
            return json.dumps(items) if items else None
            
        return None
    
    def _merge_api_org_data(self, org_df, api_data):
        """Merge API organization data with CSV data."""
        if not api_data:
            return org_df
            
        api_rows = []
        
        for domain, org_api_data in api_data.items():
            if not org_api_data:
                continue
                
            # Extract and flatten the API data
            api_row = {
                'domain': domain,
                'name': org_api_data.get('name'),            
                'industry': org_api_data.get('industry'),
                'industries': org_api_data.get('industries'),
                'secondary_industries': org_api_data.get('secondary_industries'),
                'keywords': org_api_data.get('keywords'),
                'technology_names': org_api_data.get('technology_names'),
                'city': org_api_data.get('city'),
                'region': org_api_data.get('state'),
                'postal_code': org_api_data.get('postal_code'),
                'address': org_api_data.get('street_address'),
                'annual_revenue': int(revenue) if (revenue := org_api_data.get('annual_revenue')) is not None else None,
                'total_funding_usd': int(funding) if (funding := org_api_data.get('total_funding')) is not None else None,
                'latest_funding_stage': org_api_data.get('latest_funding_stage'),
                'last_funding_on': org_api_data.get('latest_funding_round_date'),
                'linkedin_url': org_api_data.get('linkedin_url'),
                'twitter_url': org_api_data.get('twitter_url'),
                'homepage_url': org_api_data.get('website_url'),
                'source': 'api'
            }
            
            api_rows.append(api_row)
        
        if not api_rows:
            return org_df
            
        api_df = pd.DataFrame(api_rows)
        
        if 'last_funding_on' in api_df.columns:
            api_df['last_funding_on'] = pd.to_datetime(api_df['last_funding_on'], errors='coerce')
        
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

                # Use max total_funding_usd if available
                if 'total_funding_usd_api' in merged_df.columns:
                    merged_df['total_funding_usd'] = merged_df[['total_funding_usd', 'total_funding_usd_api']].max(axis=1)

                merged_df = merged_df.drop(f"{col}_api", axis=1)
        
        # Update source to indicate if enriched with API data
        merged_df.loc[merged_df['domain'].isin(api_data.keys()), 'source'] = 'csv+api'
        
        return merged_df
    
    def _merge_api_people_data(self, people_df, api_data):
        """Merge API people data with CSV data."""
        if not api_data:
            return people_df
        
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
                'functions': person_api_data.get('functions'),
                'departments': person_api_data.get('departments'),
                'city': person_api_data.get('city'),
                'region': person_api_data.get('state'),
                'country': person_api_data.get('country'),
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
    
    def transform_organizations(self, data):
        """Transform organization data."""
        logger.info("Transforming organization data")
        
        df = data['csv_data'].copy()
        api_data = data['api_data']
        
        # Clean and standardize fields
        df['name'] = df['name'].apply(self._standardize_names)
        df['legal_name'] = df['legal_name'].apply(self._standardize_names)
        df['domain'] = df['domain'].str.lower().str.strip() if 'domain' in df.columns else None
        df['employee_count'] = df['employee_count'].replace('unknown', None)
       
        # Parse list fields
        list_columns = ['category_list', 'category_groups_list', 'roles']
        for col in list_columns:
            if col in df.columns:
                df[col] = df[col].apply(self._parse_list_field)
        
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
        df['gender'] = df['gender'].replace('not_provided', None)
        
        # Set last processed timestamp
        df['last_processed_at'] = self.transformation_timestamp
        
        # Merge with API data
        enriched_df = self._merge_api_people_data(df, api_data)
        
        logger.info(f"Transformed {len(enriched_df)} people records")
        
        return enriched_df
    
    def transform_jobs(self, data):
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
        
        logger.info(f"Transformed {len(df)} job records")
        
        return df
    
  
    def validate_foreign_keys(self, transformed_data):
        """
        Validate and prepare data for foreign key constraints.
        Returns updated transformed data with foreign key issues handled.
        """
        logger.info("Validating foreign key relationships")
        
        org_df = transformed_data['organizations'].copy()
        people_df = transformed_data['people'].copy()
        jobs_df = transformed_data['jobs'].copy()
        
        # Valid organization UUIDs
        valid_org_uuids = set(org_df['uuid'].dropna().unique())
        logger.info(f"Found {len(valid_org_uuids)} valid organization UUIDs")
        
        # Valid people UUIDs
        valid_people_uuids = set(people_df['uuid'].dropna().unique())
        logger.info(f"Found {len(valid_people_uuids)} valid people UUIDs")
        
        # Fix featured_job_organization_uuid in people
        invalid_org_refs = people_df[
            ~people_df['featured_job_organization_uuid'].isna() & 
            ~people_df['featured_job_organization_uuid'].isin(valid_org_uuids)
        ]
        
        if len(invalid_org_refs) > 0:
            logger.warning(f"Found {len(invalid_org_refs)} people with invalid featured_job_organization_uuid references")
            
            # Set invalid organization references to NULL
            people_df.loc[
                ~people_df['featured_job_organization_uuid'].isna() & 
                ~people_df['featured_job_organization_uuid'].isin(valid_org_uuids),
                'featured_job_organization_uuid'
            ] = None
            
            logger.info("Set invalid featured_job_organization_uuid references to NULL")
        
        # Fix organization references in jobs
        invalid_job_orgs = jobs_df[
            ~jobs_df['org_uuid'].isna() & 
            ~jobs_df['org_uuid'].isin(valid_org_uuids)
        ]
        
        if len(invalid_job_orgs) > 0:
            logger.warning(f"Found {len(invalid_job_orgs)} jobs with invalid org_uuid references")
            
            # Set invalid organization references to NULL
            jobs_df.loc[
                ~jobs_df['org_uuid'].isna() & 
                ~jobs_df['org_uuid'].isin(valid_org_uuids),
                'org_uuid'
            ] = None
            
            logger.info("Set invalid org_uuid references in jobs to NULL")
        
        # Handle jobs with invalid person references
        invalid_job_people = jobs_df[~jobs_df['person_uuid'].isin(valid_people_uuids)]
        
        if len(invalid_job_people) > 0:
            logger.warning(f"Found {len(invalid_job_people)} jobs with invalid person_uuid references that will be filtered out")
            
            # Filter out jobs with invalid person references
            jobs_df = jobs_df[jobs_df['person_uuid'].isin(valid_people_uuids)]
            
            logger.info(f"Filtered out {len(invalid_job_people)} jobs with invalid person references")
        
        # Return the updated DataFrames
        return {
            'organizations': org_df,
            'people': people_df,
            'jobs': jobs_df
        }
    
    def transform_all_data(self, extracted_data):
        """Transform all extracted data and validate foreign keys."""
        logger.info("Starting data transformation")
        
        # Transform organizations first
        organizations_df = self.transform_organizations(extracted_data['organizations'])
        
        # Transform people
        people_df = self.transform_people(extracted_data['people'])
        
        # Transform jobs
        jobs_df = self.transform_jobs(extracted_data['jobs'])
        
        transformed_data = {
            'organizations': organizations_df,
            'people': people_df,
            'jobs': jobs_df
        }
        
        # Validate and handle foreign key relationships
        validated_data = self.validate_foreign_keys(transformed_data)
        
        return validated_data