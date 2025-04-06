import pandas as pd
import requests
import json
import sqlite3
import os
from datetime import datetime
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from dotenv import load_dotenv
load_dotenv()

class StartupDataPipeline:
    def __init__(self, api_key, db_path='startup_data.db'):
        self.api_key = api_key
        self.db_path = db_path
        self.api_base_url = "https://lm5zn6zen9.execute-api.us-east-2.amazonaws.com/api"
        self.headers = {"api_key": self.api_key, "content-type": "application/json"}
        
    def extract_csv_data(self, csv_dir):
        """Extract data from CSV files"""
        data = {}
        try:
            # Read CSV files
            data['people'] = pd.read_csv(os.path.join(csv_dir, 'people.csv'))
            data['organizations'] = pd.read_csv(os.path.join(csv_dir, 'organizations.csv'))
            data['jobs'] = pd.read_csv(os.path.join(csv_dir, 'jobs.csv'))
            
            print(f"Extracted {len(data['people'])} people records from CSV")
            print(f"Extracted {len(data['organizations'])} organization records from CSV")
            print(f"Extracted {len(data['jobs'])} job records from CSV")
            
            return data
        except Exception as e:
            print(f"Error extracting CSV data: {e}")
            return None
    
    def verify_api_connection(self):
        """Verify the API connection is working"""
        try:
            response = requests.get(f"{self.api_base_url}/", headers=self.headers)
            if response.status_code == 200:
                result = response.json()
                if result.get("Authenticated"):
                    print("API connection verified successfully")
                    return True
            print(f"API authentication failed: {response.text}")
            return False
        except Exception as e:
            print(f"API connection error: {e}")
            return False
    
    def get_organization_data(self, domain):
        """Get organization data from API"""
        try:
            payload = {"domain": domain}
            response = requests.post(
                f"{self.api_base_url}/org", 
                headers=self.headers,
                data=json.dumps(payload)
            )
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error fetching organization data for {domain}: {response.text}")
                return None
        except Exception as e:
            print(f"API error for organization {domain}: {e}")
            return None
    
    def get_person_data(self, linkedin_url):
        """Get person data from API"""
        try:
            payload = {"linkedin_url": linkedin_url}
            response = requests.post(
                f"{self.api_base_url}/person", 
                headers=self.headers,
                data=json.dumps(payload)
            )
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error fetching person data for {linkedin_url}: {response.text}")
                return None
        except Exception as e:
            print(f"API error for person {linkedin_url}: {e}")
            return None
    
    def clean_people_data(self, people_df):
        """Clean and standardize people data"""
        # Handle missing values
        people_df['gender'] = people_df['gender'].fillna('unknown')
        
        # Standardize location fields
        people_df['country_code'] = people_df['country_code'].str.upper()
        
        # Extract domains from LinkedIn URLs
        people_df['linkedin_domain'] = people_df['linkedin_url'].str.extract(r'linkedin\.com/in/([^/]+)')
        
        return people_df
    
    def clean_organization_data(self, org_df):
        """Clean and standardize organization data"""
        # Handle missing values
        org_df['employee_count'] = org_df['employee_count'].fillna('unknown')
        org_df['status'] = org_df['status'].fillna('unknown')
        
        # Convert funding to numeric
        org_df['total_funding_usd'] = pd.to_numeric(org_df['total_funding_usd'], errors='coerce')
        
        # Standardize category lists
        org_df['category_list'] = org_df['category_list'].fillna('')
        org_df['category_array'] = org_df['category_list'].str.split(',')
        
        return org_df
    
    def clean_jobs_data(self, jobs_df):
        """Clean and standardize jobs data"""
        # Handle date fields
        jobs_df['started_on'] = pd.to_datetime(jobs_df['started_on'], errors='coerce')
        jobs_df['ended_on'] = pd.to_datetime(jobs_df['ended_on'], errors='coerce')
        
        # Fill missing is_current values
        jobs_df['is_current'] = jobs_df['is_current'].fillna(False)
        
        # Standardize job titles
        jobs_df['title'] = jobs_df['title'].str.lower()
        
        # Flag founder roles
        founder_keywords = ['founder', 'co-founder', 'cofounder']
        jobs_df['is_founder'] = jobs_df['title'].str.contains('|'.join(founder_keywords), case=False)
        
        return jobs_df
    
    def create_database_schema(self):
        """Create the database schema"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create people table
        cursor.execute()
        
        # Create organizations table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS organizations (
            uuid TEXT PRIMARY KEY,
            name TEXT,
            legal_name TEXT,
            domain TEXT,
            homepage_url TEXT,
            country_code TEXT,
            state_code TEXT,
            city TEXT,
            status TEXT,
            short_description TEXT,
            category_list TEXT,
            employee_count TEXT,
            founded_on DATE,
            total_funding_usd REAL,
            num_funding_rounds INTEGER,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        ''')
        
        # Create jobs table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            uuid TEXT PRIMARY KEY,
            person_uuid TEXT,
            org_uuid TEXT,
            started_on DATE,
            ended_on DATE,
            is_current BOOLEAN,
            title TEXT,
            job_type TEXT,
            is_founder BOOLEAN,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            FOREIGN KEY (person_uuid) REFERENCES people (uuid),
            FOREIGN KEY (org_uuid) REFERENCES organizations (uuid)
        )
        ''')
        
        # Create founder_features table for similarity analysis
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS founder_features (
            person_uuid TEXT PRIMARY KEY,
            total_companies_founded INTEGER,
            company_categories TEXT,
            avg_company_lifespan REAL,
            total_funding_raised REAL,
            exits_count INTEGER,
            job_titles TEXT,
            leadership_roles_count INTEGER,
            FOREIGN KEY (person_uuid) REFERENCES people (uuid)
        )
        ''')
        
        # Create company_features table for similarity analysis
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS company_features (
            org_uuid TEXT PRIMARY KEY,
            founder_backgrounds TEXT,
            similar_companies TEXT,
            growth_stage TEXT,
            business_model TEXT,
            technologies TEXT,
            markets TEXT,
            FOREIGN KEY (org_uuid) REFERENCES organizations (uuid)
        )
        ''')
        
        conn.commit()
        conn.close()
        
        print("Database schema created successfully")
    
    def load_data_to_db(self, people_df, org_df, jobs_df):
        """Load processed data into the database"""
        conn = sqlite3.connect(self.db_path)
        
        # Load people data
        people_df.to_sql('people', conn, if_exists='replace', index=False)
        
        # Load organizations data
        org_df.to_sql('organizations', conn, if_exists='replace', index=False)
        
        # Load jobs data
        jobs_df.to_sql('jobs', conn, if_exists='replace', index=False)
        
        conn.close()
        
        print("Data loaded to database successfully")
    
    def extract_founder_features(self):
        """Extract features for founder similarity analysis"""
        conn = sqlite3.connect(self.db_path)
        
        # Query to identify founders and their companies
        founder_query = '''
        SELECT p.uuid, p.name, j.org_uuid, o.name as company_name, 
               o.category_list, o.total_funding_usd, o.founded_on,
               j.title, j.started_on, j.ended_on
        FROM people p
        JOIN jobs j ON p.uuid = j.person_uuid
        JOIN organizations o ON j.org_uuid = o.uuid
        WHERE j.is_founder = 1
        '''
        
        founders_df = pd.read_sql(founder_query, conn)
        
        # Group by person to calculate features
        founder_features = []
        
        for person_uuid, group in founders_df.groupby('uuid'):
            # Calculate founder features
            feature = {
                'person_uuid': person_uuid,
                'total_companies_founded': len(group['org_uuid'].unique()),
                'company_categories': ','.join(group['category_list'].dropna().unique()),
                'avg_company_lifespan': 0,  # Would need to calculate from founded_on and current date
                'total_funding_raised': group['total_funding_usd'].sum(),
                'exits_count': 0,  # Would need additional data
                'job_titles': ','.join(group['title'].dropna().unique()),
                'leadership_roles_count': len(group)
            }
            
            founder_features.append(feature)
        
        # Create DataFrame from features
        features_df = pd.DataFrame(founder_features)
        
        # Save to database
        features_df.to_sql('founder_features', conn, if_exists='replace', index=False)
        
        conn.close()
        
        print(f"Extracted features for {len(features_df)} founders")
        
    def calculate_founder_similarity(self, founder_uuid):
        """Calculate similarity between a founder and all other founders"""
        conn = sqlite3.connect(self.db_path)
        
        # Get all founder features
        features_df = pd.read_sql('SELECT * FROM founder_features', conn)
        
        # Get people data for names
        people_df = pd.read_sql('SELECT uuid, name FROM people', conn)
        
        # Merge for display
        features_df = features_df.merge(people_df, left_on='person_uuid', right_on='uuid')
        
        # Get target founder
        target = features_df[features_df['person_uuid'] == founder_uuid].iloc[0]
        
        # Calculate numerical similarity (example using simple approach)
        # In a real implementation, you'd use more sophisticated techniques
        features_df['funding_diff'] = abs(features_df['total_funding_raised'] - target['total_funding_raised'])
        features_df['companies_diff'] = abs(features_df['total_companies_founded'] - target['total_companies_founded'])
        
        # Calculate text similarity for categories
        vectorizer = TfidfVectorizer()
        tfidf_matrix = vectorizer.fit_transform(features_df['company_categories'].fillna(''))
        target_vector = tfidf_matrix[features_df['person_uuid'] == founder_uuid]
        cosine_similarities = cosine_similarity(target_vector, tfidf_matrix).flatten()
        
        # Assign similarities
        features_df['category_similarity'] = cosine_similarities
        
        # Combined similarity score (simple weighted average)
        features_df['similarity_score'] = (
            features_df['category_similarity'] * 0.6 +
            (1 / (1 + features_df['companies_diff'])) * 0.2 +
            (1 / (1 + features_df['funding_diff'] / 1000000)) * 0.2  # Normalize by millions
        )
        
        # Sort by similarity
        similar_founders = features_df[features_df['person_uuid'] != founder_uuid].sort_values(
            'similarity_score', ascending=False
        )
        
        conn.close()
        
        return similar_founders[['name', 'similarity_score', 'company_categories', 'total_companies_founded']]
    
    def run_pipeline(self, csv_dir):
        """Run the complete data pipeline"""
        # Step 1: Extract data
        print("Step 1: Extracting data...")
        csv_data = self.extract_csv_data(csv_dir)
        if not csv_data:
            print("Failed to extract CSV data. Pipeline stopped.")
            return False
        
        # Step 2: Verify API connection
        print("Step 2: Verifying API connection...")
        if not self.verify_api_connection():
            print("Failed to verify API connection. Pipeline will continue with CSV data only.")
        
        # Step 3: Clean and transform data
        print("Step 3: Cleaning and transforming data...")
        people_clean = self.clean_people_data(csv_data['people'])
        org_clean = self.clean_organization_data(csv_data['organizations'])
        jobs_clean = self.clean_jobs_data(csv_data['jobs'])
        
        # Step 4: Create database schema
        print("Step 4: Creating database schema...")
        self.create_database_schema()
        
        # Step 5: Load data to database
        print("Step 5: Loading data to database...")
        self.load_data_to_db(people_clean, org_clean, jobs_clean)
        
        # Step 6: Extract features for similarity analysis
        print("Step 6: Extracting features for similarity analysis...")
        self.extract_founder_features()
        
        print("Pipeline completed successfully")
        return True


if __name__ == "__main__":
    # Initialize the pipeline
    pipeline = StartupDataPipeline(api_key=os.getenv("API_KEY"))
    
    # Run the pipeline
    pipeline.run_pipeline(csv_dir="./data")
    
    # Example of similarity analysis
    similar_founders = pipeline.calculate_founder_similarity("b0e4e511-a1f1-162d-005a-006a94ee35f6")  # David Sacks
    print("\nSimilar founders to David Sacks:")
    print(similar_founders.head(5))