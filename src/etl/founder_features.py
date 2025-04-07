"""
Module for founder features processing - designed to be called from the main pipeline
but also executable as a standalone script if needed.
"""
import logging
import pandas as pd
import json

logger = logging.getLogger(__name__)

def process_founder_features(conn=None, jobs_df=None, organizations_df=None, people_df=None):
    """
    Process founder features based on jobs and organizations data.
    """
    logger.info("Processing founder features")
    
    try:     
        founder_jobs = jobs_df[
            (jobs_df['job_type'] == 'founder') | 
            (jobs_df['title'].str.lower().str.contains('founder', na=False))
        ].copy()
        orgs = organizations_df
        people = people_df
        
        # Transform data
        features_df = transform_founder_data(founder_jobs, orgs, people)
        
        # Load data
        load_founder_features(conn, features_df)
        
        return features_df
    
    except Exception as e:
        logger.error(f"Error processing founder features: {e}")
        return None

def transform_founder_data(founder_jobs, orgs, people):
    """Transform founder data to create founder features"""
    logger.info("Transforming founder data")
    
    # Unique founders
    founders = founder_jobs[['person_uuid', 'person_name']].drop_duplicates()
    logger.info(f"Processing features for {len(founders)} unique founders")
    
    founder_features = []
    
    for _, founder in founders.iterrows():
        person_uuid = founder['person_uuid']
        
        # Get all jobs for this founder
        founder_job_rows = founder_jobs[founder_jobs['person_uuid'] == person_uuid]
        
        # Get unique organizations founded by this person
        founded_org_uuids = founder_job_rows['org_uuid'].dropna().unique().tolist()
        
        # Get organization data for these orgs
        founded_orgs = orgs[orgs['uuid'].isin(founded_org_uuids)] if len(orgs) > 0 and not orgs.empty else pd.DataFrame()
        
        if founded_orgs.empty:
            continue

        # Total companies founded
        total_companies_founded = len(founded_org_uuids)
        
        # Company categories
        company_categories = []
        if not founded_orgs.empty and 'category_list' in founded_orgs.columns:
            for _, org in founded_orgs.iterrows():
                if pd.notna(org.get('category_list')) and isinstance(org['category_list'], str):
                    try:
                        categories = json.loads(org['category_list'])
                        if isinstance(categories, list):
                            company_categories.extend(categories)
                    except json.JSONDecodeError:
                        # Handle plain text category lists
                        categories = org['category_list'].split(',')
                        company_categories.extend([c.strip() for c in categories])
        
        company_categories = list(set(company_categories))
        
        total_funding_raised = 0
        if not founded_orgs.empty and 'total_funding_usd' in founded_orgs.columns:
            total_funding_raised = founded_orgs['total_funding_usd'].sum()
        
        num_acquisitions = 0
        if not founded_orgs.empty and 'status' in founded_orgs.columns:
            num_acquisitions = len(founded_orgs[founded_orgs['status'] == 'acquired'])
        
        leadership_roles_count = len(founder_job_rows[founder_job_rows['job_type'] == 'executive'])
        
        feature = {
            'person_uuid': person_uuid,
            'total_companies_founded': total_companies_founded,
            'company_categories': json.dumps(company_categories),
            'total_funding_raised': total_funding_raised,
            'num_aquisitions': num_acquisitions,
            'leadership_roles_count': leadership_roles_count
        }
        
        founder_features.append(feature)
    
    return pd.DataFrame(founder_features)

def load_founder_features(conn, feature_df):
    """Load founder features into the database"""
    logger.info(f"Loading {len(feature_df)} founder feature records into the database")
    
    if feature_df.empty:
        logger.warning("No founder features to load")
        return
    
    cursor = conn.cursor()

    # Insert or replace data
    for _, row in feature_df.iterrows():
        cursor.execute("""
        INSERT OR REPLACE INTO founder_features (
            person_uuid, total_companies_founded, company_categories,
            total_funding_raised, num_aquisitions, leadership_roles_count
        ) VALUES (?, ?, ?, ?, ?, ?)
        """, (
            row['person_uuid'],
            row['total_companies_founded'],
            row['company_categories'],
            row['total_funding_raised'],
            row['num_aquisitions'],
            row['leadership_roles_count']
        ))
    
    conn.commit()
    
    logger.info("Founder features loaded successfully")