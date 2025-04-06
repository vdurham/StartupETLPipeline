CREATE TABLE IF NOT EXISTS people (
    uuid TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT,
    cb_url TEXT,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    country_code TEXT,
    region_or_state TEXT,
    city TEXT,
    featured_job_organization_uuid TEXT,
    featured_job_organization_name TEXT,
    featured_job_title TEXT,
    linkedin_url TEXT,
    twitter_url TEXT,
    -- API enriched fields
    headline TEXT,
    seniority TEXT,
    functions TEXT,
    departments TEXT,
    -- Metadata fields
    source TEXT NOT NULL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    last_processed_at TIMESTAMP,
    -- Foreign key reference to featured org
    FOREIGN KEY (featured_job_organization_uuid) 
        REFERENCES organizations(uuid) ON DELETE SET NULL
);
