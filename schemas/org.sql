CREATE TABLE IF NOT EXISTS organizations (
    uuid TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    legal_name TEXT,
    type TEXT,
    cb_url TEXT,
    domain TEXT,
    homepage_url TEXT UNIQUE,
    country_code TEXT,
    region_or_state TEXT,
    city TEXT,
    address TEXT,
    postal_code TEXT,
    status TEXT,
    short_description TEXT,
    category_list TEXT,
    category_groups_list TEXT,
    primary_role TEXT,
    roles TEXT,
    num_funding_rounds INTEGER,
    total_funding_usd REAL,
    founded_on DATE,
    last_funding_on DATE,
    employee_count TEXT,
    linkedin_url TEXT,
    twitter_url TEXT,
    -- API enriched fields
    industry TEXT,
    industries TEXT,
    keywords TEXT,
    secondary_industries TEXT,
    technology_names TEXT,
    annual_revenue REAL,
    latest_funding_stage TEXT,
    estimated_num_employees INTEGER,
    -- Metadata fields
    source TEXT NOT NULL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    last_processed_at TIMESTAMP
);