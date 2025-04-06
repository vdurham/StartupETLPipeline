CREATE TABLE IF NOT EXISTS founder_features (
    person_uuid TEXT PRIMARY KEY,
    total_companies_founded INTEGER,
    company_categories TEXT,
    avg_company_lifespan REAL,
    total_funding_raised REAL,
    num_aquisitions INTEGER,
    leadership_roles_count INTEGER,
    FOREIGN KEY (person_uuid) REFERENCES people (uuid)
)