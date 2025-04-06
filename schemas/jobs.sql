CREATE TABLE IF NOT EXISTS jobs (
    uuid TEXT PRIMARY KEY,
    name TEXT,
    type TEXT,
    cb_url TEXT,
    person_uuid TEXT NOT NULL,
    person_name TEXT,
    org_uuid TEXT,
    org_name TEXT,
    title TEXT,
    job_type TEXT,
    started_on DATE,
    ended_on DATE,
    is_current BOOLEAN,
    -- API enriched fields
    description TEXT,
    -- Metadata fields
    source TEXT NOT NULL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    last_processed_at TIMESTAMP,
    -- Foreign keys
    FOREIGN KEY (person_uuid) REFERENCES people(uuid) ON DELETE CASCADE,
    FOREIGN KEY (org_uuid) REFERENCES organizations(uuid) ON DELETE SET NULL
);

