CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS serp_queries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    query_text TEXT NOT NULL,
    query_hash TEXT NOT NULL UNIQUE,
    region_code INTEGER NOT NULL,
    is_night_window BOOLEAN NOT NULL DEFAULT TRUE,
    status TEXT NOT NULL DEFAULT 'pending',
    scheduled_for TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::JSONB
);

CREATE INDEX IF NOT EXISTS idx_serp_queries_status ON serp_queries (status);
CREATE INDEX IF NOT EXISTS idx_serp_queries_scheduled ON serp_queries (scheduled_for);

CREATE TABLE IF NOT EXISTS serp_operations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    query_id UUID NOT NULL REFERENCES serp_queries(id) ON DELETE CASCADE,
    operation_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'created',
    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    retry_count INTEGER NOT NULL DEFAULT 0,
    error_payload JSONB,
    metadata JSONB NOT NULL DEFAULT '{}'::JSONB
);

CREATE INDEX IF NOT EXISTS idx_serp_operations_query ON serp_operations (query_id);
CREATE INDEX IF NOT EXISTS idx_serp_operations_status ON serp_operations (status);

CREATE TABLE IF NOT EXISTS serp_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    operation_id UUID NOT NULL REFERENCES serp_operations(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    domain TEXT NOT NULL,
    title TEXT,
    snippet TEXT,
    position INTEGER,
    language TEXT,
    is_processed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::JSONB
);

CREATE UNIQUE INDEX IF NOT EXISTS uidx_serp_results_operation_url ON serp_results (operation_id, url);
CREATE INDEX IF NOT EXISTS idx_serp_results_domain ON serp_results (domain);
CREATE INDEX IF NOT EXISTS idx_serp_results_processed ON serp_results (is_processed);

CREATE TABLE IF NOT EXISTS companies (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_domain TEXT,
    website_url TEXT,
    industry TEXT,
    region TEXT,
    actual_region TEXT,
    primary_email TEXT,
    primary_email_status TEXT NOT NULL DEFAULT 'unknown',
    primary_email_note TEXT,
    status TEXT NOT NULL DEFAULT 'new',
    dedupe_hash TEXT NOT NULL,
    source_result_id UUID REFERENCES serp_results(id) ON DELETE SET NULL,
    attributes JSONB NOT NULL DEFAULT '{}'::JSONB,
    opt_out BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uidx_companies_dedupe_hash ON companies (dedupe_hash);
CREATE UNIQUE INDEX IF NOT EXISTS uidx_companies_domain ON companies (canonical_domain) WHERE canonical_domain IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_companies_status ON companies (status);

CREATE TABLE IF NOT EXISTS contacts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    contact_type TEXT NOT NULL,
    value TEXT NOT NULL,
    source_url TEXT,
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    quality_score NUMERIC(5,2) NOT NULL DEFAULT 0,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::JSONB
);

CREATE UNIQUE INDEX IF NOT EXISTS uidx_contacts_value_type ON contacts (LOWER(value), contact_type);
CREATE INDEX IF NOT EXISTS idx_contacts_company ON contacts (company_id);

CREATE TABLE IF NOT EXISTS outreach_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    contact_id UUID REFERENCES contacts(id) ON DELETE SET NULL,
    channel TEXT NOT NULL DEFAULT 'email',
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    scheduled_for TIMESTAMPTZ,
    sent_at TIMESTAMPTZ,
    delivery_status TEXT,
    last_error TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_outreach_status ON outreach_messages (status);
CREATE INDEX IF NOT EXISTS idx_outreach_scheduled ON outreach_messages (scheduled_for);

CREATE TABLE IF NOT EXISTS opt_out_registry (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contact_value TEXT NOT NULL,
    contact_type TEXT NOT NULL DEFAULT 'email',
    company_id UUID REFERENCES companies(id) ON DELETE SET NULL,
    source TEXT,
    reason TEXT,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uidx_opt_out_value_type ON opt_out_registry (LOWER(contact_value), contact_type);

CREATE TABLE IF NOT EXISTS processing_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    priority SMALLINT NOT NULL DEFAULT 5,
    scheduled_for TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_processing_jobs_status ON processing_jobs (status);
CREATE INDEX IF NOT EXISTS idx_processing_jobs_schedule ON processing_jobs (scheduled_for);
CREATE INDEX IF NOT EXISTS idx_processing_jobs_type_priority ON processing_jobs (job_type, priority DESC);
