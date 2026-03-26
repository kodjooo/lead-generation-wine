ALTER TABLE companies
    ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'yandex_search_api',
    ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE TABLE IF NOT EXISTS search_ops (
    id BIGSERIAL PRIMARY KEY,
    query_hash CHAR(40) NOT NULL,
    operation_id TEXT NOT NULL,
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL DEFAULT 'submitted'
);

CREATE TABLE IF NOT EXISTS search_batch_logs (
    id BIGSERIAL PRIMARY KEY,
    niche TEXT NOT NULL,
    city TEXT,
    country TEXT,
    batch_tag TEXT,
    attempted_queries INTEGER NOT NULL DEFAULT 0,
    inserted_queries INTEGER NOT NULL DEFAULT 0,
    duplicate_queries INTEGER NOT NULL DEFAULT 0,
    scheduled_start TIMESTAMPTZ,
    scheduled_end TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'pending',
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE VIEW daily_summaries AS
SELECT
    date_trunc('day', scheduled_for) AS day,
    COUNT(*) AS total_queries,
    COUNT(*) FILTER (WHERE status = 'pending') AS pending_queries,
    COUNT(*) FILTER (WHERE status = 'in_progress') AS in_progress_queries,
    COUNT(*) FILTER (WHERE status = 'completed') AS completed_queries,
    COUNT(*) FILTER (WHERE status = 'failed') AS failed_queries
FROM serp_queries
GROUP BY 1
ORDER BY 1 DESC;

CREATE OR REPLACE VIEW top_domains AS
SELECT
    c.canonical_domain,
    COUNT(sr.id) AS serp_hits,
    MIN(sr.created_at) AS first_seen_at,
    MAX(sr.created_at) AS last_seen_at
FROM companies c
LEFT JOIN serp_results sr ON sr.domain = c.canonical_domain
GROUP BY c.canonical_domain
ORDER BY serp_hits DESC NULLS LAST;

CREATE OR REPLACE VIEW company_status_view AS
SELECT
    c.id,
    c.canonical_domain,
    c.primary_email,
    c.primary_email_status,
    c.primary_email_note,
    c.status,
    c.opt_out,
    c.first_seen_at,
    c.last_seen_at,
    COUNT(DISTINCT ct.id) AS contacts_count,
    COUNT(DISTINCT om.id) FILTER (WHERE om.status = 'sent') AS emails_sent,
    COUNT(DISTINCT om.id) FILTER (WHERE om.status = 'failed') AS emails_failed
FROM companies c
LEFT JOIN contacts ct ON ct.company_id = c.id
LEFT JOIN outreach_messages om ON om.company_id = c.id
GROUP BY c.id;
