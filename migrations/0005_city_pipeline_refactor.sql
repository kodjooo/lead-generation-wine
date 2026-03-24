ALTER TABLE search_batch_logs
    ALTER COLUMN niche DROP NOT NULL;

ALTER TABLE search_batch_logs
    ADD COLUMN IF NOT EXISTS entity_scope TEXT;

DROP VIEW IF EXISTS daily_summaries;
DROP VIEW IF EXISTS top_domains;
DROP VIEW IF EXISTS company_status_view;

CREATE OR REPLACE VIEW daily_summaries AS
SELECT
    date_trunc('day', scheduled_for) AS day,
    COUNT(*) AS total_queries,
    COUNT(*) FILTER (WHERE metadata ->> 'entity_type' = 'mall') AS mall_queries,
    COUNT(*) FILTER (WHERE metadata ->> 'entity_type' = 'real_estate_agency') AS agency_queries,
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
    c.industry AS entity_type,
    c.region AS source_city,
    COUNT(sr.id) AS serp_hits,
    MIN(sr.created_at) AS first_seen_at,
    MAX(sr.created_at) AS last_seen_at
FROM companies c
LEFT JOIN serp_results sr ON sr.domain = c.canonical_domain
GROUP BY c.canonical_domain, c.industry, c.region
ORDER BY serp_hits DESC NULLS LAST;

CREATE OR REPLACE VIEW company_status_view AS
SELECT
    c.id,
    c.name,
    c.canonical_domain,
    c.industry AS entity_type,
    c.region AS source_city,
    c.status,
    c.opt_out,
    c.first_seen_at,
    c.last_seen_at,
    COUNT(DISTINCT ct.id) AS contacts_count,
    COUNT(DISTINCT om.id) FILTER (WHERE om.status = 'scheduled') AS emails_scheduled,
    COUNT(DISTINCT om.id) FILTER (WHERE om.status = 'sent') AS emails_sent,
    COUNT(DISTINCT om.id) FILTER (WHERE om.status = 'failed') AS emails_failed
FROM companies c
LEFT JOIN contacts ct ON ct.company_id = c.id
LEFT JOIN outreach_messages om ON om.company_id = c.id
GROUP BY c.id;
