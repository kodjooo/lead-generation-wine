ALTER TABLE companies
    ADD COLUMN IF NOT EXISTS actual_region TEXT;

UPDATE companies
SET actual_region = NULLIF(attributes ->> 'detected_city', '')
WHERE COALESCE(actual_region, '') = '';

DROP VIEW IF EXISTS top_domains;
DROP VIEW IF EXISTS company_status_view;

CREATE OR REPLACE VIEW top_domains AS
SELECT
    c.canonical_domain,
    c.industry AS entity_type,
    c.region AS source_city,
    c.actual_region,
    COUNT(sr.id) AS serp_hits,
    MIN(sr.created_at) AS first_seen_at,
    MAX(sr.created_at) AS last_seen_at
FROM companies c
LEFT JOIN serp_results sr ON sr.domain = c.canonical_domain
GROUP BY c.canonical_domain, c.industry, c.region, c.actual_region
ORDER BY serp_hits DESC NULLS LAST;

CREATE OR REPLACE VIEW company_status_view AS
SELECT
    c.id,
    c.canonical_domain,
    c.industry AS entity_type,
    c.region AS source_city,
    c.actual_region,
    c.primary_email,
    c.primary_email_status,
    c.primary_email_note,
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
