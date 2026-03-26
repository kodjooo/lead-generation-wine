DROP VIEW IF EXISTS company_status_view;

ALTER TABLE companies
    DROP COLUMN IF EXISTS name;

ALTER TABLE companies
    ADD COLUMN IF NOT EXISTS primary_email TEXT;

ALTER TABLE companies
    ADD COLUMN IF NOT EXISTS primary_email_status TEXT NOT NULL DEFAULT 'unknown';

ALTER TABLE companies
    ADD COLUMN IF NOT EXISTS primary_email_note TEXT;

UPDATE companies
SET primary_email = primary_contact.value,
    primary_email_status = CASE
        WHEN primary_contact.value IS NOT NULL THEN 'identified'
        ELSE 'unknown'
    END,
    primary_email_note = CASE
        WHEN primary_contact.value IS NOT NULL THEN NULL
        ELSE primary_email_note
    END
FROM (
    SELECT DISTINCT ON (company_id)
        company_id,
        value
    FROM contacts
    WHERE contact_type = 'email'
    ORDER BY company_id, is_primary DESC, quality_score DESC, first_seen_at ASC
) AS primary_contact
WHERE companies.id = primary_contact.company_id;

CREATE OR REPLACE VIEW company_status_view AS
SELECT
    c.id,
    c.canonical_domain,
    c.industry AS entity_type,
    c.region AS source_city,
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
