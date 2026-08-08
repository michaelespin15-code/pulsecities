-- Expression index resolving /llc/{slug} URLs back to deed-party names.
-- The expression must match _SLUG_SQL in api/routes/frontend.py exactly or
-- the planner will not use it.
-- Run once: psql $DATABASE_URL -f scripts/add_entity_slug_index.sql

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ownership_raw_entity_slug
ON ownership_raw (btrim(regexp_replace(lower(party_name_normalized), '[^a-z0-9]+', '-', 'g'), '-'))
WHERE doc_type = 'DEED';
