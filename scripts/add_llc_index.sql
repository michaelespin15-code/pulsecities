-- Partial index on ownership_raw for LLC acquisition queries.
-- Eliminates full table scans on party_name_normalized LIKE '%LLC%'
-- in the top-risk neighborhood scoring query.
-- Run once: psql $DATABASE_URL -f scripts/add_llc_index.sql

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ownership_raw_llc_acquisitions
ON ownership_raw (doc_date DESC, bbl)
WHERE party_type = '2'
  AND doc_type IN ('DEED', 'DEEDP', 'ASST')
  AND party_name_normalized LIKE '%LLC%';
