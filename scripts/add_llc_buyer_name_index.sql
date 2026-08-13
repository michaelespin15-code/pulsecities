-- Covering index for the /llc directory's group-by-buyer query.
-- Without it the planner sorts ~17k rows and reads ~30MB per cold build
-- (3.3s). Pre-sorted by name, with bbl and doc_date carried so the
-- aggregate never touches the heap.
-- The partial predicate must match the route's WHERE clause exactly.
-- Run once: psql $DATABASE_URL -f scripts/add_llc_buyer_name_index.sql

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ownership_raw_llc_buyers_by_name
ON ownership_raw (party_name_normalized, bbl, doc_date)
WHERE doc_type = 'DEED' AND party_type = '2'
  AND party_name_normalized LIKE '%LLC%';
