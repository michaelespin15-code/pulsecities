CREATE INDEX idx_complaints_raw_location ON public.complaints_raw USING gist (location);
CREATE INDEX idx_complaints_raw_type_date ON public.complaints_raw USING btree (complaint_type, created_date);
effective_cache_size = 5242888kB
maintenance_work_mem = 65536kB

shared_buffers = 163848kB
work_mem = 4096kB
