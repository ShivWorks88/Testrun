-- One Postgres container hosts three local databases:
--   source_replica  synthetic EHR / LIS / Billing replicas (filled by data-gen)
--   airflow         Airflow metadata
--   ops_state       local stand-in for Cloud SQL when running the API on your laptop
CREATE DATABASE source_replica;
CREATE DATABASE airflow;
CREATE DATABASE ops_state;
