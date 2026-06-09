CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS workers (
    worker_id uuid PRIMARY KEY,
    endpoint text NOT NULL,
    status text NOT NULL DEFAULT 'running', -- initializing, running, unreachable, error, stopped, shutting_down
    cpus int NOT NULL,
    memory int NOT NULL, -- amount of memory available in MB
    disk_space int NOT NULL, -- amount of disk space available in MB
    max_concurrent_queries int NOT NULL DEFAULT 10,
    memory_used_mb int,
    cpu_usage double precision,
    started_at timestamptz NOT NULL DEFAULT now(),
    last_heartbeat_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS users (
    user_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    sub text NOT NULL UNIQUE,
    email text,
    name text,
    created_at timestamptz NOT NULL DEFAULT now(),
    last_login_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS queries (
    query_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    worker_id uuid REFERENCES workers(worker_id),
    user_id uuid REFERENCES users(user_id),
    status text NOT NULL DEFAULT 'pending', -- pending, running, completed, failed, cancelled
    query text NOT NULL,
    error text,
    cpus int, -- NULL is no limit, 0 is operational query
    memory int, -- amount of memory requested in MB
    disk_space int, -- amount of disk space requested in MB
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    completed_at timestamptz,
    result_row_count bigint
);

CREATE TABLE IF NOT EXISTS sheets (
    sheet_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    name text NOT NULL,
    sql text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE workers ADD COLUMN IF NOT EXISTS memory_used_mb int;
ALTER TABLE workers ADD COLUMN IF NOT EXISTS cpu_usage double precision;
ALTER TABLE queries ADD COLUMN IF NOT EXISTS result_row_count bigint;
ALTER TABLE queries ADD COLUMN IF NOT EXISTS user_id uuid REFERENCES users(user_id);

CREATE INDEX IF NOT EXISTS idx_queries_pending ON queries (created_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_queries_running_worker ON queries (worker_id) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS idx_workers_heartbeat ON workers (last_heartbeat_at) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions (expires_at);
CREATE INDEX IF NOT EXISTS idx_queries_user_created ON queries (user_id, created_at DESC)
    WHERE user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sheets_user ON sheets (user_id, updated_at DESC);
