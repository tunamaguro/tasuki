-- Add migration script here
CREATE TYPE tasuki_job_status AS ENUM ('pending', 'running', 'completed', 'failed', 'canceled');

CREATE TABLE IF NOT EXISTS  tasuki_job (
    id  UUID NOT NULL DEFAULT gen_random_uuid(),
    status tasuki_job_status NOT NULL DEFAULT 'pending',

    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    
    lease_expires_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    
    job_data jsonb NOT NULL,
    
    PRIMARY KEY (id)
);
