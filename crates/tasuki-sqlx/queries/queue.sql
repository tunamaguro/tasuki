-- name: GetAvailableJobs :many
UPDATE 
  tasuki_job j
SET
  status = 'running'::tasuki_job_status,
  attempts = j.attempts + 1,
  lease_expires_at = clock_timestamp() + sqlc.arg(lease_interval)::INTERVAL,
  lease_token = gen_random_uuid()
WHERE 
  id in (
    SELECT 
      id 
    FROM
      tasuki_job
    WHERE 
      (
        (status = 'pending')
        OR
        (status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at <= NOW())
      )
      AND 
      (scheduled_at <= NOW() AND attempts < max_attempts AND queue_name = sqlc.arg(queue_name)::TEXT)
    ORDER BY scheduled_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT sqlc.arg(batch_size)
  )
RETURNING j.id, j.job_data, j.lease_token::UUID;

-- name: HeartBeatJob :exec
UPDATE 
  tasuki_job j
SET
  lease_expires_at = clock_timestamp() + sqlc.arg(lease_interval)::INTERVAL
WHERE
  id = sqlc.arg(id)
  AND lease_token = sqlc.arg(lease_token);

-- name: CompleteJob :exec
UPDATE 
  tasuki_job j
SET 
  status = 'completed'::tasuki_job_status,
  lease_expires_at = NULL
WHERE
  id = sqlc.arg(id)
  AND lease_token = sqlc.arg(lease_token);

-- name: CancelJob :exec
UPDATE
  tasuki_job j
SET
  status = 'canceled'::tasuki_job_status,
  lease_expires_at = NULL
WHERE
  id = sqlc.arg(id)
  AND lease_token = sqlc.arg(lease_token);

-- name: RetryJob :exec
UPDATE tasuki_job j
SET 
  status = CASE 
            WHEN j.attempts < j.max_attempts THEN 'pending'::tasuki_job_status
            ELSE 'failed'::tasuki_job_status
           END,
  scheduled_at = CASE
                  WHEN j.attempts < j.max_attempts 
                    THEN clock_timestamp() + coalesce(
                      sqlc.narg(interval),
                      make_interval(secs := power(j.attempts, 4.0) * (0.9 + random() * 0.2))
                      )::INTERVAL
                  ELSE scheduled_at
                 END,
  lease_expires_at = NULL
WHERE 
  id = sqlc.arg(id)
  AND lease_token = sqlc.arg(lease_token);

-- name: InsertJobOne :exec
INSERT INTO
  tasuki_job
  (max_attempts, job_data, queue_name)
VALUES
  ($1, $2, $3);

-- name: InsertJobMany :copyfrom
INSERT INTO
  tasuki_job
  (max_attempts, job_data, queue_name)
VALUES
  ($1, $2, $3);

-- name: AddJobNotify :exec
SELECT pg_notify(
  sqlc.arg(channel_name)::TEXT,
  json_build_object('q', sqlc.arg(queue_name)::TEXT)::TEXT
);

-- name: RetryFailedByQueue :exec
UPDATE tasuki_job j
SET
  status = 'pending'::tasuki_job_status,
  attempts = 0,
  scheduled_at = clock_timestamp(),
  lease_expires_at = NULL
WHERE
  j.status = 'failed'::tasuki_job_status
  AND j.attempts < j.max_attempts
  AND j.queue_name = sqlc.arg(queue_name)::TEXT;
