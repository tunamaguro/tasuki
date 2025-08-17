-- name: GetAvailableJobs :many
UPDATE 
  tasuki_job j
SET
  status = 'running',
  attempts = j.attempts + 1,
  lease_expires_at = clock_timestamp() + sqlc.arg(lease_interval)::INTERVAL
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
      (scheduled_at <= NOW() AND attempts <= max_attempts AND queue_name = sqlc.arg(queue_name)::TEXT)
    ORDER BY scheduled_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT sqlc.arg(batch_size)
  )
RETURNING j.id, j.job_data;

-- name: HeartBeatJob :exec
UPDATE 
  tasuki_job j
SET
  lease_expires_at = clock_timestamp() + sqlc.arg(lease_interval)::INTERVAL
WHERE
  id = $1;

-- name: CompleteJob :exec
UPDATE 
  tasuki_job j
SET 
  status = 'completed'
WHERE
  id = $1;

-- name: CancelJob :exec
UPDATE
  tasuki_job j
SET
  status = 'canceled'
WHERE
  id = $1;

-- name: RetryJob :exec
UPDATE tasuki_job j
SET 
  status = CASE 
            WHEN j.attempts <= j.max_attempts THEN 'pending'
            ELSE 'failed'
           END,
  scheduled_at = CASE
                  WHEN j.attempts <= j.max_attempts THEN clock_timestamp() + sqlc.arg(interval)::INTERVAL
                  ELSE scheduled_at
                 END 
WHERE 
  id = $1;

-- name: InsertJobOne :exec
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
