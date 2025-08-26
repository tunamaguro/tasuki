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
RETURNING j.id, j.job_data, j.lease_token::UUID AS lease_token;

-- name: HeartBeatJob :one
UPDATE 
  tasuki_job j
SET
  lease_expires_at = CASE
    WHEN j.status = 'running'::tasuki_job_status
      THEN clock_timestamp() + sqlc.arg(lease_interval)::INTERVAL
    ELSE j.lease_expires_at
  END
WHERE
  id = sqlc.arg(id)
  AND lease_token = sqlc.arg(lease_token)
RETURNING j.status;

-- name: CompleteJob :exec
UPDATE 
  tasuki_job j
SET 
  status = 'completed'::tasuki_job_status
WHERE
  id = sqlc.arg(id)
  AND lease_token = sqlc.arg(lease_token);

-- name: CancelJob :exec
UPDATE
  tasuki_job j
SET
  status = 'canceled'::tasuki_job_status
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
  (max_attempts, job_data, queue_name, scheduled_at)
VALUES
  ($1, $2, $3, clock_timestamp() + sqlc.arg(interval)::INTERVAL);

-- name: InsertJobMany :copyfrom
INSERT INTO
  tasuki_job
  (max_attempts, job_data, queue_name, scheduled_at)
VALUES
  ($1, $2, $3, $4);

-- name: AddJobNotify :exec
SELECT pg_notify(
  sqlc.arg(channel_name)::TEXT,
  json_build_object('q', sqlc.arg(queue_name)::TEXT)::TEXT
);

-- name: CancelJobById :exec
UPDATE
  tasuki_job j
SET 
  status = 'canceled'::tasuki_job_status
WHERE
  id = sqlc.arg(id);

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

-- name: AggregateQueueStat :one
SELECT 
  SUM(
    CASE WHEN status = 'pending'
      THEN 1
      ELSE 0
    END
  ) AS pending,
  SUM(
    CASE WHEN status = 'running'
      THEN 1
      ELSE 0
    END
  ) AS running,
  SUM(
    CASE WHEN status = 'completed'
      THEN 1
      ELSE 0
    END
  ) AS completed,
  SUM(
    CASE WHEN status = 'failed'
      THEN 1
      ELSE 0
    END
  ) AS failed,
  SUM(
    CASE WHEN status = 'canceled'
      THEN 1
      ELSE 0
    END
  ) AS canceled
FROM 
  tasuki_job
WHERE 
  queue_name = sqlc.arg(queue_name);

-- name: CleanJobs :many
DELETE FROM
  tasuki_job
WHERE 
  status = sqlc.arg(job_status)
  AND queue_name = sqlc.arg(queue_name)
RETURNING id;

-- name: ListJobs :many
SELECT
  j.id,
  j.status
FROM
  tasuki_job j
WHERE
  j.created_at < CASE 
    WHEN sqlc.narg(cursor_job_id)::UUID IS NOT NULL
      THEN (
        SELECT 
          created_at
        FROM 
          tasuki_job jj
        WHERE
          jj.id = sqlc.narg(cursor_job_id)::UUID
          AND jj.queue_name = sqlc.narg(queue_name)
        )
    ELSE (
        SELECT
          MAX(created_at)
        FROM
          tasuki_job jj
        WHERE
          jj.queue_name = sqlc.arg(queue_name)
        LIMIT 1
        )
  END
  AND j.queue_name = sqlc.arg(queue_name)
ORDER BY j.created_at DESC, j.id DESC
LIMIT sqlc.arg(page_size);
