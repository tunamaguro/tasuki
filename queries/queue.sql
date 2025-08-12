-- name: GetAvailableJobs :many
UPDATE 
  tasuki_job j
SET
  status = 'running',
  attempts = j.attempts + 1,
  lease_expires_at = clock_timestamp() + make_interval(secs := sqlc.arg(lease_interval)::INTERVAL)
WHERE 
  id in (
    SELECT 
      id 
    FROM
      tasuki_job
    WHERE 
      (status = 'pending' AND scheduled_at <= NOW())
      OR
      (status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at <= NOW())
    ORDER BY scheduled_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT sqlc.arg(batch_size)
  )
RETURNING j.id, j.job_data;


-- name: InsertJobOne :exec
INSERT INTO
  tasuki_job
  (max_attempts, job_data)
VALUES
  ($1,$2);
