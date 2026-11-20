-- A queue's completed jobs, one row per hour: what ComputeQueueStats reads past a day, rebuilt
-- from the jobs by the roll-up routine and kept past their retention.
CREATE TABLE scheduler.queue_hour_stats (
    -- The queue's resource name (queues/{queue}).
    queue TEXT NOT NULL,
    -- The hour's start, UTC.
    hour TIMESTAMP NOT NULL,
    succeeded_count BIGINT NOT NULL,
    failed_count BIGINT NOT NULL,
    cancelled_count BIGINT NOT NULL,
    attempt_count BIGINT NOT NULL,
    -- Seconds by histogram bucket (go/scheduler/histogram) and the exact longest; both NULL
    -- when no job was measured.
    wait_histogram BIGINT[],
    wait_max_seconds DOUBLE PRECISION,
    run_histogram BIGINT[],
    run_max_seconds DOUBLE PRECISION,
    PRIMARY KEY (queue, hour)
);

-- Every queue's hours, and the retention sweep.
CREATE INDEX queue_hour_stats_hour_idx ON scheduler.queue_hour_stats (hour);
