-- Hand-written: an executor that released an agent it could not serve is skipped by its placement until expire_time.
CREATE TABLE executor_refusal (
    agent TEXT NOT NULL,
    organization_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    executor_id TEXT NOT NULL,
    expire_time TIMESTAMP NOT NULL,
    PRIMARY KEY (agent, organization_id, user_id, executor_id)
);
