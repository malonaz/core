-- Hand-written: the lease on a RUNNING agent's turn, held by one replica of the service.
CREATE TABLE agent_lease (
    organization_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    -- The holding replica's process: a replica restarted on the same address holds none of its leases.
    replica UUID NOT NULL,
    -- The holding replica's advertised gRPC address, where its turns are cancelled and streamed.
    address TEXT NOT NULL,
    expire_time TIMESTAMP NOT NULL,
    PRIMARY KEY (organization_id, user_id, agent_id)
);

CREATE INDEX agent_lease_replica_idx ON agent_lease (replica);
