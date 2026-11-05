package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/malonaz/core/gengo/ai/agent/model"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
)

// now is the database clock, which every lease is measured against: replicas' clocks may disagree.
const now = "(NOW() AT TIME ZONE 'UTC')"

var (
	agentStateIdle    = int16(agentpb.Agent_STATE_IDLE)
	agentStateRunning = int16(agentpb.Agent_STATE_RUNNING)
	leaseSelectQuery  = "SELECT agent_lease.replica::TEXT, agent_lease.address, agent_lease.expire_time, agent_lease.expire_time <= " +
		now + " AS expired FROM agent_lease "
)

// Lease is the lease on a RUNNING agent's turn, held by one replica.
type Lease struct {
	// The holding replica's process id.
	Replica string `db:"replica"`
	// The holding replica's advertised gRPC address.
	Address    string    `db:"address"`
	ExpireTime time.Time `db:"expire_time"`
	// Whether the lease expired, by the database clock at read time.
	Expired bool `db:"expired"`
}

// ClaimLease takes an agent's lease for a replica, for duration, unless another replica holds it unexpired. It
// reports whether it did. Claiming a lease the replica holds renews it.
func (t *Tx) ClaimLease(ctx context.Context, organizationID, userID, agentID, replica, address string, duration time.Duration) (bool, error) {
	query := `INSERT INTO agent_lease (organization_id, user_id, agent_id, replica, address, expire_time)
		VALUES ($1, $2, $3, $4, $5, ` + now + ` + $6::BIGINT * INTERVAL '1 microsecond')
		ON CONFLICT (organization_id, user_id, agent_id) DO UPDATE SET
			replica = EXCLUDED.replica, address = EXCLUDED.address, expire_time = EXCLUDED.expire_time
		WHERE agent_lease.expire_time <= ` + now + ` OR agent_lease.replica = EXCLUDED.replica`
	commandTag, err := t.tx.Exec(ctx, query, organizationID, userID, agentID, replica, address, duration.Microseconds())
	if err != nil {
		return false, fmt.Errorf("claiming lease: %w", err)
	}
	return commandTag.RowsAffected() > 0, nil
}

// ReleaseLease drops an agent's lease if the replica holds it. It reports whether it did.
func (t *Tx) ReleaseLease(ctx context.Context, organizationID, userID, agentID, replica string) (bool, error) {
	query := "DELETE FROM agent_lease WHERE organization_id = $1 AND user_id = $2 AND agent_id = $3 AND replica = $4"
	commandTag, err := t.tx.Exec(ctx, query, organizationID, userID, agentID, replica)
	if err != nil {
		return false, fmt.Errorf("releasing lease: %w", err)
	}
	return commandTag.RowsAffected() > 0, nil
}

// DeleteLease drops an agent's lease, whoever holds it.
func (t *Tx) DeleteLease(ctx context.Context, organizationID, userID, agentID string) error {
	query := "DELETE FROM agent_lease WHERE organization_id = $1 AND user_id = $2 AND agent_id = $3"
	if _, err := t.tx.Exec(ctx, query, organizationID, userID, agentID); err != nil {
		return fmt.Errorf("deleting lease: %w", err)
	}
	return nil
}

// GetLease reads an agent's lease, expired or not: nil if it has none.
func (t *Tx) GetLease(ctx context.Context, organizationID, userID, agentID string) (*Lease, error) {
	return getLease(ctx, t.tx, organizationID, userID, agentID)
}

// GetLease reads an agent's lease, expired or not: nil if it has none.
func (s *Store) GetLease(ctx context.Context, organizationID, userID, agentID string) (*Lease, error) {
	return getLease(ctx, s.client, organizationID, userID, agentID)
}

func getLease(ctx context.Context, q querier, organizationID, userID, agentID string) (*Lease, error) {
	query := leaseSelectQuery + "WHERE organization_id = $1 AND user_id = $2 AND agent_id = $3"
	rows, err := q.Query(ctx, query, organizationID, userID, agentID)
	if err != nil {
		return nil, fmt.Errorf("selecting lease: %w", err)
	}
	lease, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[Lease])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("collecting lease: %w", err)
	}
	return lease, nil
}

// ListTreeLeases returns the leases of the agents of the tree rooted at rootAgent, by agent name.
func (s *Store) ListTreeLeases(ctx context.Context, rootAgent string) (map[string]*Lease, error) {
	query := `SELECT agent_lease.organization_id, agent_lease.user_id, agent_lease.agent_id, agent_lease.replica::TEXT,
			agent_lease.address, agent_lease.expire_time, agent_lease.expire_time <= ` + now + ` AS expired
		FROM agent_lease JOIN agent USING (organization_id, user_id, agent_id) WHERE agent.root_agent = $1`
	rows, err := s.client.Query(ctx, query, rootAgent)
	if err != nil {
		return nil, fmt.Errorf("selecting leases: %w", err)
	}
	type agentLease struct {
		OrganizationID string `db:"organization_id"`
		UserID         string `db:"user_id"`
		AgentID        string `db:"agent_id"`
		Lease
	}
	agentLeases, err := pgx.CollectRows(rows, pgx.RowToStructByName[agentLease])
	if err != nil {
		return nil, fmt.Errorf("collecting leases: %w", err)
	}
	agentToLease := make(map[string]*Lease, len(agentLeases))
	for _, agentLease := range agentLeases {
		agentRn := &agentpb.AgentRn{Organization: agentLease.OrganizationID, User: agentLease.UserID, Agent: agentLease.AgentID}
		agentToLease[agentRn.String()] = &agentLease.Lease
	}
	return agentToLease, nil
}

// RenewLeases extends by duration the leases a replica holds on the given agents and returns the names of the agents
// whose lease it renewed: a turn whose agent is missing lost its lease.
func (s *Store) RenewLeases(ctx context.Context, replica string, agents []string, duration time.Duration) ([]string, error) {
	organizationIDs := make([]string, 0, len(agents))
	userIDs := make([]string, 0, len(agents))
	agentIDs := make([]string, 0, len(agents))
	for _, agent := range agents {
		agentRn, err := agentpb.ParseAgentRn(agent)
		if err != nil {
			return nil, fmt.Errorf("parsing agent name: %w", err)
		}
		organizationIDs = append(organizationIDs, agentRn.Organization)
		userIDs = append(userIDs, agentRn.User)
		agentIDs = append(agentIDs, agentRn.Agent)
	}
	query := `UPDATE agent_lease SET expire_time = ` + now + ` + $2::BIGINT * INTERVAL '1 microsecond'
		FROM UNNEST($3::TEXT[], $4::TEXT[], $5::TEXT[]) AS held (organization_id, user_id, agent_id)
		WHERE agent_lease.replica = $1 AND agent_lease.organization_id = held.organization_id
			AND agent_lease.user_id = held.user_id AND agent_lease.agent_id = held.agent_id
		RETURNING agent_lease.organization_id, agent_lease.user_id, agent_lease.agent_id`
	rows, err := s.client.Query(ctx, query, replica, duration.Microseconds(), organizationIDs, userIDs, agentIDs)
	if err != nil {
		return nil, fmt.Errorf("renewing leases: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (string, error) {
		agentRn := &agentpb.AgentRn{}
		if err := row.Scan(&agentRn.Organization, &agentRn.User, &agentRn.Agent); err != nil {
			return "", err
		}
		return agentRn.String(), nil
	})
}

// ReleaseLeases drops every lease a replica holds, so other replicas take its turns over at once.
func (s *Store) ReleaseLeases(ctx context.Context, replica string) error {
	if _, err := s.client.Exec(ctx, "DELETE FROM agent_lease WHERE replica = $1", replica); err != nil {
		return fmt.Errorf("releasing leases: %w", err)
	}
	return nil
}

// ListClaimableAgents lists, oldest update first and locked until the transaction ends, up to limit live agents
// needing a replica: RUNNING ones whose lease expired or is missing, and IDLE ones with undelivered messages. Agents
// locked by other transactions are skipped.
func (t *Tx) ListClaimableAgents(ctx context.Context, limit int) ([]*model.Agent, error) {
	query := agentSelectQuery + `WHERE (organization_id, user_id, agent_id) IN (
			SELECT agent.organization_id, agent.user_id, agent.agent_id
			FROM agent LEFT JOIN agent_lease USING (organization_id, user_id, agent_id)
			WHERE agent.delete_time IS NULL AND agent.state = $1
				AND (agent_lease.expire_time IS NULL OR agent_lease.expire_time <= ` + now + `)
			UNION
			SELECT agent_mailbox.organization_id, agent_mailbox.user_id, agent_mailbox.agent_id
			FROM agent_mailbox JOIN agent USING (organization_id, user_id, agent_id)
			WHERE agent_mailbox.sent_count > agent_mailbox.delivered_count AND agent.delete_time IS NULL AND agent.state = $2
		) ORDER BY update_time LIMIT $3 FOR UPDATE SKIP LOCKED`
	return collectAgents(ctx, t.tx, query, agentStateRunning, agentStateIdle, limit)
}
