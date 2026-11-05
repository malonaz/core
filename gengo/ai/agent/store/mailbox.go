package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
)

// Mailbox counts the messages sent to an agent against those its turns delivered.
type Mailbox struct {
	// The number of messages sent to the agent, each counted once queued in its chat.
	SentCount int64 `db:"sent_count"`
	// The sent count read before a turn's latest delivery: every message it counts was delivered.
	DeliveredCount int64 `db:"delivered_count"`
}

// CountSentMessage counts a message sent to an agent, once it is queued in the agent's chat.
func (t *Tx) CountSentMessage(ctx context.Context, organizationID, userID, agentID string) error {
	query := `INSERT INTO agent_mailbox (organization_id, user_id, agent_id, sent_count, delivered_count) VALUES ($1, $2, $3, 1, 0)
		ON CONFLICT (organization_id, user_id, agent_id) DO UPDATE SET sent_count = agent_mailbox.sent_count + 1`
	if _, err := t.tx.Exec(ctx, query, organizationID, userID, agentID); err != nil {
		return fmt.Errorf("counting sent message: %w", err)
	}
	return nil
}

// SetDeliveredCount records that a turn delivered every message a sent count counts. It never moves backwards.
func (t *Tx) SetDeliveredCount(ctx context.Context, organizationID, userID, agentID string, deliveredCount int64) error {
	query := `INSERT INTO agent_mailbox (organization_id, user_id, agent_id, sent_count, delivered_count) VALUES ($1, $2, $3, $4, $4)
		ON CONFLICT (organization_id, user_id, agent_id) DO UPDATE SET
			delivered_count = GREATEST(agent_mailbox.delivered_count, EXCLUDED.delivered_count)`
	if _, err := t.tx.Exec(ctx, query, organizationID, userID, agentID, deliveredCount); err != nil {
		return fmt.Errorf("setting delivered count: %w", err)
	}
	return nil
}

// GetMailbox reads an agent's mailbox: zero counts if no message was ever sent to it.
func (t *Tx) GetMailbox(ctx context.Context, organizationID, userID, agentID string) (*Mailbox, error) {
	query := "SELECT sent_count, delivered_count FROM agent_mailbox WHERE organization_id = $1 AND user_id = $2 AND agent_id = $3"
	rows, err := t.tx.Query(ctx, query, organizationID, userID, agentID)
	if err != nil {
		return nil, fmt.Errorf("selecting mailbox: %w", err)
	}
	mailbox, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[Mailbox])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return &Mailbox{}, nil
		}
		return nil, fmt.Errorf("collecting mailbox: %w", err)
	}
	return mailbox, nil
}

// GetSentCount reads the number of messages sent to an agent.
func (s *Store) GetSentCount(ctx context.Context, organizationID, userID, agentID string) (int64, error) {
	query := `SELECT COALESCE((SELECT sent_count FROM agent_mailbox
		WHERE organization_id = $1 AND user_id = $2 AND agent_id = $3), 0)`
	var sentCount int64
	if err := s.client.QueryRow(ctx, query, organizationID, userID, agentID).Scan(&sentCount); err != nil {
		return 0, fmt.Errorf("selecting sent count: %w", err)
	}
	return sentCount, nil
}

// ListTreeSentCounts returns the number of messages sent to each agent of the tree rooted at rootAgent, by agent name.
// Agents never sent a message are missing.
func (s *Store) ListTreeSentCounts(ctx context.Context, rootAgent string) (map[string]int64, error) {
	query := `SELECT agent_mailbox.organization_id, agent_mailbox.user_id, agent_mailbox.agent_id, agent_mailbox.sent_count
		FROM agent_mailbox JOIN agent USING (organization_id, user_id, agent_id) WHERE agent.root_agent = $1`
	rows, err := s.client.Query(ctx, query, rootAgent)
	if err != nil {
		return nil, fmt.Errorf("selecting sent counts: %w", err)
	}
	agentToSentCount := map[string]int64{}
	var agentRn agentpb.AgentRn
	var sentCount int64
	if _, err := pgx.ForEachRow(rows, []any{&agentRn.Organization, &agentRn.User, &agentRn.Agent, &sentCount}, func() error {
		agentToSentCount[agentRn.String()] = sentCount
		return nil
	}); err != nil {
		return nil, fmt.Errorf("collecting sent counts: %w", err)
	}
	return agentToSentCount, nil
}
