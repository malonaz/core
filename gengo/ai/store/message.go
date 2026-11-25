package store

import (
	"context"

	v5 "github.com/jackc/pgx/v5"

	"github.com/malonaz/core/gengo/ai/model"
	"github.com/malonaz/core/go/postgres"
)

// toolCallSettlementColumns are the columns settling a tool call writes.
var toolCallSettlementColumns = []string{"result", "update_time", "etag"}

// pendingToolCallsQuery selects a chat's live pending tool calls by the ids their results name.
var pendingToolCallsQuery = "SELECT " + postgres.QualifyColumns(ToolCallPostgresColumns, "tool_call") + ` FROM tool_call
	WHERE organization_id = $1 AND user_id = $2 AND chat_id = $3 AND tool_call_id = ANY($4) AND result IS NULL AND delete_time IS NULL`

// settleToolCallQuery writes the result of a pending tool call.
var settleToolCallQuery = `UPDATE tool_call SET result = $1, update_time = $2, etag = $3
	WHERE organization_id = $4 AND user_id = $5 AND chat_id = $6 AND message_id = $7 AND position = $8 AND result IS NULL`

// InsertMessage inserts a message with the tool calls it makes, and settles the pending tool calls of its chat that
// the results it carries answer, in one transaction: a message never exists without its calls, nor a result beside
// its call still pending. settle returns the calls settled among the pending ones named by callIDs. A replay of the
// message's request returns the message its first insert committed.
func (s *Store) InsertMessage(
	ctx context.Context, requestID string, message *model.Message,
	toolCallRequestIDs []string, toolCalls []*model.ToolCall,
	callIDs []string, settle func(pending []*model.ToolCall) ([]*model.ToolCall, error),
) (*model.Message, error) {
	var inserted *model.Message
	transactionFN := func(tx postgres.Tx) error {
		query, params := postgres.BatchInsertQuery(messageInsertPostgresQuery, []*MessageWithRequestID{{RequestID: requestID, Message: *message}}, MessageWithRequestIDPostgresColumns...)
		rows, err := tx.Query(ctx, query+messageInsertReturningClause, params...)
		if err != nil {
			return err
		}
		upserted, err := v5.CollectRows(rows, v5.RowToAddrOfStructByNameLax[MessageWithRequestID])
		if err != nil {
			return err
		}
		messages, err := orderMessagesByRequestID([]string{requestID}, upserted)
		if err != nil {
			return err
		}
		inserted = messages[0]

		if len(toolCalls) > 0 {
			withRequestIDs := make([]*ToolCallWithRequestID, len(toolCalls))
			for i, toolCall := range toolCalls {
				withRequestIDs[i] = &ToolCallWithRequestID{RequestID: toolCallRequestIDs[i], ToolCall: *toolCall}
			}
			query, params := postgres.BatchInsertQuery(toolCallInsertPostgresQuery, withRequestIDs, ToolCallWithRequestIDPostgresColumns...)
			if _, err := tx.Exec(ctx, query, params...); err != nil {
				return err
			}
		}

		if len(callIDs) == 0 {
			return nil
		}
		rows, err = tx.Query(ctx, pendingToolCallsQuery, message.OrganizationID, message.UserID, message.ChatID, callIDs)
		if err != nil {
			return err
		}
		pending, err := v5.CollectRows(rows, v5.RowToAddrOfStructByNameLax[model.ToolCall])
		if err != nil {
			return err
		}
		settled, err := settle(pending)
		if err != nil {
			return err
		}
		for _, toolCall := range settled {
			params := append(postgres.GetParams(toolCall, toolCallSettlementColumns...),
				toolCall.OrganizationID, toolCall.UserID, toolCall.ChatID, toolCall.MessageID, toolCall.ToolCallID)
			if _, err := tx.Exec(ctx, settleToolCallQuery, params...); err != nil {
				return err
			}
		}
		return nil
	}

	if err := s.client.ExecuteTransaction(ctx, postgres.ReadCommitted, transactionFN); err != nil {
		// A replay with a server-generated id collides on request_id: the first insert committed everything.
		if postgres.IsUniqueViolation(err) {
			rows, lookupErr := s.client.Query(ctx, messageGetByRequestIDsQuery, []string{requestID})
			if lookupErr != nil {
				return nil, lookupErr
			}
			existing, lookupErr := v5.CollectRows(rows, v5.RowToAddrOfStructByNameLax[MessageWithRequestID])
			if lookupErr != nil {
				return nil, lookupErr
			}
			if len(existing) == 1 {
				return &existing[0].Message, nil
			}
			return nil, model.ErrMessageAlreadyExists
		}
		return nil, err
	}
	return inserted, nil
}
