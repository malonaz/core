package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	v5 "github.com/jackc/pgx/v5"

	"github.com/malonaz/core/gengo/ai/model"
	"github.com/malonaz/core/go/aip"
)

// IncrementChatMessageCount atomically adds delta to one of the chat's
// per-role message count columns. The increment is applied in SQL rather than
// read-modify-write so concurrent message writes never lose an update; the
// etag is recomputed from the resulting row in the same transaction.
func (s *Store) IncrementChatMessageCount(ctx context.Context, organizationID, userID, chatID, column string, delta int32) (*model.Chat, error) {
	incrementQuery := fmt.Sprintf(
		`UPDATE chat SET %[1]s = %[1]s + $1, update_time = $2
		 WHERE organization_id = $3 AND user_id = $4 AND chat_id = $5 AND delete_time IS NULL
		 RETURNING `+strings.Join(ChatPostgresColumns, ","),
		column,
	)
	var chat *model.Chat
	err := s.client.ExecuteTransaction(ctx, v5.ReadCommitted, func(tx v5.Tx) error {
		rows, err := tx.Query(ctx, incrementQuery, delta, time.Now().UTC(), organizationID, userID, chatID)
		if err != nil {
			return err
		}
		chat, err = v5.CollectOneRow(rows, v5.RowToAddrOfStructByNameLax[model.Chat])
		if err != nil {
			if err == v5.ErrNoRows {
				return model.ErrChatNotExist
			}
			return err
		}
		chatPb, err := chat.ToPb()
		if err != nil {
			return fmt.Errorf("converting chat to pb: %w", err)
		}
		chat.Etag, err = aip.ComputeETag(chatPb)
		if err != nil {
			return fmt.Errorf("computing etag: %w", err)
		}
		_, err = tx.Exec(ctx, `UPDATE chat SET etag = $1 WHERE organization_id = $2 AND user_id = $3 AND chat_id = $4`, chat.Etag, organizationID, userID, chatID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return chat, nil
}
