package decisionstore

import (
	"context"
	"database/sql"
)

type ChannelInputCommitStorage struct {
	Index, Decisions                                 int
	DraftStatus, CardStatus, DraftFields, CardFields string
}

func ReadChannelInputCommitStorageTx(ctx context.Context, tx *sql.Tx, card string) (ChannelInputCommitStorage, error) {
	var result ChannelInputCommitStorage
	err := tx.QueryRowContext(ctx, `SELECT d.next_field_index,d.status,c.status,CAST(d.input_fields AS TEXT),CAST(c.fields AS TEXT),
		(SELECT COUNT(*) FROM events e WHERE e.run_id=c.run_id AND e.event_name='mailbox.card_decided')
		FROM decision_card_input_drafts d JOIN decision_cards c ON c.card_id=d.card_id WHERE c.card_id=$1`, card).
		Scan(&result.Index, &result.DraftStatus, &result.CardStatus, &result.DraftFields, &result.CardFields, &result.Decisions)
	return result, err
}
