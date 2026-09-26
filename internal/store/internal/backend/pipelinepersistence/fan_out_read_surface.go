package pipelinepersistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

type fanOutListCursor struct {
	RunID      string                     `json:"run_id"`
	FilterHash string                     `json:"filter_hash"`
	Order      string                     `json:"order"`
	After      fanoutobligation.IntentKey `json:"after"`
}

func fanOutListFilterHash(filter fanoutobligation.ListFilter) string {
	raw, _ := json.Marshal(filter)
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}

func decodeFanOutListCursor(q fanoutobligation.ListQuery) (*fanOutListCursor, error) {
	if q.Cursor == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(q.Cursor)
	if err != nil {
		return nil, fanoutobligation.ErrInvalidListCursor
	}
	var cursor fanOutListCursor
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil {
		return nil, fanoutobligation.ErrInvalidListCursor
	}
	if decoder.Decode(new(any)) != io.EOF || cursor.RunID != q.RunID || cursor.FilterHash != fanOutListFilterHash(q.Filter) || cursor.Order != fanoutobligation.ListIdentityOrder || cursor.After.RunID != q.RunID || cursor.After.Validate() != nil {
		return nil, fanoutobligation.ErrInvalidListCursor
	}
	canonical, err := json.Marshal(cursor)
	if err != nil || !bytes.Equal(raw, canonical) {
		return nil, fanoutobligation.ErrInvalidListCursor
	}
	if (q.Filter.TriggeringDeliveryID != "" && q.Filter.TriggeringDeliveryID != cursor.After.TriggeringDeliveryID) ||
		(q.Filter.FlowPath != "" && q.Filter.FlowPath != cursor.After.ElementRef.FlowPath) ||
		(q.Filter.SemanticPath != "" && q.Filter.SemanticPath != cursor.After.ElementRef.SemanticPath) {
		return nil, fanoutobligation.ErrInvalidListCursor
	}
	return &cursor, nil
}

func (s *PipelinePostgresOwner) ListFanOutIntents(ctx context.Context, q fanoutobligation.ListQuery) (fanoutobligation.ListPage, error) {
	return listFanOutIntents(ctx, s.backend.ConstructionHandle(), true, time.Now, q)
}

func (s *PipelineSQLiteOwner) ListFanOutIntents(ctx context.Context, q fanoutobligation.ListQuery) (fanoutobligation.ListPage, error) {
	return listFanOutIntents(ctx, s.backend.ConstructionHandle(), false, s.now, q)
}

// One read transaction observes the run and a bounded header page. Immutable
// source values, outcomes and delivery history are deliberately not hydrated.
func listFanOutIntents(ctx context.Context, db *sql.DB, postgres bool, now func() time.Time, q fanoutobligation.ListQuery) (fanoutobligation.ListPage, error) {
	var page fanoutobligation.ListPage
	if err := q.Validate(); err != nil {
		return page, err
	}
	cursor, err := decodeFanOutListCursor(q)
	if err != nil {
		return page, err
	}
	if q.Limit == 0 {
		q.Limit = fanoutobligation.DefaultListLimit
	}
	opts := &sql.TxOptions{ReadOnly: true}
	if postgres {
		opts.Isolation = sql.LevelRepeatableRead
	}
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return page, err
	}
	defer tx.Rollback()
	page.RunID, page.Order = q.RunID, fanoutobligation.ListIdentityOrder
	if err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id=$1`, q.RunID).Scan(&page.RunStatus); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return page, operatorread.ErrRunNotFound
		}
		return page, err
	}
	page.ObservedAt, err = fanOutAdmissionTime(ctx, tx, postgres, now)
	if err != nil {
		return page, err
	}
	args := []any{q.RunID}
	query := `SELECT ` + fanOutIntentColumns + ` FROM fan_out_intents WHERE run_id=$1`
	for _, filter := range []struct{ column, value string }{
		{"status", string(q.Filter.Status)}, {"triggering_delivery_id", q.Filter.TriggeringDeliveryID},
		{"flow_path", q.Filter.FlowPath}, {"semantic_path", q.Filter.SemanticPath},
	} {
		if filter.value != "" {
			args = append(args, filter.value)
			query += fmt.Sprintf(" AND %s=$%d", filter.column, len(args))
		}
	}
	// Explicit byte collation makes structured identity order backend-independent.
	collation := ` COLLATE BINARY`
	if postgres {
		collation = ` COLLATE "C"`
	}
	identity := `COALESCE(CAST(triggering_delivery_id AS TEXT),'')` + collation + `,COALESCE(flow_path,'')` + collation + `,COALESCE(declaration_family,'')` + collation + `,COALESCE(semantic_path,'')` + collation + `,COALESCE(CAST(deployment_feed_id AS TEXT),'')` + collation
	if cursor != nil {
		key := cursor.After
		start := len(args) + 1
		args = append(args, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath, key.DeploymentFeedID)
		query += fmt.Sprintf(" AND (%s) > ($%d,$%d,$%d,$%d,$%d)", identity, start, start+1, start+2, start+3, start+4)
	}
	args = append(args, q.Limit+1)
	query += fmt.Sprintf(" ORDER BY %s LIMIT $%d", identity, len(args))
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	page.Intents = make([]fanoutobligation.IntentReadback, 0, q.Limit)
	for rows.Next() {
		intent, err := scanFanOutIntent(rows)
		if err != nil {
			return page, err
		}
		row, err := intent.ReadbackAt(page.ObservedAt)
		if err != nil {
			return page, err
		}
		if len(page.Intents) == q.Limit {
			token := fanOutListCursor{RunID: q.RunID, FilterHash: fanOutListFilterHash(q.Filter), Order: page.Order, After: page.Intents[len(page.Intents)-1].Key}
			raw, err := json.Marshal(token)
			if err != nil {
				return page, err
			}
			page.NextCursor = base64.RawURLEncoding.EncodeToString(raw)
			if len(page.NextCursor) > 16384 {
				return page, errors.New("fan-out identity exceeds bounded cursor size")
			}
			break
		}
		page.Intents = append(page.Intents, row)
	}
	if err := rows.Err(); err != nil {
		return page, err
	}
	if err := rows.Close(); err != nil {
		return page, err
	}
	if err := tx.Commit(); err != nil {
		return page, err
	}
	return page, nil
}
