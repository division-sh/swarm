package mailboxpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/mailbox"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	storeapiidempotency "github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	storeoperator "github.com/division-sh/swarm/internal/store/internal/backend/operatorchannel"
	storesource "github.com/division-sh/swarm/internal/store/internal/sourceartifact"
)

func noticeRequestSource(ctx context.Context, req apiidempotency.Request) (correlation.SourceArtifactFact, error) {
	if err := req.Actor.ValidateMethod(req.Method); err != nil {
		return correlation.SourceArtifactFact{}, err
	}
	if req.Method != "mailbox.acknowledge" || req.ResourceID == "" || req.RequestHash == "" {
		return correlation.SourceArtifactFact{}, fmt.Errorf("exact notice acknowledgment identity is required")
	}
	fact, ok := correlation.SourceArtifactFactFromContext(ctx)
	if !ok || fact.Validate() != nil {
		return fact, fmt.Errorf("admitted notice source fact is required")
	}
	return fact, nil
}

func (s *MailboxPostgresOwner) AcknowledgeMailboxNotice(ctx context.Context, req apiidempotency.Request) (completion apiidempotency.Completion, replayed bool, err error) {
	fact, err := noticeRequestSource(ctx, req)
	if err != nil {
		return completion, false, err
	}
	lease, err := storeapiidempotency.AcquirePostgresRequest(ctx, s.idempotency, req)
	if err != nil {
		return completion, false, err
	}
	defer func() { err = errors.Join(err, lease.Release(ctx)) }()
	err = s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if err := storesource.RequirePostgresSourceArtifactTx(txctx, tx, fact.BundleHash()); err != nil {
			return err
		}
		if err := storeoperator.RequirePrincipalTx(txctx, tx, req.Actor.ID, true); err != nil {
			return err
		}
		if completion, replayed = lease.Replay(); replayed {
			return nil
		}
		var err error
		completion, err = acknowledgeNoticeTx(txctx, tx, req.ResourceID, true)
		if err != nil {
			return err
		}
		return storeapiidempotency.StorePostgresCompletionTx(txctx, lease, tx, completion)
	})
	if err != nil {
		return apiidempotency.Completion{}, false, err
	}
	return completion, replayed, nil
}

func (s *MailboxSQLiteOwner) AcknowledgeMailboxNotice(ctx context.Context, req apiidempotency.Request) (apiidempotency.Completion, bool, error) {
	fact, err := noticeRequestSource(ctx, req)
	if err != nil {
		return apiidempotency.Completion{}, false, err
	}
	lease, err := storeapiidempotency.AcquireSQLiteRequest(ctx, s.idempotency, req)
	if err != nil {
		return apiidempotency.Completion{}, false, err
	}
	defer lease.Release()
	var completion apiidempotency.Completion
	var replayed bool
	err = s.backend.RunTransaction(ctx, "sqlite notice acknowledgment", func(txctx context.Context, tx *sql.Tx) error {
		if err := storesource.RequireSQLiteSourceArtifactTx(txctx, tx, fact.BundleHash()); err != nil {
			return err
		}
		if err := storeoperator.RequirePrincipalTx(txctx, tx, req.Actor.ID, false); err != nil {
			return err
		}
		if completion, replayed = lease.Replay(); replayed {
			return nil
		}
		var err error
		completion, err = acknowledgeNoticeTx(txctx, tx, req.ResourceID, false)
		if err != nil {
			return err
		}
		return storeapiidempotency.StoreSQLiteCompletionTx(txctx, lease, tx, completion)
	})
	if err != nil {
		return apiidempotency.Completion{}, false, err
	}
	return completion, replayed, nil
}

func acknowledgeNoticeTx(ctx context.Context, tx *sql.Tx, id string, postgres bool) (apiidempotency.Completion, error) {
	var isCard bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM decision_cards WHERE card_id=$1)`, id).Scan(&isCard); err != nil {
		return apiidempotency.Completion{}, err
	}
	if isCard {
		return apiidempotency.Completion{}, mailbox.ErrNotNotice
	}
	query := `SELECT item_type,CAST(payload AS TEXT) FROM mailbox WHERE item_id=$1`
	if postgres {
		query += ` FOR UPDATE`
	}
	var itemType, payload string
	if err := tx.QueryRowContext(ctx, query, id).Scan(&itemType, &payload); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return apiidempotency.Completion{}, mailbox.ErrV1NotFound
		}
		return apiidempotency.Completion{}, err
	}
	if err := validateGenericMailboxNotice(itemType, []byte(payload)); err != nil {
		return apiidempotency.Completion{}, err
	}
	updated, err := tx.ExecContext(ctx, `UPDATE mailbox SET notified=true WHERE item_id=$1`, id)
	if err != nil {
		return apiidempotency.Completion{}, err
	}
	count, err := updated.RowsAffected()
	if err != nil {
		return apiidempotency.Completion{}, err
	}
	if count != 1 {
		return apiidempotency.Completion{}, mailbox.ErrV1NotFound
	}
	raw, err := canonicaljson.Bytes(map[string]any{"ok": true, "mailbox_id": id, "kind": decisioncard.KindNotice})
	return apiidempotency.Completion{ResourceID: id, Response: raw}, err
}
