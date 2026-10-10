package mailboxpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/mailbox"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	channelrender "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	storeapiidempotency "github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	storechannel "github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
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
	return s.acknowledgeMailboxNotice(ctx, req, nil)
}

func (s *MailboxPostgresOwner) AcknowledgeChannelNotice(ctx context.Context, req apiidempotency.Request, action operatorchannel.InboundAction) (apiidempotency.Completion, bool, error) {
	return s.acknowledgeMailboxNotice(ctx, req, &action)
}

func (s *MailboxPostgresOwner) acknowledgeMailboxNotice(ctx context.Context, req apiidempotency.Request, action *operatorchannel.InboundAction) (completion apiidempotency.Completion, replayed bool, err error) {
	fact, err := noticeRequestSource(ctx, req)
	if err != nil {
		return completion, false, err
	}
	lease, err := storeapiidempotency.AcquirePostgresRequest(ctx, s.idempotency, req)
	if err != nil {
		return completion, false, err
	}
	defer func() { err = errors.Join(err, lease.Release(ctx)) }()
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		changed = false
		if err := storesource.RequirePostgresSourceArtifactTx(txctx, tx, fact.BundleHash()); err != nil {
			return err
		}
		if err := storeoperator.RequirePrincipalTx(txctx, tx, req.Actor.ID, true); err != nil {
			return err
		}
		state := ""
		if action != nil {
			state, err = storechannel.RequireNoticeActionTx(txctx, tx, *action, req.Actor.ID, req.ResourceID, true)
			if err != nil {
				return err
			}
		}
		if completion, replayed = lease.Replay(); replayed {
			if action != nil && state != "settled" {
				return fmt.Errorf("channel notice replay lacks its committed action")
			}
			return nil
		}
		if action != nil && state != "pending" {
			return fmt.Errorf("channel notice action already settled without completion")
		}
		var err error
		completion, changed, err = acknowledgeNoticeTx(txctx, tx, req.ResourceID, true)
		if err != nil {
			return err
		}
		if action != nil {
			if err := storechannel.SettleAppliedActionIntentTx(txctx, tx, *action, channelrender.ActionApplied, true); err != nil {
				return err
			}
			changed = true
		}
		return storeapiidempotency.StorePostgresCompletionTx(txctx, lease, tx, completion)
	})
	err = errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, channelrender.ReconcileOrdinary))
	if err != nil {
		return apiidempotency.Completion{}, false, err
	}
	return completion, replayed, nil
}

func (s *MailboxSQLiteOwner) AcknowledgeMailboxNotice(ctx context.Context, req apiidempotency.Request) (apiidempotency.Completion, bool, error) {
	return s.acknowledgeMailboxNotice(ctx, req, nil)
}

func (s *MailboxSQLiteOwner) AcknowledgeChannelNotice(ctx context.Context, req apiidempotency.Request, action operatorchannel.InboundAction) (apiidempotency.Completion, bool, error) {
	return s.acknowledgeMailboxNotice(ctx, req, &action)
}

func (s *MailboxSQLiteOwner) acknowledgeMailboxNotice(ctx context.Context, req apiidempotency.Request, action *operatorchannel.InboundAction) (apiidempotency.Completion, bool, error) {
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
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, "sqlite notice acknowledgment", func(txctx context.Context, tx *sql.Tx) error {
		changed = false
		if err := storesource.RequireSQLiteSourceArtifactTx(txctx, tx, fact.BundleHash()); err != nil {
			return err
		}
		if err := storeoperator.RequirePrincipalTx(txctx, tx, req.Actor.ID, false); err != nil {
			return err
		}
		state := ""
		if action != nil {
			state, err = storechannel.RequireNoticeActionTx(txctx, tx, *action, req.Actor.ID, req.ResourceID, false)
			if err != nil {
				return err
			}
		}
		if completion, replayed = lease.Replay(); replayed {
			if action != nil && state != "settled" {
				return fmt.Errorf("channel notice replay lacks its committed action")
			}
			return nil
		}
		if action != nil && state != "pending" {
			return fmt.Errorf("channel notice action already settled without completion")
		}
		var err error
		completion, changed, err = acknowledgeNoticeTx(txctx, tx, req.ResourceID, false)
		if err != nil {
			return err
		}
		if action != nil {
			if err := storechannel.SettleAppliedActionIntentTx(txctx, tx, *action, channelrender.ActionApplied, false); err != nil {
				return err
			}
			changed = true
		}
		return storeapiidempotency.StoreSQLiteCompletionTx(txctx, lease, tx, completion)
	})
	err = errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, channelrender.ReconcileOrdinary))
	if err != nil {
		return apiidempotency.Completion{}, false, err
	}
	return completion, replayed, nil
}

func acknowledgeNoticeTx(ctx context.Context, tx *sql.Tx, id string, postgres bool) (apiidempotency.Completion, bool, error) {
	var isCard bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM decision_cards WHERE card_id=$1)`, id).Scan(&isCard); err != nil {
		return apiidempotency.Completion{}, false, err
	}
	if isCard {
		return apiidempotency.Completion{}, false, mailbox.ErrNotNotice
	}
	query := `SELECT item_type,CAST(payload AS TEXT),notified FROM mailbox WHERE item_id=$1`
	if postgres {
		query += ` FOR UPDATE`
	}
	var itemType, payload string
	var notified bool
	if err := tx.QueryRowContext(ctx, query, id).Scan(&itemType, &payload, &notified); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return apiidempotency.Completion{}, false, mailbox.ErrV1NotFound
		}
		return apiidempotency.Completion{}, false, err
	}
	if err := validateGenericMailboxNotice(itemType, []byte(payload)); err != nil {
		return apiidempotency.Completion{}, false, err
	}
	updated, err := tx.ExecContext(ctx, `UPDATE mailbox SET notified=true WHERE item_id=$1`, id)
	if err != nil {
		return apiidempotency.Completion{}, false, err
	}
	count, err := updated.RowsAffected()
	if err != nil {
		return apiidempotency.Completion{}, false, err
	}
	if count != 1 {
		return apiidempotency.Completion{}, false, mailbox.ErrV1NotFound
	}
	raw, err := canonicaljson.Bytes(map[string]any{"ok": true, "mailbox_id": id, "kind": decisioncard.KindNotice})
	return apiidempotency.Completion{ResourceID: id, Response: raw}, !notified, err
}
