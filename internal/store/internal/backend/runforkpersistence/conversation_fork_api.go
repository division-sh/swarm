package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	storeapiidempotency "github.com/division-sh/swarm/internal/store/internal/apiidempotency"
)

func validateAPIConversationFork(req runfork.APIConversationForkCreateRequest) error {
	if req.Idempotency.Method != "conversation.fork" || strings.TrimSpace(req.Idempotency.ActorTokenID) == "" ||
		strings.TrimSpace(req.Idempotency.ActorTokenID) != strings.TrimSpace(req.Creation.CreatedBy) {
		return fmt.Errorf("conversation.fork requires its exact method and creator request identity")
	}
	return nil
}

func conversationForkCompletion(fork runfork.OperatorConversationForkSession) (apiidempotency.Completion, error) {
	response, err := json.Marshal(runfork.ConversationForkCreateResult{Fork: fork})
	return apiidempotency.Completion{ResourceID: fork.ForkID, Response: response}, err
}

func replayConversationFork(completion apiidempotency.Completion) (runfork.ConversationForkCreateResult, error) {
	var result runfork.ConversationForkCreateResult
	if err := json.Unmarshal(completion.Response, &result); err != nil {
		return result, fmt.Errorf("decode conversation.fork idempotency response: %w", err)
	}
	result.IdempotencyReplayed = true
	return result, nil
}

// CreateAPIConversationFork holds the existing request lease before BEGIN and
// commits the fork and its response association together. No API callback runs
// under this transaction, and loss of the response is resolved by lease replay.
func (s *RunForkPostgresOwner) CreateAPIConversationFork(ctx context.Context, req runfork.APIConversationForkCreateRequest) (result runfork.ConversationForkCreateResult, err error) {
	if err := validateAPIConversationFork(req); err != nil {
		return result, err
	}
	owner, err := postgresConversationForkStore(s)
	if err != nil {
		return result, err
	}
	if strings.TrimSpace(req.Idempotency.IdempotencyKey) == "" {
		result.Fork, err = owner.createOperatorConversationFork(ctx, req.Creation)
		return result, err
	}
	lease, err := storeapiidempotency.AcquirePostgresRequest(ctx, s.apiIdempotency, req.Idempotency)
	if err != nil {
		return result, err
	}
	defer func() {
		err = errors.Join(err, lease.Release(ctx))
	}()
	if completion, replay := lease.Replay(); replay {
		return replayConversationFork(completion)
	}
	result.Fork, err = owner.createConversationForkWithCompletion(ctx, req.Creation, func(txctx context.Context, tx *sql.Tx, fork runfork.OperatorConversationForkSession) error {
		completion, err := conversationForkCompletion(fork)
		if err != nil {
			return err
		}
		return storeapiidempotency.StorePostgresCompletionTx(txctx, lease, tx, completion)
	})
	return result, err
}

func (s *RunForkSQLiteOwner) CreateAPIConversationFork(ctx context.Context, req runfork.APIConversationForkCreateRequest) (result runfork.ConversationForkCreateResult, err error) {
	if err := validateAPIConversationFork(req); err != nil {
		return result, err
	}
	owner, err := sqliteConversationForkStore(s)
	if err != nil {
		return result, err
	}
	if strings.TrimSpace(req.Idempotency.IdempotencyKey) == "" {
		result.Fork, err = owner.createOperatorConversationFork(ctx, req.Creation)
		return result, err
	}
	lease, err := storeapiidempotency.AcquireSQLiteRequest(ctx, s.apiIdempotency, req.Idempotency)
	if err != nil {
		return result, err
	}
	defer lease.Release()
	if completion, replay := lease.Replay(); replay {
		return replayConversationFork(completion)
	}
	result.Fork, err = owner.createConversationForkWithCompletion(ctx, req.Creation, func(txctx context.Context, tx *sql.Tx, fork runfork.OperatorConversationForkSession) error {
		completion, err := conversationForkCompletion(fork)
		if err != nil {
			return err
		}
		return storeapiidempotency.StoreSQLiteCompletionTx(txctx, lease, tx, completion)
	})
	return result, err
}
