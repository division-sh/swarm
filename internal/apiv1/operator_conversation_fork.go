package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store"
)

const conversationForkIdempotencyTTL = 24 * time.Hour
const conversationForkChatHeartbeatInterval = 30 * time.Second

type ConversationForkReadStore interface {
	ListOperatorConversationForks(context.Context, store.ConversationForkListOptions) (store.ConversationForkListResult, error)
	LoadOperatorConversationFork(context.Context, string) (store.OperatorConversationForkSession, error)
}

type ConversationForkLifecycleStore interface {
	CreateOperatorConversationFork(context.Context, store.ConversationForkCreateRequest) (store.OperatorConversationForkSession, error)
	PrepareOperatorConversationForkChat(context.Context, store.ConversationForkChatPrepareRequest) (store.ConversationForkChatPrepared, error)
	HeartbeatOperatorConversationForkChat(context.Context, store.ConversationForkChatPrepared, time.Time) error
	RecordOperatorConversationForkChat(context.Context, store.ConversationForkChatRecordRequest) (store.ConversationForkChatResult, error)
	FailOperatorConversationForkChat(context.Context, store.ConversationForkChatFailureRequest) error
	DeleteOperatorConversationFork(context.Context, string, time.Time) (store.ConversationForkDeleteResult, error)
}

type ForkChatExecutor interface {
	ExecuteForkChat(context.Context, store.ConversationForkChatPrepared, string) (store.ConversationForkChatExecution, error)
}

type conversationForkCreateResult struct {
	Fork                store.OperatorConversationForkSession `json:"fork"`
	IdempotencyReplayed bool                                  `json:"idempotency_replayed"`
}

type conversationForkDeleteResult struct {
	OK                  bool   `json:"ok"`
	ForkID              string `json:"fork_id"`
	Deleted             bool   `json:"deleted"`
	AlreadyDeleted      bool   `json:"already_deleted"`
	IdempotencyReplayed bool   `json:"idempotency_replayed"`
}

type conversationForkErrorDetails struct {
	SessionID string
	ForkID    string
	TurnID    string
	EventID   string
}

func OperatorConversationForkHandlers(opts OperatorReadOptions) map[string]MethodHandler {
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	handlers := map[string]MethodHandler{}
	if opts.ConversationForkLifecycle != nil && opts.Idempotency != nil {
		handlers["conversation.fork"] = func(ctx context.Context, req Request) (any, error) {
			return executeConversationForkCreate(ctx, req, opts, now().UTC())
		}
		handlers["conversation.fork_chat"] = func(ctx context.Context, req Request) (any, error) {
			return executeConversationForkChat(ctx, req, opts, now().UTC())
		}
		handlers["conversation.fork_delete"] = func(ctx context.Context, req Request) (any, error) {
			return executeConversationForkDelete(ctx, req, opts, now().UTC())
		}
	}
	if opts.ConversationForks != nil {
		handlers["conversation.fork_list"] = func(ctx context.Context, req Request) (any, error) {
			listOpts, err := conversationForkListOptionsFromParams(req.Params, now().UTC())
			if err != nil {
				return nil, err
			}
			result, err := opts.ConversationForks.ListOperatorConversationForks(ctx, listOpts)
			if err != nil {
				return nil, conversationForkError(err, conversationForkErrorDetails{})
			}
			if result.Forks == nil {
				result.Forks = []store.OperatorConversationForkSession{}
			}
			return result, nil
		}
		handlers["conversation.fork_view"] = func(ctx context.Context, req Request) (any, error) {
			forkID, err := requiredStringParam(req.Params, "fork_id")
			if err != nil {
				return nil, err
			}
			result, err := opts.ConversationForks.LoadOperatorConversationFork(ctx, forkID)
			if err != nil {
				return nil, conversationForkError(err, conversationForkErrorDetails{ForkID: forkID})
			}
			return result, nil
		}
	}
	if len(handlers) == 0 {
		return nil
	}
	return handlers
}

func executeConversationForkCreate(ctx context.Context, req Request, opts OperatorReadOptions, now time.Time) (any, error) {
	sourceSessionID, err := requiredStringParam(req.Params, "source_session_id")
	if err != nil {
		return nil, err
	}
	forkPoint, err := conversationForkPointSelectorFromParams(req.Params)
	if err != nil {
		return nil, err
	}
	idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
	if err != nil {
		return nil, err
	}
	completion, replay, err := opts.Idempotency.WithAPIIdempotency(ctx, store.APIIdempotencyRequest{
		Method:         req.Method,
		ActorTokenID:   req.ActorTokenID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    req.RequestHash,
		ResourceID:     sourceSessionID,
		TTL:            conversationForkIdempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (store.APIIdempotencyCompletion, error) {
		fork, err := opts.ConversationForkLifecycle.CreateOperatorConversationFork(ctx, store.ConversationForkCreateRequest{
			SourceSessionID: sourceSessionID,
			ForkPoint:       forkPoint,
			CreatedBy:       req.ActorTokenID,
			Now:             now,
		})
		if err != nil {
			return store.APIIdempotencyCompletion{}, conversationForkError(err, conversationForkErrorDetails{
				SessionID: sourceSessionID,
				TurnID:    forkPoint.TurnID,
				EventID:   forkPoint.EventID,
			})
		}
		response, err := json.Marshal(conversationForkCreateResult{
			Fork:                fork,
			IdempotencyReplayed: false,
		})
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		return store.APIIdempotencyCompletion{ResourceID: fork.ForkID, Response: response}, nil
	})
	if err != nil {
		return nil, conversationForkError(err, conversationForkErrorDetails{
			SessionID: sourceSessionID,
			TurnID:    forkPoint.TurnID,
			EventID:   forkPoint.EventID,
		})
	}
	var result conversationForkCreateResult
	if err := json.Unmarshal(completion.Response, &result); err != nil {
		if replay {
			return nil, fmt.Errorf("decode conversation.fork idempotency response: %w", err)
		}
		return nil, fmt.Errorf("decode conversation.fork response: %w", err)
	}
	result.IdempotencyReplayed = replay
	return result, nil
}

func executeConversationForkChat(ctx context.Context, req Request, opts OperatorReadOptions, now time.Time) (any, error) {
	forkID, err := requiredStringParam(req.Params, "fork_id")
	if err != nil {
		return nil, err
	}
	message, err := requiredStringParam(req.Params, "message")
	if err != nil {
		return nil, err
	}
	idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
	if err != nil {
		return nil, err
	}
	completion, replay, err := opts.Idempotency.WithAPIIdempotency(ctx, store.APIIdempotencyRequest{
		Method:         req.Method,
		ActorTokenID:   req.ActorTokenID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    req.RequestHash,
		ResourceID:     forkID,
		TTL:            conversationForkIdempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (store.APIIdempotencyCompletion, error) {
		if opts.ForkChatExecutor == nil {
			return store.APIIdempotencyCompletion{}, fmt.Errorf("conversation fork chat executor is required")
		}
		prepared, err := opts.ConversationForkLifecycle.PrepareOperatorConversationForkChat(ctx, store.ConversationForkChatPrepareRequest{
			ForkID: forkID, Message: message, Method: req.Method, ActorTokenID: req.ActorTokenID,
			RequestHash: req.RequestHash, IdempotencyKey: idempotencyKey, Now: now,
		})
		if err != nil {
			return store.APIIdempotencyCompletion{}, conversationForkError(err, conversationForkErrorDetails{ForkID: forkID})
		}
		execution, err := executeConversationForkChatWithHeartbeat(ctx, opts.ConversationForkLifecycle, opts.ForkChatExecutor, prepared, message)
		if err != nil {
			failure := runtimefailures.FromError(err, "conversation-fork-chat", "execute")
			failErr := opts.ConversationForkLifecycle.FailOperatorConversationForkChat(context.WithoutCancel(ctx), store.ConversationForkChatFailureRequest{
				Prepared: prepared, Cause: err, OutcomeUncertain: failure.Failure.Class == runtimefailures.ClassOutcomeUncertain, Now: now,
			})
			if failErr != nil {
				return store.APIIdempotencyCompletion{}, errors.Join(err, failErr)
			}
			return store.APIIdempotencyCompletion{}, err
		}
		result, err := opts.ConversationForkLifecycle.RecordOperatorConversationForkChat(ctx, store.ConversationForkChatRecordRequest{
			ForkID:       forkID,
			Message:      message,
			ActorTokenID: req.ActorTokenID,
			Prepared:     prepared,
			Execution:    execution,
			Now:          now,
		})
		if err != nil {
			return store.APIIdempotencyCompletion{}, conversationForkError(err, conversationForkErrorDetails{ForkID: forkID})
		}
		result.IdempotencyReplayed = false
		response, err := json.Marshal(result)
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		return store.APIIdempotencyCompletion{ResourceID: result.ForkID, Response: response}, nil
	})
	if err != nil {
		return nil, conversationForkError(err, conversationForkErrorDetails{ForkID: forkID})
	}
	var result store.ConversationForkChatResult
	if err := json.Unmarshal(completion.Response, &result); err != nil {
		if replay {
			return nil, fmt.Errorf("decode conversation.fork_chat idempotency response: %w", err)
		}
		return nil, fmt.Errorf("decode conversation.fork_chat response: %w", err)
	}
	result.IdempotencyReplayed = replay
	return result, nil
}

func executeConversationForkChatWithHeartbeat(
	ctx context.Context,
	lifecycle ConversationForkLifecycleStore,
	executor ForkChatExecutor,
	prepared store.ConversationForkChatPrepared,
	message string,
) (store.ConversationForkChatExecution, error) {
	if err := lifecycle.HeartbeatOperatorConversationForkChat(ctx, prepared, time.Now().UTC()); err != nil {
		return store.ConversationForkChatExecution{}, err
	}
	executionCtx, cancel := context.WithCancel(ctx)
	stop := make(chan struct{})
	done := make(chan struct{})
	heartbeatErr := make(chan error, 1)
	go func() {
		defer close(done)
		ticker := time.NewTicker(conversationForkChatHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-executionCtx.Done():
				return
			case <-ticker.C:
				if err := lifecycle.HeartbeatOperatorConversationForkChat(context.WithoutCancel(executionCtx), prepared, time.Now().UTC()); err != nil {
					heartbeatErr <- err
					cancel()
					return
				}
			}
		}
	}()
	execution, executionErr := executor.ExecuteForkChat(executionCtx, prepared, message)
	close(stop)
	cancel()
	<-done
	select {
	case err := <-heartbeatErr:
		heartbeatFailure := runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "conversation_fork_chat_heartbeat_failed", "conversation-fork-chat", "execute", nil, err)
		if executionErr != nil {
			return store.ConversationForkChatExecution{}, errors.Join(executionErr, heartbeatFailure)
		}
		return store.ConversationForkChatExecution{}, heartbeatFailure
	default:
		return execution, executionErr
	}
}

func executeConversationForkDelete(ctx context.Context, req Request, opts OperatorReadOptions, now time.Time) (any, error) {
	forkID, err := requiredStringParam(req.Params, "fork_id")
	if err != nil {
		return nil, err
	}
	idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
	if err != nil {
		return nil, err
	}
	completion, replay, err := opts.Idempotency.WithAPIIdempotency(ctx, store.APIIdempotencyRequest{
		Method:         req.Method,
		ActorTokenID:   req.ActorTokenID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    req.RequestHash,
		ResourceID:     forkID,
		TTL:            conversationForkIdempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (store.APIIdempotencyCompletion, error) {
		deleted, err := opts.ConversationForkLifecycle.DeleteOperatorConversationFork(ctx, forkID, now)
		if err != nil {
			return store.APIIdempotencyCompletion{}, conversationForkError(err, conversationForkErrorDetails{ForkID: forkID})
		}
		response, err := json.Marshal(conversationForkDeleteResult{
			OK:                  true,
			ForkID:              deleted.ForkID,
			Deleted:             deleted.Deleted,
			AlreadyDeleted:      deleted.AlreadyDeleted,
			IdempotencyReplayed: false,
		})
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		return store.APIIdempotencyCompletion{ResourceID: deleted.ForkID, Response: response}, nil
	})
	if err != nil {
		return nil, conversationForkError(err, conversationForkErrorDetails{ForkID: forkID})
	}
	var result conversationForkDeleteResult
	if err := json.Unmarshal(completion.Response, &result); err != nil {
		if replay {
			return nil, fmt.Errorf("decode conversation.fork_delete idempotency response: %w", err)
		}
		return nil, fmt.Errorf("decode conversation.fork_delete response: %w", err)
	}
	result.IdempotencyReplayed = replay
	return result, nil
}

func conversationForkListOptionsFromParams(params map[string]any, now time.Time) (store.ConversationForkListOptions, error) {
	sourceSessionID, _, err := optionalStringParam(params, "source_session_id")
	if err != nil {
		return store.ConversationForkListOptions{}, err
	}
	cursor, _, err := optionalStringParam(params, "cursor")
	if err != nil {
		return store.ConversationForkListOptions{}, err
	}
	limit, err := boundedIntegerParam(params, "limit", 1, 500)
	if err != nil {
		return store.ConversationForkListOptions{}, err
	}
	return store.ConversationForkListOptions{
		SourceSessionID: sourceSessionID,
		Limit:           limit,
		Cursor:          cursor,
		Now:             now,
	}, nil
}

func conversationForkPointSelectorFromParams(params map[string]any) (store.ConversationForkPointSelector, error) {
	raw, ok := params["fork_point"]
	if !ok || isEmptyParam(raw) {
		return store.ConversationForkPointSelector{}, NewInvalidParamsError(map[string]any{"field": "fork_point", "reason": "is required"})
	}
	obj, ok := raw.(map[string]any)
	if !ok {
		return store.ConversationForkPointSelector{}, NewInvalidParamsError(map[string]any{"field": "fork_point", "reason": "must be an object"})
	}
	for key := range obj {
		switch key {
		case "kind", "turn_id", "event_id", "at":
		default:
			return store.ConversationForkPointSelector{}, NewInvalidParamsError(map[string]any{"field": "fork_point." + key, "reason": "unknown field"})
		}
	}
	kind, err := requiredStringParam(obj, "kind")
	if err != nil {
		return store.ConversationForkPointSelector{}, err
	}
	selector := store.ConversationForkPointSelector{Kind: strings.ToLower(strings.TrimSpace(kind))}
	if turnID, present, err := optionalStringParam(obj, "turn_id"); err != nil {
		return store.ConversationForkPointSelector{}, err
	} else if present {
		selector.TurnID = turnID
	}
	if eventID, present, err := optionalStringParam(obj, "event_id"); err != nil {
		return store.ConversationForkPointSelector{}, err
	} else if present {
		selector.EventID = eventID
	}
	if at, err := timestampParam(obj, "at"); err != nil {
		return store.ConversationForkPointSelector{}, err
	} else if at != nil {
		selector.At = at
	}
	switch selector.Kind {
	case "turn", "event", "time":
		return selector, nil
	default:
		return store.ConversationForkPointSelector{}, NewInvalidParamsError(map[string]any{"field": "fork_point.kind", "reason": "must be one of turn, event, time"})
	}
}

func conversationForkError(err error, details conversationForkErrorDetails) error {
	var conflict *store.APIIdempotencyConflictError
	if errors.As(err, &conflict) {
		return NewApplicationError(IdempotencyConflictCode, false, map[string]any{
			"original_request_hash":    conflict.OriginalRequestHash,
			"conflicting_request_hash": conflict.ConflictingRequestHash,
			"original_response_ref": map[string]any{
				"method":      conflict.Method,
				"resource_id": conflict.ResourceID,
			},
		})
	}
	if errors.Is(err, store.ErrSessionNotFound) {
		return NewApplicationError(SessionNotFoundCode, false, map[string]any{"session_id": details.SessionID})
	}
	if errors.Is(err, store.ErrTurnNotFound) {
		errorDetails := map[string]any{"session_id": details.SessionID}
		if strings.TrimSpace(details.TurnID) != "" {
			errorDetails["turn_id"] = details.TurnID
		}
		return NewApplicationError(TurnNotFoundCode, false, errorDetails)
	}
	if errors.Is(err, store.ErrEventNotFound) {
		return NewApplicationError(EventNotFoundCode, false, map[string]any{"event_id": details.EventID})
	}
	if errors.Is(err, store.ErrConversationForkNotFound) {
		return NewApplicationError(ForkNotFoundCode, false, map[string]any{"fork_id": details.ForkID})
	}
	if errors.Is(err, store.ErrInvalidConversationForkCursor) {
		return NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid conversation fork cursor"})
	}
	if paramErr := entityReadParamError(err); paramErr != nil {
		return NewInvalidParamsError(map[string]any{"field": paramErr.Field, "reason": paramErr.Reason})
	}
	return err
}
