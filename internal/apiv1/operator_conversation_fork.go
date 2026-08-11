package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	operatorread "github.com/division-sh/swarm/internal/operatorread"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

const conversationForkIdempotencyTTL = 24 * time.Hour
const conversationForkChatHeartbeatInterval = 30 * time.Second

type ConversationForkReadStore interface {
	ListOperatorConversationForks(context.Context, runfork.ConversationForkListOptions) (runfork.ConversationForkListResult, error)
	LoadOperatorConversationFork(context.Context, string) (runfork.OperatorConversationForkSession, error)
}

type ConversationForkLifecycleStore interface {
	CreateOperatorConversationFork(context.Context, runfork.ConversationForkCreateRequest) (runfork.OperatorConversationForkSession, error)
	AdmitOperatorConversationForkChat(context.Context, string, executionposture.Posture) error
	PrepareOperatorConversationForkChat(context.Context, runfork.ConversationForkChatPrepareRequest) (runfork.ConversationForkChatPrepared, error)
	HeartbeatOperatorConversationForkChat(context.Context, runfork.ConversationForkChatPrepared, time.Time) error
	RecordOperatorConversationForkChat(context.Context, runfork.ConversationForkChatRecordRequest) (runfork.ConversationForkChatResult, error)
	FailOperatorConversationForkChat(context.Context, runfork.ConversationForkChatFailureRequest) error
	DeleteOperatorConversationFork(context.Context, string, time.Time) (runfork.ConversationForkDeleteResult, error)
}

type ForkChatExecutor interface {
	ExecuteForkChat(context.Context, runfork.ConversationForkChatPrepared, string) (runfork.ConversationForkChatExecution, error)
}

type conversationForkCreateResult struct {
	Fork                runfork.OperatorConversationForkSession `json:"fork"`
	IdempotencyReplayed bool                                    `json:"idempotency_replayed"`
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

func OperatorConversationForkHandlers(opts ConversationForkHandlerOptions) map[string]MethodHandler {
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	handlers := map[string]MethodHandler{}
	if opts.Lifecycle != nil && opts.Idempotency != nil {
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
	if opts.Reads != nil {
		handlers["conversation.fork_list"] = func(ctx context.Context, req Request) (any, error) {
			listOpts, err := conversationForkListOptionsFromParams(req.Params, now().UTC())
			if err != nil {
				return nil, err
			}
			result, err := opts.Reads.ListOperatorConversationForks(ctx, listOpts)
			if err != nil {
				return nil, conversationForkError(err, conversationForkErrorDetails{})
			}
			if result.Forks == nil {
				result.Forks = []runfork.OperatorConversationForkSession{}
			}
			return result, nil
		}
		handlers["conversation.fork_view"] = func(ctx context.Context, req Request) (any, error) {
			forkID, err := requiredStringParam(req.Params, "fork_id")
			if err != nil {
				return nil, err
			}
			result, err := opts.Reads.LoadOperatorConversationFork(ctx, forkID)
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

func executeConversationForkCreate(ctx context.Context, req Request, opts ConversationForkHandlerOptions, now time.Time) (any, error) {
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
	completion, replay, err := opts.Idempotency.WithAPIIdempotency(ctx, apiidempotency.Request{
		Method:         req.Method,
		ActorTokenID:   req.ActorTokenID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    req.RequestHash,
		ResourceID:     sourceSessionID,
		TTL:            conversationForkIdempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (apiidempotency.Completion, error) {
		fork, err := opts.Lifecycle.CreateOperatorConversationFork(ctx, runfork.ConversationForkCreateRequest{
			SourceSessionID: sourceSessionID,
			ForkPoint:       forkPoint,
			CreatedBy:       req.ActorTokenID,
			Now:             now,
		})
		if err != nil {
			return apiidempotency.Completion{}, conversationForkError(err, conversationForkErrorDetails{
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
			return apiidempotency.Completion{}, err
		}
		return apiidempotency.Completion{ResourceID: fork.ForkID, Response: response}, nil
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

func executeConversationForkChat(ctx context.Context, req Request, opts ConversationForkHandlerOptions, now time.Time) (any, error) {
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
	if err := opts.Lifecycle.AdmitOperatorConversationForkChat(ctx, forkID, opts.ExecutionPosture); err != nil {
		return nil, conversationForkError(err, conversationForkErrorDetails{ForkID: forkID})
	}
	completion, replay, err := opts.Idempotency.WithAPIIdempotency(ctx, apiidempotency.Request{
		Method:         req.Method,
		ActorTokenID:   req.ActorTokenID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    req.RequestHash,
		ResourceID:     forkID,
		TTL:            conversationForkIdempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (apiidempotency.Completion, error) {
		if opts.Chat == nil {
			return apiidempotency.Completion{}, fmt.Errorf("conversation fork chat executor is required")
		}
		prepared, err := opts.Lifecycle.PrepareOperatorConversationForkChat(ctx, runfork.ConversationForkChatPrepareRequest{
			ForkID: forkID, Message: message, Method: req.Method, ActorTokenID: req.ActorTokenID,
			RequestHash: req.RequestHash, IdempotencyKey: idempotencyKey, Now: now, ExecutionPosture: opts.ExecutionPosture,
		})
		if err != nil {
			return apiidempotency.Completion{}, conversationForkError(err, conversationForkErrorDetails{ForkID: forkID})
		}
		execution, err := executeConversationForkChatWithHeartbeat(ctx, opts.Lifecycle, opts.Chat, prepared, message)
		if err != nil {
			failure := runtimefailures.FromError(err, "conversation-fork-chat", "execute")
			failErr := opts.Lifecycle.FailOperatorConversationForkChat(context.WithoutCancel(ctx), runfork.ConversationForkChatFailureRequest{
				Prepared: prepared, Cause: err, OutcomeUncertain: failure.Failure.Class == runtimefailures.ClassOutcomeUncertain, Now: now,
			})
			if failErr != nil {
				return apiidempotency.Completion{}, errors.Join(err, failErr)
			}
			return apiidempotency.Completion{}, err
		}
		result, err := opts.Lifecycle.RecordOperatorConversationForkChat(ctx, runfork.ConversationForkChatRecordRequest{
			ForkID:       forkID,
			Message:      message,
			ActorTokenID: req.ActorTokenID,
			Prepared:     prepared,
			Execution:    execution,
			Now:          now,
		})
		if err != nil {
			return apiidempotency.Completion{}, conversationForkError(err, conversationForkErrorDetails{ForkID: forkID})
		}
		result.IdempotencyReplayed = false
		response, err := json.Marshal(result)
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		return apiidempotency.Completion{ResourceID: result.ForkID, Response: response}, nil
	})
	if err != nil {
		return nil, conversationForkError(err, conversationForkErrorDetails{ForkID: forkID})
	}
	var result runfork.ConversationForkChatResult
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
	prepared runfork.ConversationForkChatPrepared,
	message string,
) (runfork.ConversationForkChatExecution, error) {
	if err := lifecycle.HeartbeatOperatorConversationForkChat(ctx, prepared, time.Now().UTC()); err != nil {
		return runfork.ConversationForkChatExecution{}, err
	}
	executionCtx, cancel := context.WithCancel(ctx)
	stop := make(chan struct{})
	done := make(chan struct{})
	heartbeatErr := make(chan error, 1)
	owner, ok := worklifetime.ProcessFromContext(ctx)
	if !ok {
		cancel()
		return runfork.ConversationForkChatExecution{}, errors.New("process work owner is required for conversation fork heartbeat")
	}
	lease, err := owner.Begin(executionCtx)
	if err != nil {
		cancel()
		return runfork.ConversationForkChatExecution{}, fmt.Errorf("admit conversation fork heartbeat: %w", err)
	}
	go func() {
		defer close(done)
		defer func() { _ = lease.Done() }()
		ticker := time.NewTicker(conversationForkChatHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-lease.Context().Done():
				return
			case <-ticker.C:
				if err := lifecycle.HeartbeatOperatorConversationForkChat(context.WithoutCancel(lease.Context()), prepared, time.Now().UTC()); err != nil {
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
			return runfork.ConversationForkChatExecution{}, errors.Join(executionErr, heartbeatFailure)
		}
		return runfork.ConversationForkChatExecution{}, heartbeatFailure
	default:
		return execution, executionErr
	}
}

func executeConversationForkDelete(ctx context.Context, req Request, opts ConversationForkHandlerOptions, now time.Time) (any, error) {
	forkID, err := requiredStringParam(req.Params, "fork_id")
	if err != nil {
		return nil, err
	}
	idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
	if err != nil {
		return nil, err
	}
	completion, replay, err := opts.Idempotency.WithAPIIdempotency(ctx, apiidempotency.Request{
		Method:         req.Method,
		ActorTokenID:   req.ActorTokenID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    req.RequestHash,
		ResourceID:     forkID,
		TTL:            conversationForkIdempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (apiidempotency.Completion, error) {
		deleted, err := opts.Lifecycle.DeleteOperatorConversationFork(ctx, forkID, now)
		if err != nil {
			return apiidempotency.Completion{}, conversationForkError(err, conversationForkErrorDetails{ForkID: forkID})
		}
		response, err := json.Marshal(conversationForkDeleteResult{
			OK:                  true,
			ForkID:              deleted.ForkID,
			Deleted:             deleted.Deleted,
			AlreadyDeleted:      deleted.AlreadyDeleted,
			IdempotencyReplayed: false,
		})
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		return apiidempotency.Completion{ResourceID: deleted.ForkID, Response: response}, nil
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

func conversationForkListOptionsFromParams(params map[string]any, now time.Time) (runfork.ConversationForkListOptions, error) {
	sourceSessionID, _, err := optionalStringParam(params, "source_session_id")
	if err != nil {
		return runfork.ConversationForkListOptions{}, err
	}
	cursor, _, err := optionalStringParam(params, "cursor")
	if err != nil {
		return runfork.ConversationForkListOptions{}, err
	}
	limit, err := boundedIntegerParam(params, "limit", 1, 500)
	if err != nil {
		return runfork.ConversationForkListOptions{}, err
	}
	return runfork.ConversationForkListOptions{
		SourceSessionID: sourceSessionID,
		Limit:           limit,
		Cursor:          cursor,
		Now:             now,
	}, nil
}

func conversationForkPointSelectorFromParams(params map[string]any) (runfork.ConversationForkPointSelector, error) {
	raw, ok := params["fork_point"]
	if !ok || isEmptyParam(raw) {
		return runfork.ConversationForkPointSelector{}, NewInvalidParamsError(map[string]any{"field": "fork_point", "reason": "is required"})
	}
	obj, ok := raw.(map[string]any)
	if !ok {
		return runfork.ConversationForkPointSelector{}, NewInvalidParamsError(map[string]any{"field": "fork_point", "reason": "must be an object"})
	}
	for key := range obj {
		switch key {
		case "kind", "turn_id", "event_id", "at":
		default:
			return runfork.ConversationForkPointSelector{}, NewInvalidParamsError(map[string]any{"field": "fork_point." + key, "reason": "unknown field"})
		}
	}
	kind, err := requiredStringParam(obj, "kind")
	if err != nil {
		return runfork.ConversationForkPointSelector{}, err
	}
	selector := runfork.ConversationForkPointSelector{Kind: strings.ToLower(strings.TrimSpace(kind))}
	if turnID, present, err := optionalStringParam(obj, "turn_id"); err != nil {
		return runfork.ConversationForkPointSelector{}, err
	} else if present {
		selector.TurnID = turnID
	}
	if eventID, present, err := optionalStringParam(obj, "event_id"); err != nil {
		return runfork.ConversationForkPointSelector{}, err
	} else if present {
		selector.EventID = eventID
	}
	if at, err := timestampParam(obj, "at"); err != nil {
		return runfork.ConversationForkPointSelector{}, err
	} else if at != nil {
		selector.At = at
	}
	switch selector.Kind {
	case "turn", "event", "time":
		return selector, nil
	default:
		return runfork.ConversationForkPointSelector{}, NewInvalidParamsError(map[string]any{"field": "fork_point.kind", "reason": "must be one of turn, event, time"})
	}
}

func conversationForkError(err error, details conversationForkErrorDetails) error {
	var conflict *apiidempotency.ConflictError
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
	if errors.Is(err, operatorread.ErrSessionNotFound) {
		return NewApplicationError(SessionNotFoundCode, false, map[string]any{"session_id": details.SessionID})
	}
	if errors.Is(err, operatorread.ErrTurnNotFound) {
		errorDetails := map[string]any{"session_id": details.SessionID}
		if strings.TrimSpace(details.TurnID) != "" {
			errorDetails["turn_id"] = details.TurnID
		}
		return NewApplicationError(TurnNotFoundCode, false, errorDetails)
	}
	if errors.Is(err, operatorread.ErrEventNotFound) {
		return NewApplicationError(EventNotFoundCode, false, map[string]any{"event_id": details.EventID})
	}
	if errors.Is(err, runfork.ErrConversationForkNotFound) {
		return NewApplicationError(ForkNotFoundCode, false, map[string]any{"fork_id": details.ForkID})
	}
	if errors.Is(err, runfork.ErrInvalidConversationForkCursor) {
		return NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid conversation fork cursor"})
	}
	if paramErr := entityReadParamError(err); paramErr != nil {
		return NewInvalidParamsError(map[string]any{"field": paramErr.Field, "reason": paramErr.Reason})
	}
	return err
}
