package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const apiIdempotencyLockNamespace = "swarm:api-idempotency:"

var ErrAPIIdempotencyConflict = errors.New("api idempotency conflict")

type APIIdempotencyRequest struct {
	Method         string
	ActorTokenID   string
	IdempotencyKey string
	RequestHash    string
	ResourceID     string
	TTL            time.Duration
	Now            time.Time
}

type APIIdempotencyCompletion struct {
	ResourceID string
	Response   json.RawMessage
}

type APIIdempotencyConflictError struct {
	OriginalRequestHash    string
	ConflictingRequestHash string
	Method                 string
	ResourceID             string
}

func (e *APIIdempotencyConflictError) Error() string {
	return "api idempotency conflict"
}

func (e *APIIdempotencyConflictError) Is(target error) bool {
	return target == ErrAPIIdempotencyConflict
}

func (s *PostgresStore) WithAPIIdempotency(
	ctx context.Context,
	req APIIdempotencyRequest,
	execute func(context.Context) (APIIdempotencyCompletion, error),
) (APIIdempotencyCompletion, bool, error) {
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		completion, err := execute(ctx)
		return completion, false, err
	}
	if s == nil || s.DB == nil {
		return APIIdempotencyCompletion{}, false, fmt.Errorf("postgres store is required")
	}
	if execute == nil {
		return APIIdempotencyCompletion{}, false, fmt.Errorf("api idempotency executor is required")
	}
	req.Method = strings.TrimSpace(req.Method)
	req.ActorTokenID = strings.TrimSpace(req.ActorTokenID)
	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	req.RequestHash = strings.TrimSpace(req.RequestHash)
	req.ResourceID = strings.TrimSpace(req.ResourceID)
	if req.Method == "" || req.ActorTokenID == "" || req.RequestHash == "" {
		return APIIdempotencyCompletion{}, false, fmt.Errorf("method, actor token id, and request hash are required")
	}
	if req.TTL <= 0 {
		req.TTL = 24 * time.Hour
	}
	if req.Now.IsZero() {
		req.Now = time.Now().UTC()
	}

	conn, err := s.DB.Conn(ctx)
	if err != nil {
		return APIIdempotencyCompletion{}, false, fmt.Errorf("acquire api idempotency connection: %w", err)
	}
	defer conn.Close()

	lockKey := apiIdempotencyLockKey(req.Method, req.ActorTokenID, req.IdempotencyKey)
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock(hashtext($1))`, lockKey); err != nil {
		return APIIdempotencyCompletion{}, false, fmt.Errorf("lock api idempotency key: %w", err)
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock(hashtext($1))`, lockKey)
	}()

	if err := purgeExpiredAPIIdempotency(ctx, conn, req.Now); err != nil {
		return APIIdempotencyCompletion{}, false, err
	}
	existing, ok, err := loadAPIIdempotency(ctx, conn, req)
	if err != nil {
		return APIIdempotencyCompletion{}, false, err
	}
	if ok {
		if existing.RequestHash != req.RequestHash {
			return APIIdempotencyCompletion{}, false, &APIIdempotencyConflictError{
				OriginalRequestHash:    existing.RequestHash,
				ConflictingRequestHash: req.RequestHash,
				Method:                 req.Method,
				ResourceID:             existing.ResourceID,
			}
		}
		return APIIdempotencyCompletion{
			ResourceID: existing.ResourceID,
			Response:   append(json.RawMessage(nil), existing.Response...),
		}, true, nil
	}

	completion, err := execute(ctx)
	if err != nil {
		return APIIdempotencyCompletion{}, false, err
	}
	if len(completion.Response) == 0 {
		return APIIdempotencyCompletion{}, false, fmt.Errorf("api idempotency response is required")
	}
	if strings.TrimSpace(completion.ResourceID) == "" {
		completion.ResourceID = req.ResourceID
	}
	if err := storeAPIIdempotency(ctx, conn, req, completion); err != nil {
		return APIIdempotencyCompletion{}, false, err
	}
	return completion, false, nil
}

type apiIdempotencyRecord struct {
	RequestHash string
	ResourceID  string
	Response    json.RawMessage
}

func loadAPIIdempotency(ctx context.Context, q *sql.Conn, req APIIdempotencyRequest) (apiIdempotencyRecord, bool, error) {
	var record apiIdempotencyRecord
	err := q.QueryRowContext(ctx, `
		SELECT request_hash, resource_id, response
		FROM api_idempotency
		WHERE method = $1
		  AND actor_token_id = $2
		  AND idempotency_key = $3
		  AND expires_at > $4
	`, req.Method, req.ActorTokenID, req.IdempotencyKey, req.Now).Scan(&record.RequestHash, &record.ResourceID, &record.Response)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return apiIdempotencyRecord{}, false, nil
	case err != nil:
		return apiIdempotencyRecord{}, false, fmt.Errorf("load api idempotency response: %w", err)
	default:
		return record, true, nil
	}
}

func storeAPIIdempotency(ctx context.Context, q *sql.Conn, req APIIdempotencyRequest, completion APIIdempotencyCompletion) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO api_idempotency (
			method, actor_token_id, idempotency_key, request_hash,
			resource_id, response, created_at, expires_at
		)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
	`, req.Method, req.ActorTokenID, req.IdempotencyKey, req.RequestHash, strings.TrimSpace(completion.ResourceID), string(completion.Response), req.Now, req.Now.Add(req.TTL))
	if err != nil {
		return fmt.Errorf("store api idempotency response: %w", err)
	}
	return nil
}

func purgeExpiredAPIIdempotency(ctx context.Context, q *sql.Conn, now time.Time) error {
	_, err := q.ExecContext(ctx, `DELETE FROM api_idempotency WHERE expires_at <= $1`, now)
	if err != nil {
		return fmt.Errorf("purge expired api idempotency responses: %w", err)
	}
	return nil
}

func apiIdempotencyLockKey(method, actorTokenID, idempotencyKey string) string {
	return apiIdempotencyLockNamespace + strings.Join([]string{
		strings.TrimSpace(method),
		strings.TrimSpace(actorTokenID),
		strings.TrimSpace(idempotencyKey),
	}, "|")
}
