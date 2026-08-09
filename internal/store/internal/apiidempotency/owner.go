package apiidempotency

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	apiidempotencycontract "github.com/division-sh/swarm/internal/apiidempotency"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

const apiIdempotencyLockNamespace = "swarm:api-idempotency:"

type PostgresOwner struct {
	backend     *postgresbackend.Backend
	schemaGuard func() error
}

func NewPostgres(backend *postgresbackend.Backend, schemaGuard func() error) (*PostgresOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("api idempotency postgres backend is required")
	}
	if schemaGuard == nil {
		return nil, fmt.Errorf("api idempotency postgres schema guard is required")
	}
	return &PostgresOwner{backend: backend, schemaGuard: schemaGuard}, nil
}

func (s *PostgresOwner) WithAPIIdempotency(
	ctx context.Context,
	req apiidempotencycontract.Request,
	execute func(context.Context) (apiidempotencycontract.Completion, error),
) (apiidempotencycontract.Completion, bool, error) {
	if execute == nil {
		return apiidempotencycontract.Completion{}, false, fmt.Errorf("api idempotency executor is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		completion, err := execute(ctx)
		return completion, false, err
	}
	if s == nil || s.backend == nil {
		return apiidempotencycontract.Completion{}, false, fmt.Errorf("postgres store is required")
	}
	if err := s.schemaGuard(); err != nil {
		return apiidempotencycontract.Completion{}, false, err
	}
	req.Method = strings.TrimSpace(req.Method)
	req.ActorTokenID = strings.TrimSpace(req.ActorTokenID)
	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	req.RequestHash = strings.TrimSpace(req.RequestHash)
	req.ResourceID = strings.TrimSpace(req.ResourceID)
	if req.Method == "" || req.ActorTokenID == "" || req.RequestHash == "" {
		return apiidempotencycontract.Completion{}, false, fmt.Errorf("method, actor token id, and request hash are required")
	}
	if req.TTL <= 0 {
		req.TTL = 24 * time.Hour
	}
	if req.Now.IsZero() {
		req.Now = time.Now().UTC()
	}
	releaseCapacity := s.backend.RetainConnectionCapacity()
	defer releaseCapacity()
	conn, err := s.backend.Conn(ctx)
	if err != nil {
		return apiidempotencycontract.Completion{}, false, fmt.Errorf("acquire api idempotency connection: %w", err)
	}
	defer conn.Close()

	lockKey := apiIdempotencyLockKey(req.Method, req.ActorTokenID, req.IdempotencyKey)
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock(hashtext($1))`, lockKey); err != nil {
		return apiidempotencycontract.Completion{}, false, fmt.Errorf("lock api idempotency key: %w", err)
	}
	defer func() {
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock(hashtext($1))`, lockKey)
	}()

	if err := purgeExpiredAPIIdempotency(ctx, conn, req.Now); err != nil {
		return apiidempotencycontract.Completion{}, false, err
	}
	existing, ok, err := loadAPIIdempotency(ctx, conn, req)
	if err != nil {
		return apiidempotencycontract.Completion{}, false, err
	}
	if ok {
		if existing.RequestHash != req.RequestHash {
			return apiidempotencycontract.Completion{}, false, &apiidempotencycontract.ConflictError{
				OriginalRequestHash:    existing.RequestHash,
				ConflictingRequestHash: req.RequestHash,
				Method:                 req.Method,
				ResourceID:             existing.ResourceID,
			}
		}
		return apiidempotencycontract.Completion{
			ResourceID: existing.ResourceID,
			Response:   append(json.RawMessage(nil), existing.Response...),
		}, true, nil
	}

	completion, err := execute(ctx)
	if err != nil {
		return apiidempotencycontract.Completion{}, false, err
	}
	if len(completion.Response) == 0 {
		return apiidempotencycontract.Completion{}, false, fmt.Errorf("api idempotency response is required")
	}
	if strings.TrimSpace(completion.ResourceID) == "" {
		completion.ResourceID = req.ResourceID
	}
	if err := storeAPIIdempotency(ctx, conn, req, completion); err != nil {
		return apiidempotencycontract.Completion{}, false, err
	}
	return completion, false, nil
}

type apiIdempotencyRecord struct {
	RequestHash string
	ResourceID  string
	Response    json.RawMessage
}

type execQueryer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadAPIIdempotency(ctx context.Context, q execQueryer, req apiidempotencycontract.Request) (apiIdempotencyRecord, bool, error) {
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

func storeAPIIdempotency(ctx context.Context, q execQueryer, req apiidempotencycontract.Request, completion apiidempotencycontract.Completion) error {
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

func purgeExpiredAPIIdempotency(ctx context.Context, q execQueryer, now time.Time) error {
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

type SQLiteOwner struct {
	backend     *sqlitebackend.Backend
	schemaGuard func() error
	path        string
}

var sqliteLocks = struct {
	sync.Mutex
	byPath map[string]*sync.Mutex
}{byPath: map[string]*sync.Mutex{}}

func NewSQLite(backend *sqlitebackend.Backend, path string, schemaGuard func() error) (*SQLiteOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("api idempotency sqlite backend is required")
	}
	if schemaGuard == nil {
		return nil, fmt.Errorf("api idempotency sqlite schema guard is required")
	}
	return &SQLiteOwner{backend: backend, path: path, schemaGuard: schemaGuard}, nil
}

func (s *SQLiteOwner) WithAPIIdempotency(ctx context.Context, req apiidempotencycontract.Request, execute func(context.Context) (apiidempotencycontract.Completion, error)) (apiidempotencycontract.Completion, bool, error) {
	if execute == nil {
		return apiidempotencycontract.Completion{}, false, fmt.Errorf("api idempotency executor is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		completion, err := execute(ctx)
		return completion, false, err
	}
	if s == nil || s.backend == nil {
		return apiidempotencycontract.Completion{}, false, fmt.Errorf("sqlite api idempotency owner is required")
	}
	if err := s.schemaGuard(); err != nil {
		return apiidempotencycontract.Completion{}, false, err
	}
	req = normalizeRequest(req)
	if req.Method == "" || req.ActorTokenID == "" || req.RequestHash == "" {
		return apiidempotencycontract.Completion{}, false, fmt.Errorf("method, actor token id, and request hash are required")
	}
	lock := sqliteLockForPath(s.path)
	lock.Lock()
	defer lock.Unlock()

	var existing apiIdempotencyRecord
	var found bool
	err := s.backend.RunTransaction(ctx, "sqlite api idempotency lookup", func(txCtx context.Context, tx *sql.Tx) error {
		if err := purgeExpiredSQLite(txCtx, tx, req.Now); err != nil {
			return err
		}
		var err error
		existing, found, err = loadSQLite(txCtx, tx, req)
		return err
	})
	if err != nil {
		return apiidempotencycontract.Completion{}, false, err
	}
	if found {
		if existing.RequestHash != req.RequestHash {
			return apiidempotencycontract.Completion{}, false, &apiidempotencycontract.ConflictError{OriginalRequestHash: existing.RequestHash, ConflictingRequestHash: req.RequestHash, Method: req.Method, ResourceID: existing.ResourceID}
		}
		return apiidempotencycontract.Completion{ResourceID: existing.ResourceID, Response: append(json.RawMessage(nil), existing.Response...)}, true, nil
	}
	completion, err := execute(ctx)
	if err != nil {
		return apiidempotencycontract.Completion{}, false, err
	}
	if len(completion.Response) == 0 {
		return apiidempotencycontract.Completion{}, false, fmt.Errorf("api idempotency response is required")
	}
	if strings.TrimSpace(completion.ResourceID) == "" {
		completion.ResourceID = req.ResourceID
	}
	if err := s.backend.RunTransaction(ctx, "sqlite api idempotency completion", func(txCtx context.Context, tx *sql.Tx) error {
		return storeSQLite(txCtx, tx, req, completion)
	}); err != nil {
		return apiidempotencycontract.Completion{}, false, err
	}
	return completion, false, nil
}

func normalizeRequest(req apiidempotencycontract.Request) apiidempotencycontract.Request {
	req.Method = strings.TrimSpace(req.Method)
	req.ActorTokenID = strings.TrimSpace(req.ActorTokenID)
	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	req.RequestHash = strings.TrimSpace(req.RequestHash)
	req.ResourceID = strings.TrimSpace(req.ResourceID)
	if req.Now.IsZero() {
		req.Now = time.Now().UTC()
	}
	if req.TTL <= 0 {
		req.TTL = 24 * time.Hour
	}
	return req
}

func sqliteLockForPath(path string) *sync.Mutex {
	key := strings.TrimSpace(path)
	if key == "" {
		key = "<unknown>"
	} else if absolute, err := filepath.Abs(filepath.Clean(key)); err == nil {
		key = absolute
	}
	sqliteLocks.Lock()
	defer sqliteLocks.Unlock()
	lock := sqliteLocks.byPath[key]
	if lock == nil {
		lock = &sync.Mutex{}
		sqliteLocks.byPath[key] = lock
	}
	return lock
}

func purgeExpiredSQLite(ctx context.Context, q execQueryer, now time.Time) error {
	if _, err := q.ExecContext(ctx, `DELETE FROM api_idempotency WHERE expires_at <= ?`, now.UTC()); err != nil {
		return fmt.Errorf("purge expired sqlite api idempotency: %w", err)
	}
	return nil
}

func loadSQLite(ctx context.Context, q execQueryer, req apiidempotencycontract.Request) (apiIdempotencyRecord, bool, error) {
	var record apiIdempotencyRecord
	var response []byte
	err := q.QueryRowContext(ctx, `
		SELECT request_hash, resource_id, response
		FROM api_idempotency
		WHERE method = ? AND actor_token_id = ? AND idempotency_key = ? AND expires_at > ?
	`, req.Method, req.ActorTokenID, req.IdempotencyKey, req.Now.UTC()).Scan(&record.RequestHash, &record.ResourceID, &response)
	if errors.Is(err, sql.ErrNoRows) {
		return apiIdempotencyRecord{}, false, nil
	}
	if err != nil {
		return apiIdempotencyRecord{}, false, fmt.Errorf("load sqlite api idempotency response: %w", err)
	}
	record.Response = json.RawMessage(response)
	return record, true, nil
}

func storeSQLite(ctx context.Context, q execQueryer, req apiidempotencycontract.Request, completion apiidempotencycontract.Completion) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO api_idempotency (
			method, actor_token_id, idempotency_key, request_hash,
			resource_id, response, created_at, expires_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, req.Method, req.ActorTokenID, req.IdempotencyKey, req.RequestHash, strings.TrimSpace(completion.ResourceID), string(completion.Response), req.Now.UTC(), req.Now.Add(req.TTL).UTC())
	if err != nil {
		return fmt.Errorf("store sqlite api idempotency response: %w", err)
	}
	return nil
}
