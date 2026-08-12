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
	acquire     func(context.Context, *postgresbackend.Backend, string) (*postgresbackend.AdvisoryLockLease, error)
}

type PostgresRequestLease struct {
	lease      *postgresbackend.AdvisoryLockLease
	req        apiidempotencycontract.Request
	completion apiidempotencycontract.Completion
	replay     bool
}

func (l *PostgresRequestLease) Replay() (apiidempotencycontract.Completion, bool) {
	if l == nil || !l.replay {
		return apiidempotencycontract.Completion{}, false
	}
	return cloneCompletion(l.completion), true
}

func StorePostgresCompletionTx(ctx context.Context, lease *PostgresRequestLease, tx *sql.Tx, completion apiidempotencycontract.Completion) error {
	if lease == nil || tx == nil {
		return fmt.Errorf("PostgreSQL API idempotency transaction lease is required")
	}
	completion, err := normalizeCompletion(lease.req, completion)
	if err != nil {
		return err
	}
	return storeAPIIdempotency(ctx, tx, lease.req, completion)
}

func (l *PostgresRequestLease) Release(ctx context.Context) error {
	if l == nil || l.lease == nil {
		return nil
	}
	lease := l.lease
	l.lease = nil
	return lease.ReleaseTerminal(context.WithoutCancel(ctx))
}

func AcquirePostgresRequest(ctx context.Context, owner *PostgresOwner, req apiidempotencycontract.Request) (*PostgresRequestLease, error) {
	if owner == nil || owner.backend == nil {
		return nil, fmt.Errorf("postgres store is required")
	}
	if err := owner.schemaGuard(); err != nil {
		return nil, err
	}
	req = normalizeRequest(req)
	if err := validateRequest(req); err != nil {
		return nil, err
	}
	acquire := owner.acquire
	if acquire == nil {
		acquire = acquireAPIIdempotencyLease
	}
	lease, err := acquire(ctx, owner.backend, apiIdempotencyLockKey(req.Method, req.ActorTokenID, req.IdempotencyKey))
	if err != nil {
		return nil, err
	}
	requestLease := &PostgresRequestLease{lease: lease, req: req}
	session := lease.Session()
	if session == nil {
		_ = requestLease.Release(ctx)
		return nil, fmt.Errorf("api idempotency authority has no current session")
	}
	if err := purgeExpiredAPIIdempotency(ctx, session, req.Now); err != nil {
		_ = requestLease.Release(ctx)
		return nil, err
	}
	existing, found, err := loadAPIIdempotency(ctx, session, req)
	if err != nil {
		_ = requestLease.Release(ctx)
		return nil, err
	}
	if found {
		if existing.RequestHash != req.RequestHash {
			_ = requestLease.Release(ctx)
			return nil, conflictError(req, existing)
		}
		requestLease.replay = true
		requestLease.completion = apiidempotencycontract.Completion{ResourceID: existing.ResourceID, Response: append(json.RawMessage(nil), existing.Response...)}
	}
	return requestLease, nil
}

func NewPostgres(backend *postgresbackend.Backend, schemaGuard func() error) (*PostgresOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("api idempotency postgres backend is required")
	}
	if schemaGuard == nil {
		return nil, fmt.Errorf("api idempotency postgres schema guard is required")
	}
	return &PostgresOwner{backend: backend, schemaGuard: schemaGuard, acquire: acquireAPIIdempotencyLease}, nil
}

func (s *PostgresOwner) WithAPIIdempotency(
	ctx context.Context,
	req apiidempotencycontract.Request,
	execute func(context.Context) (apiidempotencycontract.Completion, error),
) (completion apiidempotencycontract.Completion, replay bool, err error) {
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
	requestLease, err := AcquirePostgresRequest(ctx, s, req)
	if err != nil {
		return apiidempotencycontract.Completion{}, false, err
	}
	defer func() {
		if releaseErr := requestLease.Release(ctx); releaseErr != nil {
			completion = apiidempotencycontract.Completion{}
			replay = false
			err = errors.Join(err, fmt.Errorf("release api idempotency authority: %w", releaseErr))
		}
	}()
	if existing, ok := requestLease.Replay(); ok {
		return existing, true, nil
	}

	completion, err = execute(ctx)
	if err != nil {
		return apiidempotencycontract.Completion{}, false, err
	}
	completion, err = normalizeCompletion(requestLease.req, completion)
	if err != nil {
		return apiidempotencycontract.Completion{}, false, err
	}
	if err := storeAPIIdempotency(ctx, requestLease.lease.Session(), requestLease.req, completion); err != nil {
		return apiidempotencycontract.Completion{}, false, err
	}
	return completion, false, nil
}

func acquireAPIIdempotencyLease(ctx context.Context, backend *postgresbackend.Backend, lockKey string) (*postgresbackend.AdvisoryLockLease, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("api idempotency postgres backend is required")
	}
	releaseCapacity := backend.RetainConnectionCapacity()
	lease, acquired, err := postgresbackend.AcquireAdvisoryLockLeaseWith(ctx, backend, lockKey,
		func(ctx context.Context, session *postgresbackend.SessionAuthority, key string) (bool, error) {
			_, err := session.ExecContext(ctx, `SELECT pg_advisory_lock(hashtext($1))`, key)
			return err == nil, err
		})
	if err != nil {
		releaseCapacity()
		return nil, fmt.Errorf("lock api idempotency key: %w", err)
	}
	if !acquired || lease == nil {
		releaseCapacity()
		return nil, fmt.Errorf("lock api idempotency key: blocking acquisition returned without authority")
	}
	if !lease.InstallTerminalOwner(releaseCapacity, nil, nil) {
		releaseCapacity()
		return nil, errors.Join(
			fmt.Errorf("lock api idempotency key: authority retired before ownership transfer"),
			lease.ReleaseTerminal(context.WithoutCancel(ctx)),
		)
	}
	return lease, nil
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

type SQLiteRequestLease struct {
	lock       *sync.Mutex
	req        apiidempotencycontract.Request
	completion apiidempotencycontract.Completion
	replay     bool
	released   bool
}

func (l *SQLiteRequestLease) Replay() (apiidempotencycontract.Completion, bool) {
	if l == nil || !l.replay {
		return apiidempotencycontract.Completion{}, false
	}
	return cloneCompletion(l.completion), true
}

func StoreSQLiteCompletionTx(ctx context.Context, lease *SQLiteRequestLease, tx *sql.Tx, completion apiidempotencycontract.Completion) error {
	if lease == nil || tx == nil {
		return fmt.Errorf("SQLite API idempotency transaction lease is required")
	}
	completion, err := normalizeCompletion(lease.req, completion)
	if err != nil {
		return err
	}
	return storeSQLite(ctx, tx, lease.req, completion)
}

func (l *SQLiteRequestLease) Release() {
	if l == nil || l.released || l.lock == nil {
		return
	}
	l.released = true
	l.lock.Unlock()
}

func AcquireSQLiteRequest(ctx context.Context, owner *SQLiteOwner, req apiidempotencycontract.Request) (*SQLiteRequestLease, error) {
	if owner == nil || owner.backend == nil {
		return nil, fmt.Errorf("sqlite api idempotency owner is required")
	}
	if err := owner.schemaGuard(); err != nil {
		return nil, err
	}
	req = normalizeRequest(req)
	if err := validateRequest(req); err != nil {
		return nil, err
	}
	lock := sqliteLockForPath(owner.path)
	lock.Lock()
	requestLease := &SQLiteRequestLease{lock: lock, req: req}
	var existing apiIdempotencyRecord
	var found bool
	err := owner.backend.RunTransaction(ctx, "sqlite api idempotency lookup", func(txCtx context.Context, tx *sql.Tx) error {
		if err := purgeExpiredSQLite(txCtx, tx, req.Now); err != nil {
			return err
		}
		var err error
		existing, found, err = loadSQLite(txCtx, tx, req)
		return err
	})
	if err != nil {
		requestLease.Release()
		return nil, err
	}
	if found {
		if existing.RequestHash != req.RequestHash {
			requestLease.Release()
			return nil, conflictError(req, existing)
		}
		requestLease.replay = true
		requestLease.completion = apiidempotencycontract.Completion{ResourceID: existing.ResourceID, Response: append(json.RawMessage(nil), existing.Response...)}
	}
	return requestLease, nil
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
	requestLease, err := AcquireSQLiteRequest(ctx, s, req)
	if err != nil {
		return apiidempotencycontract.Completion{}, false, err
	}
	defer requestLease.Release()
	if existing, ok := requestLease.Replay(); ok {
		return existing, true, nil
	}
	completion, err := execute(ctx)
	if err != nil {
		return apiidempotencycontract.Completion{}, false, err
	}
	completion, err = normalizeCompletion(requestLease.req, completion)
	if err != nil {
		return apiidempotencycontract.Completion{}, false, err
	}
	if err := s.backend.RunTransaction(ctx, "sqlite api idempotency completion", func(txCtx context.Context, tx *sql.Tx) error {
		return storeSQLite(txCtx, tx, requestLease.req, completion)
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

func validateRequest(req apiidempotencycontract.Request) error {
	if req.Method == "" || req.ActorTokenID == "" || req.RequestHash == "" || req.IdempotencyKey == "" {
		return fmt.Errorf("method, actor token id, idempotency key, and request hash are required")
	}
	return nil
}

func normalizeCompletion(req apiidempotencycontract.Request, completion apiidempotencycontract.Completion) (apiidempotencycontract.Completion, error) {
	if len(completion.Response) == 0 {
		return apiidempotencycontract.Completion{}, fmt.Errorf("api idempotency response is required")
	}
	if strings.TrimSpace(completion.ResourceID) == "" {
		completion.ResourceID = req.ResourceID
	}
	return cloneCompletion(completion), nil
}

func cloneCompletion(completion apiidempotencycontract.Completion) apiidempotencycontract.Completion {
	completion.ResourceID = strings.TrimSpace(completion.ResourceID)
	completion.Response = append(json.RawMessage(nil), completion.Response...)
	return completion
}

func conflictError(req apiidempotencycontract.Request, existing apiIdempotencyRecord) error {
	return &apiidempotencycontract.ConflictError{
		OriginalRequestHash: existing.RequestHash, ConflictingRequestHash: req.RequestHash,
		Method: req.Method, ResourceID: existing.ResourceID,
	}
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
