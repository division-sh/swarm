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
	storeoperatorchannel "github.com/division-sh/swarm/internal/store/internal/backend/operatorchannel"
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
	started    time.Time
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
	if err := admitPrincipalTx(ctx, tx, lease.req, true); err != nil {
		return err
	}
	if lease.req.IdempotencyKey == "" {
		return nil
	}
	return storeAPIIdempotency(ctx, tx, completionRequest(lease.req, lease.started), completion)
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
	if req.IdempotencyKey == "" {
		l := &PostgresRequestLease{req: req, started: time.Now()}
		err := owner.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error { return admitPrincipalTx(txctx, tx, req, true) })
		return l, err
	}
	acquire := owner.acquire
	if acquire == nil {
		acquire = acquireAPIIdempotencyLease
	}
	started := time.Now()
	lease, err := acquire(ctx, owner.backend, apiIdempotencyLockKey(req.Method, req.Actor, req.IdempotencyKey))
	if err != nil {
		return nil, err
	}
	requestLease := &PostgresRequestLease{lease: lease, req: req, started: started}
	session := lease.Session()
	if session == nil {
		return nil, errors.Join(fmt.Errorf("api idempotency authority has no current session"), requestLease.Release(ctx))
	}
	var existing apiIdempotencyRecord
	var found bool
	err = postgresbackend.RunAuthorityTransaction(ctx, session, func(sqlCtx context.Context, tx *sql.Tx) error {
		if err := admitPrincipalTx(sqlCtx, tx, req, true); err != nil {
			return err
		}
		if err := purgeExpiredAPIIdempotency(sqlCtx, tx, req.Now); err != nil {
			return err
		}
		var err error
		existing, found, err = loadAPIIdempotency(sqlCtx, tx, req)
		return err
	})
	if err != nil {
		return nil, errors.Join(err, requestLease.Release(ctx))
	}
	if found {
		if completionConflicts(req, existing) {
			return nil, errors.Join(conflictError(req, existing), requestLease.Release(ctx))
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
	if apiidempotencycontract.IsHumanMailboxMethod(req.Method) {
		return completion, false, fmt.Errorf("human mailbox completion requires its domain transaction owner")
	}
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
	if err := postgresbackend.RunAuthorityTransaction(ctx, requestLease.lease.Session(), func(sqlCtx context.Context, tx *sql.Tx) error {
		return StorePostgresCompletionTx(sqlCtx, requestLease, tx, completion)
	}); err != nil {
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
		  AND actor_kind = $2 AND actor_id = $3
		  AND idempotency_key = $4
		  AND expires_at > $5
	`, req.Method, req.Actor.Kind, req.Actor.ID, req.IdempotencyKey, req.Now).Scan(&record.RequestHash, &record.ResourceID, &record.Response)
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
			method, actor_kind, actor_id, idempotency_key, request_hash,
			resource_id, response, created_at, expires_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9)
	`, req.Method, req.Actor.Kind, req.Actor.ID, req.IdempotencyKey, req.RequestHash, strings.TrimSpace(completion.ResourceID), string(completion.Response), req.Now, req.Now.Add(req.TTL))
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

func apiIdempotencyLockKey(method string, actor apiidempotencycontract.Actor, idempotencyKey string) string {
	raw, _ := json.Marshal([]string{method, string(actor.Kind), actor.ID, idempotencyKey})
	return apiIdempotencyLockNamespace + string(raw)
}

type SQLiteOwner struct {
	backend     *sqlitebackend.Backend
	schemaGuard func() error
	path        string
}

type SQLiteRequestLease struct {
	lock       *sync.Mutex
	started    time.Time
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
	if err := admitPrincipalTx(ctx, tx, lease.req, false); err != nil {
		return err
	}
	if lease.req.IdempotencyKey == "" {
		return nil
	}
	return storeSQLite(ctx, tx, completionRequest(lease.req, lease.started), completion)
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
	if req.IdempotencyKey == "" {
		l := &SQLiteRequestLease{req: req, started: time.Now()}
		err := owner.backend.RunTransaction(ctx, "admit mailbox principal", func(txctx context.Context, tx *sql.Tx) error { return admitPrincipalTx(txctx, tx, req, false) })
		return l, err
	}
	started := time.Now()
	lock := sqliteLockForPath(owner.path)
	lock.Lock()
	requestLease := &SQLiteRequestLease{lock: lock, req: req, started: started}
	var existing apiIdempotencyRecord
	var found bool
	err := owner.backend.RunTransaction(ctx, "sqlite api idempotency lookup", func(txCtx context.Context, tx *sql.Tx) error {
		if err := admitPrincipalTx(txCtx, tx, req, false); err != nil {
			return err
		}
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
		if completionConflicts(req, existing) {
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
	if apiidempotencycontract.IsHumanMailboxMethod(req.Method) {
		return apiidempotencycontract.Completion{}, false, fmt.Errorf("human mailbox completion requires its domain transaction owner")
	}
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
		return StoreSQLiteCompletionTx(txCtx, requestLease, tx, completion)
	}); err != nil {
		return apiidempotencycontract.Completion{}, false, err
	}
	return completion, false, nil
}

func normalizeRequest(req apiidempotencycontract.Request) apiidempotencycontract.Request {
	req.Method = strings.TrimSpace(req.Method)
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
	if err := req.Actor.ValidateMethod(req.Method); err != nil {
		return err
	}
	if req.RequestHash == "" || (req.IdempotencyKey == "" && !apiidempotencycontract.IsHumanMailboxMethod(req.Method)) {
		return fmt.Errorf("idempotency key and request hash are required")
	}
	return nil
}

func admitPrincipalTx(ctx context.Context, tx *sql.Tx, req apiidempotencycontract.Request, postgres bool) error {
	if req.Actor.Kind != apiidempotencycontract.ActorOperatorPrincipal {
		return nil
	}
	return storeoperatorchannel.RequirePrincipalTx(ctx, tx, req.Actor.ID, postgres)
}

func normalizeCompletion(req apiidempotencycontract.Request, completion apiidempotencycontract.Completion) (apiidempotencycontract.Completion, error) {
	if len(completion.Response) == 0 || !json.Valid(completion.Response) {
		return apiidempotencycontract.Completion{}, fmt.Errorf("valid API idempotency response is required")
	}
	if req.Actor.Kind == apiidempotencycontract.ActorOperatorPrincipal && (req.ResourceID == "" || completion.ResourceID != req.ResourceID) {
		return apiidempotencycontract.Completion{}, fmt.Errorf("mailbox completion must name the exact request resource")
	}
	if strings.TrimSpace(completion.ResourceID) == "" {
		completion.ResourceID = req.ResourceID
	}
	return cloneCompletion(completion), nil
}

func completionConflicts(req apiidempotencycontract.Request, existing apiIdempotencyRecord) bool {
	return existing.RequestHash != req.RequestHash ||
		(req.Actor.Kind == apiidempotencycontract.ActorOperatorPrincipal && existing.ResourceID != req.ResourceID)
}

// Preserve a caller-supplied admission clock while including lease wait and
// execution time. Retention starts at completion, not request preparation.
func completionRequest(req apiidempotencycontract.Request, started time.Time) apiidempotencycontract.Request {
	req.Now = req.Now.Add(time.Since(started)).UTC()
	return req
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
		WHERE method = ? AND actor_kind = ? AND actor_id = ? AND idempotency_key = ? AND expires_at > ?
	`, req.Method, req.Actor.Kind, req.Actor.ID, req.IdempotencyKey, req.Now.UTC()).Scan(&record.RequestHash, &record.ResourceID, &response)
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
			method, actor_kind, actor_id, idempotency_key, request_hash,
			resource_id, response, created_at, expires_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, req.Method, req.Actor.Kind, req.Actor.ID, req.IdempotencyKey, req.RequestHash, strings.TrimSpace(completion.ResourceID), string(completion.Response), req.Now.UTC(), req.Now.Add(req.TTL).UTC())
	if err != nil {
		return fmt.Errorf("store sqlite api idempotency response: %w", err)
	}
	return nil
}
