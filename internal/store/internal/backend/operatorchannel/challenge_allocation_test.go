package operatorchannel

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	domain "github.com/division-sh/swarm/internal/operatorchannel"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/schemastore"
	"github.com/division-sh/swarm/tests/support/testutil"
	"github.com/google/uuid"
)

func TestOperatorChallengeAllocationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, d, principal := challengeAllocationFixture(t, backend)
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			operation := func() domain.Operation {
				return domain.Operation{OperationID: uuid.NewString(), Kind: domain.OperationConnect, PrincipalID: principal,
					Interface: domain.InterfaceIdentity{InterfaceRef: domain.InterfaceHITLChannelV2, ChannelPackID: "provider.telegram.hitl_channel", ChannelPackVersion: "1.0.0", ChannelManifestHash: "manifest", SemanticGeneration: "plan"},
					State:     domain.StateAwaitingClaim, Revision: 1, ProofStatus: domain.ProofSkipped,
					RequestedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(domain.DefaultChallengeTTL),
					ProviderAuthority: domain.ProviderAuthority{Kind: domain.ProviderAuthorityCredential, Credential: runtimecredentials.ValueEvidence{
						Key: "channel.telegram.provider", Seal: runtimecredentials.ValueSeal("credential-value-seal-v1:" + strings.Repeat("a", 64))}}}
			}
			allocate := func(ctx context.Context, op *domain.Operation, key string, draw func() (string, error)) error {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					return err
				}
				defer tx.Rollback()
				if err := allocateOperationChallenge(ctx, op, draw, func(candidate domain.Operation) error {
					return insertOperation(ctx, tx, d, candidate, key, key, 0, "")
				}); err != nil {
					return err
				}
				var count int
				if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM operator_channel_operations`).Scan(&count); err != nil {
					return fmt.Errorf("transaction unusable after allocation: %w", err)
				}
				return tx.Commit()
			}
			fixed := func(code string) func() (string, error) { return func() (string, error) { return code, nil } }
			first := operation()
			if err := allocate(ctx, &first, first.OperationID, fixed("00000")); err != nil {
				t.Fatal(err)
			}
			second, draws := operation(), 0
			if err := allocate(ctx, &second, second.OperationID, func() (string, error) {
				draws++
				return []string{"00000", "00001"}[draws-1], nil
			}); err != nil || second.Challenge != "00001" || draws != 2 {
				t.Fatalf("forced collision: code=%q draws=%d err=%v", second.Challenge, draws, err)
			}
			for _, failure := range []string{"bounded_collision", "request_key", "state_constraint", "entropy", "cancelled"} {
				t.Run(failure, func(t *testing.T) {
					op, key, calls := operation(), uuid.NewString(), 0
					attemptCtx := ctx
					cause := errors.New("entropy unavailable")
					if failure == "request_key" {
						key = first.OperationID
					} else if failure == "state_constraint" {
						op.State = "not-a-state"
					} else if failure == "cancelled" {
						var stop context.CancelFunc
						attemptCtx, stop = context.WithCancel(ctx)
						stop()
					}
					err := allocate(attemptCtx, &op, key, func() (string, error) {
						calls++
						if failure == "entropy" {
							return "", cause
						}
						if failure == "bounded_collision" {
							return "00000", nil
						}
						return "99999", nil
					})
					wantCalls := 1
					if failure == "bounded_collision" {
						wantCalls = challengeAllocationDraws
						if !errors.Is(err, domain.ErrConflict) || !strings.Contains(err.Error(), "after 16 draws") {
							t.Fatalf("untruthful allocation refusal: %v", err)
						}
					} else if failure == "cancelled" {
						wantCalls = 0
						if !errors.Is(err, context.Canceled) {
							t.Fatal(err)
						}
					} else if failure == "entropy" && !errors.Is(err, cause) {
						t.Fatal(err)
					}
					if err == nil || errors.Is(err, errChallengeCollision) || calls != wantCalls {
						t.Fatalf("failure=%s calls=%d want=%d err=%v", failure, calls, wantCalls, err)
					}
				})
			}
			type allocation struct {
				code  string
				draws int
				err   error
			}
			sampled, release, done := make(chan struct{}, 2), make(chan struct{}), make(chan allocation, 2)
			for _, fallback := range []string{"00003", "00004"} {
				go func() {
					op, calls := operation(), 0
					err := allocate(ctx, &op, op.OperationID, func() (string, error) {
						calls++
						if calls == 1 {
							sampled <- struct{}{}
							select {
							case <-release:
								return "00002", nil
							case <-ctx.Done():
								return "", ctx.Err()
							}
						}
						return fallback, nil
					})
					done <- allocation{op.Challenge, calls, err}
				}()
			}
			for range 2 {
				select {
				case <-sampled:
				case <-ctx.Done():
					t.Error("concurrent allocation did not reach the draw barrier", ctx.Err())
				}
			}
			close(release)
			one, two := <-done, <-done
			if one.err != nil || two.err != nil || one.code == two.code || one.draws+two.draws != 3 ||
				one.code != "00002" && two.code != "00002" {
				t.Fatalf("same-new-code contention: first=%+v second=%+v", one, two)
			}
			var count, unique int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*), COUNT(DISTINCT challenge) FROM operator_channel_operations`).Scan(&count, &unique); err != nil || count != 4 || unique != 4 {
				t.Fatalf("committed allocation set: rows=%d unique=%d err=%v", count, unique, err)
			}
		})
	}
}

func challengeAllocationFixture(t *testing.T, backend string) (*sql.DB, dialect, string) {
	t.Helper()
	var db *sql.DB
	d := dialectPostgres
	if backend == "postgres" {
		_, db, _ = testutil.StartPostgres(t)
	} else {
		d = dialectSQLite
		dsn := "file:" + filepath.Join(t.TempDir(), "codes.db") + "?_pragma=foreign_keys(ON)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)"
		var err error
		db, err = sql.Open("sqlite", dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		b, err := sqlitebackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		schema, err := schemastore.NewSQLiteWithBackend(b, dsn)
		if err != nil {
			t.Fatal(err)
		}
		spec, err := runtimecontracts.LoadPlatformSpecDocument(filepath.Join("..", "..", "..", "..", "..", "platform-spec.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		plans, err := schemastore.GeneratePlatformTableDDLs(spec)
		if err != nil {
			t.Fatal(err)
		}
		if err := schema.BootstrapSchema(t.Context(), schemastore.SchemaBootstrapRequest{PlatformPlans: plans,
			Origin: schemastore.RuntimeStoreOrigin{SwarmVersion: "challenge-test", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC()}}); err != nil {
			t.Fatal(err)
		}
	}
	principal := uuid.NewString()
	if _, err := db.ExecContext(t.Context(), d.bind(`INSERT INTO operator_principals (singleton_id, principal_id, created_at) VALUES (1, ?, ?)`), principal, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	return db, d, principal
}
