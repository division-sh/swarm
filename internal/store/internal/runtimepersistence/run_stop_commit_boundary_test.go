package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"modernc.org/sqlite"
)

// This connector changes only a test database's physical Commit boundary. All
// SQL, lifecycle mutations, rollback, and owner finalizers are production code.
// No global driver registration or production transaction-wrapper hook exists.
type stopCommitConnector struct {
	driver driver.Driver
	dsn    string
	mu     sync.Mutex
	next   func(driver.Tx) error
	begin  chan struct{}
}

func (c *stopCommitConnector) Driver() driver.Driver { return c.driver }
func (c *stopCommitConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &stopCommitConn{Conn: conn, owner: c}, nil
}

func (c *stopCommitConnector) arm(next func(driver.Tx) error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.next = next
}

type stopCommitConn struct {
	driver.Conn
	owner *stopCommitConnector
}

func (c *stopCommitConn) Begin() (driver.Tx, error) {
	tx, err := c.Conn.Begin()
	if err != nil {
		return nil, err
	}
	return &stopCommitTx{Tx: tx, owner: c.owner}, nil
}

func (c *stopCommitConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	if begin, ok := c.Conn.(driver.ConnBeginTx); ok {
		tx, err := begin.BeginTx(ctx, options)
		if err != nil {
			return nil, err
		}
		c.owner.mu.Lock()
		if c.owner.begin != nil {
			close(c.owner.begin)
			c.owner.begin = nil
		}
		c.owner.mu.Unlock()
		return &stopCommitTx{Tx: tx, owner: c.owner}, nil
	}
	if options.Isolation != 0 || options.ReadOnly {
		return nil, errors.New("test driver requires native transaction options")
	}
	return c.Begin()
}

type stopCommitTx struct {
	driver.Tx
	owner *stopCommitConnector
}

func (tx *stopCommitTx) Commit() error {
	tx.owner.mu.Lock()
	next := tx.owner.next
	tx.owner.next = nil
	tx.owner.mu.Unlock()
	if next != nil {
		return next(tx.Tx)
	}
	return tx.Tx.Commit()
}

func newStopCommitStore(t *testing.T, backend string) (selectedFanOutLifecycleOwner, *sql.DB, *stopCommitConnector) {
	t.Helper()
	connector := &stopCommitConnector{}
	if backend == "postgres" {
		connector.dsn, _, _ = testutil.StartPostgres(t)
		connector.driver = &pq.Driver{}
	} else {
		path := filepath.Join(t.TempDir(), "stop-commit.sqlite")
		newBootstrappedSQLiteRuntimeStoreForPath(t, path)
		connector.dsn = "file:" + path + "?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
		connector.driver = &sqlite.Driver{}
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(12)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return newStopCommitStoreOwner(t, backend, db), db, connector
}

func newStopCommitStoreOwner(t *testing.T, backend string, db *sql.DB) selectedFanOutLifecycleOwner {
	t.Helper()
	if backend == "postgres" {
		return admitTestPostgresStore(t, db)
	}
	selected := NewSQLiteRuntimeStoreForTest(db)
	selected.schema.AcceptCurrentForTest()
	selected.SetEventPayloadAdmitter(storeTestPayloadAdmitter)
	registerTestAuthorActivityCatalog(t, selected)
	return selected
}

func TestRunStopCommitUncertaintyPreservesRealOutcomeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, committed := range []bool{false, true} {
			name := "rollback_before_ack"
			if committed {
				name = "commit_before_lost_ack"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				owner, db, connector := newStopCommitStore(t, backend)
				ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 10*time.Second)
				defer cancel()
				fixture := seedFanOutOwnerFixture(t, ctx, db, owner, backend == "postgres", 3, time.Now().UTC())
				ctx, cancel = context.WithTimeout(testAuthorActivityContextForBundle(fixture.bundleHash), 10*time.Second)
				defer cancel()
				before := readStopCommitEvidence(t, db, fixture.runID)
				cause := errors.New("injected stop COMMIT acknowledgement unavailable")
				var calls atomic.Int32
				connector.arm(func(tx driver.Tx) error {
					calls.Add(1)
					if committed {
						return errors.Join(cause, tx.Commit())
					}
					return errors.Join(cause, tx.Rollback())
				})
				state, err := owner.StopRunControl(ctx, runcontrol.TransitionRequest{RunID: fixture.runID})
				failure, typed := runtimefailures.EnvelopeFromError(err)
				if !errors.Is(err, cause) || !typed || failure.Class != runtimefailures.ClassOutcomeUncertain || failure.Detail.Code != "run_stop_commit_unconfirmed" || failure.Detail.Attributes["stage"] != "commit" || failure.Retryable || state.RunID != "" || calls.Load() != 1 {
					t.Fatalf("uncertain stop: state=%+v error=%v failure=%+v calls=%d", state, err, failure, calls.Load())
				}
				after := readStopCommitEvidence(t, db, fixture.runID)
				if !committed {
					if after != before {
						t.Fatalf("unacknowledged rollback changed state: before=%+v after=%+v", before, after)
					}
				} else {
					if after.status != "cancelled" || after.control != "stopped" || after.canceled != 1 || after.pending != 0 || after.revisions != before.revisions+1 || after.outcomes != before.outcomes {
						t.Fatalf("unacknowledged real commit missing atomic evidence: before=%+v after=%+v", before, after)
					}
					if _, err := owner.StopRunControl(ctx, runcontrol.TransitionRequest{RunID: fixture.runID}); !errors.Is(err, runcontrol.ErrAlreadyTerminal) {
						t.Fatalf("uncertain committed stop repeated transition: %v", err)
					}
					if again := readStopCommitEvidence(t, db, fixture.runID); again != after {
						t.Fatalf("terminal refusal changed committed evidence: before=%+v after=%+v", after, again)
					}
				}
			})
		}
	}
}

func TestRunStopKnownCommitRejectionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			owner, db, connector := newStopCommitStore(t, backend)
			fixture := seedFanOutOwnerFixture(t, testAuthorActivityContext(), db, owner, backend == "postgres", 3, time.Now().UTC())
			ctx, cancel := context.WithTimeout(testAuthorActivityContextForBundle(fixture.bundleHash), 10*time.Second)
			defer cancel()
			statements := []string{
				`CREATE TABLE stop_commit_parent (id INTEGER PRIMARY KEY)`,
				`CREATE TABLE stop_commit_child (id INTEGER REFERENCES stop_commit_parent(id) DEFERRABLE INITIALLY DEFERRED)`,
			}
			if backend == "postgres" {
				statements = append(statements,
					`CREATE FUNCTION stop_commit_violation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO stop_commit_child(id) VALUES (1); RETURN NEW; END $$`,
					`CREATE TRIGGER stop_commit_violation AFTER UPDATE ON runs FOR EACH ROW WHEN (NEW.status='cancelled') EXECUTE FUNCTION stop_commit_violation()`)
			} else {
				statements = append(statements, `CREATE TRIGGER stop_commit_violation AFTER UPDATE ON runs WHEN NEW.status='cancelled' BEGIN INSERT INTO stop_commit_child(id) VALUES (1); END`)
			}
			for _, statement := range statements {
				if _, err := db.ExecContext(ctx, statement); err != nil {
					t.Fatal(err)
				}
			}
			before := readStopCommitEvidence(t, db, fixture.runID)
			var commitErr error
			var calls int
			connector.arm(func(tx driver.Tx) error {
				calls++
				commitErr = tx.Commit()
				return commitErr
			})
			state, err := owner.StopRunControl(ctx, runcontrol.TransitionRequest{RunID: fixture.runID})
			failure, ok := runtimefailures.EnvelopeFromError(err)
			if calls != 1 || commitErr == nil || !errors.Is(err, commitErr) || !ok || failure.Class != runtimefailures.ClassInternalFailure || failure.Detail.Code != "run_stop_failed" || failure.Detail.Attributes["stage"] != "commit" || failure.Retryable || state.RunID != "" {
				t.Fatalf("known rejected commit: state=%+v error=%v failure=%+v calls=%d physical=%v", state, err, failure, calls, commitErr)
			}
			if backend == "postgres" {
				var native *pq.Error
				if !errors.As(err, &native) || native.Code != "23503" {
					t.Fatalf("lost postgres deferred constraint cause: %v", err)
				}
			} else {
				var native *sqlite.Error
				if !errors.As(err, &native) || native.Code() != 787 {
					t.Fatalf("lost sqlite deferred constraint cause: %v", err)
				}
			}
			if after := readStopCommitEvidence(t, db, fixture.runID); after != before {
				t.Fatalf("known rejected commit changed durable state: before=%+v after=%+v", before, after)
			}
			var children int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM stop_commit_child`).Scan(&children); err != nil || children != 0 {
				t.Fatalf("deferred violation survived transaction: children=%d err=%v", children, err)
			}
		})
	}
}

func TestIssue2394StopCommitRaceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, winner := range []string{"stop", "chunk"} {
			t.Run(backend+"/"+winner, func(t *testing.T) {
				owner, db, connector := newP16RaceStore(t, backend)
				fixture, planCtx, bus, plans, serving, group, claim := prepareStopRacePublications(t, owner, db, backend)
				ctx, cancel := context.WithTimeout(planCtx, 10*time.Second)
				defer cancel()
				before := readStopCommitEvidence(t, db, fixture.runID)
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				t.Cleanup(unblock)
				connector.arm(func(tx driver.Tx) error {
					close(entered)
					select {
					case <-release:
						return tx.Commit()
					case <-ctx.Done():
						return errors.Join(ctx.Err(), tx.Rollback())
					}
				})
				stop := func() error {
					_, err := owner.StopRunControl(ctx, runcontrol.TransitionRequest{RunID: fixture.runID})
					return err
				}
				var committed pipeline.CommittedFanOutChunk
				chunk := func() error {
					var err error
					committed, err = serving.CommitFanOutChunk(ctx, pipeline.FanOutChunkCommand{Claim: claim, Now: time.Now().UTC(), Outcomes: []pipeline.FanOutChunkOutcome{{Ordinal: 0, Publication: plans[0]}, {Ordinal: 1, Publication: plans[1]}}})
					return err
				}
				first, second := stop, chunk
				if winner == "chunk" {
					first, second = chunk, stop
				}
				firstDone, secondDone := make(chan error, 1), make(chan error, 1)
				go func() { firstDone <- first() }()
				select {
				case <-entered:
				case err := <-firstDone:
					t.Fatalf("winner did not reach physical COMMIT: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				secondStarted := make(chan struct{})
				if backend == "postgres" {
					connector.mu.Lock()
					connector.begin = secondStarted
					connector.mu.Unlock()
				}
				go func() {
					// SQLite's canonical writer token precedes physical BEGIN.
					// Keep that owner intact rather than constructing a competing
					// store facade to bypass its admission and claim registry.
					if backend == "sqlite" {
						close(secondStarted)
					}
					secondDone <- second()
				}()
				select {
				case <-secondStarted:
				case err := <-secondDone:
					t.Fatalf("contender failed before physical BEGIN: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				select {
				case err := <-secondDone:
					t.Fatalf("contender escaped winner's uncommitted mutation fence: %v", err)
				default:
				}
				unblock()
				if err := <-firstDone; err != nil {
					t.Fatalf("%s winner: %v", winner, err)
				}
				secondErr := <-secondDone
				if winner == "stop" && !errors.Is(secondErr, fanoutobligation.ErrStaleClaim) {
					t.Fatalf("stop-winning stale writer=%v", secondErr)
				}
				if winner == "chunk" {
					failure, typed := runtimefailures.EnvelopeFromError(secondErr)
					if !errors.Is(secondErr, pipelineobligation.ErrBusy) || !typed || failure.Detail.Code != "pipeline_parent_claim_busy" || failure.Detail.Attributes["stage"] != "pipeline_claim" || failure.Detail.Attributes["purpose"] != string(pipelineobligation.PurposePublication) {
						t.Fatalf("stop stole committed publication before mandatory handoff: error=%v failure=%+v", secondErr, failure)
					}
					if snapshot := readStopCommitEvidence(t, db, fixture.runID); snapshot.status != before.status || snapshot.control != before.control || snapshot.canceled != 0 || snapshot.revisions != before.revisions+1 {
						t.Fatalf("busy stop partially mutated chunk-winning run: before=%+v after=%+v", before, snapshot)
					}
					if err := bus.FinalizeFanOutPublications(ctx, group, committed.Publications); err != nil {
						t.Fatal(err)
					}
					if err := bus.DispatchFanOutPublications(context.WithoutCancel(ctx), group, committed.Publications); err != nil {
						t.Fatal(err)
					}
					if snapshot := readStopCommitEvidence(t, db, fixture.runID); snapshot.status != before.status || snapshot.control != before.control || snapshot.canceled != 0 || snapshot.revisions != before.revisions+2 {
						t.Fatalf("handoff requires one chunk and one complete settlement-segment revision, no stop revision: before=%+v after=%+v", before, snapshot)
					}
					if err := stop(); err != nil {
						t.Fatalf("stop after mandatory committed publication handoff: %v", err)
					}
				} else {
					prepared := []runtimeengine.DurablePublicationPlan{plans[0], plans[1]}
					if err := bus.ReleaseEnginePublications(ctx, prepared); err != nil {
						t.Fatal(err)
					}
				}
				wantPrefix := 0
				if winner == "chunk" {
					wantPrefix = 2
				}
				assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, wantPrefix, wantPrefix)
				for ordinal, plan := range plans {
					var count int
					if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id=$1 AND run_id=$2`, plan.DurablePublicationEventID(), fixture.runID).Scan(&count); err != nil || count != wantPrefix/2 {
						t.Fatalf("prefix event ordinal=%d count=%d want=%d err=%v", ordinal, count, wantPrefix/2, err)
					}
					if wantPrefix != 0 {
						var eventID, disposition string
						if err := db.QueryRowContext(ctx, `SELECT event_id,outcome_kind FROM fan_out_outcomes WHERE run_id=$1 AND ordinal=$2`, fixture.runID, ordinal).Scan(&eventID, &disposition); err != nil || eventID != plan.DurablePublicationEventID() || disposition != "committed" {
							t.Fatalf("committed ordinal=%d event=%s status=%s err=%v", ordinal, eventID, disposition, err)
						}
					}
				}
				after := readStopCommitEvidence(t, db, fixture.runID)
				// The approved group contract has one revision per physical segment,
				// not the retired per-member intermediate settlement cuts.
				if after.status != "cancelled" || after.control != "stopped" || after.canceled != 1 || after.pending != 0 || after.events != before.events+wantPrefix || after.revisions != before.revisions+1+wantPrefix {
					t.Fatalf("race accounting: before=%+v after=%+v prefix=%d", before, after, wantPrefix)
				}
				if _, err := serving.LoadFanOutEvaluation(ctx, claim); !errors.Is(err, fanoutobligation.ErrStaleClaim) {
					t.Fatalf("stopped run retained evaluation authority before group close: %v", err)
				}
				if err := group.Close(ctx); err != nil {
					t.Fatal(err)
				}
				if _, err := serving.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, wantPrefix, 1, time.Now().UTC())); !errors.Is(err, fanoutobligation.ErrStaleClaim) {
					t.Fatalf("late old claim retained mutation authority: %v", err)
				}
				if final := readStopCommitEvidence(t, db, fixture.runID); final != after {
					t.Fatalf("stale commit changed state: before=%+v after=%+v", after, final)
				}
			})
		}
	}
}

func prepareStopRacePublications(t *testing.T, selected selectedFanOutLifecycleOwner, db *sql.DB, backend string) (fanOutOwnerFixture, context.Context, *runtimebus.EventBus, []runtimebus.EnginePublicationPlan, pipeline.FanOutObligationOwner, pipelineobligation.PublicationGroup, fanoutobligation.Claim) {
	t.Helper()
	owner := selected.(selectedFanOutMixedRouteOwner)
	artifact, bundle := fanOutMixedRouteSource(t)
	runtimeID := uuid.NewString()
	ctx := runtimeauthoractivity.WithScope(testAuthorActivityContextForBundle(artifact.BundleHash()), runtimeauthoractivity.BundleScope(runtimeID, artifact.BundleHash()))
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, mustStoreTestSourceArtifactFact(artifact.BundleHash()))
	fixture := seedFanOutOwnerFixtureWithArtifact(t, ctx, db, owner, backend == "postgres", 5, time.Now().UTC(), artifact)
	ctx = runtimecorrelation.WithRunID(ctx, fixture.runID)
	scope, ok := runtimeauthoractivity.ScopeFromContext(ctx)
	if !ok {
		t.Fatal("race fixture requires author scope")
	}
	lease, err := owner.RegisterAuthorActivityEventCatalog(scope, []runtimeauthoractivity.EventDescriptor{{EventType: "producer/mixed.none", Disposition: runtimeauthoractivity.StoryAuthored}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lease.Release)
	bus, err := newStoreTestEventBus(t, owner, runtimebus.EventBusOptions{ContractBundle: semanticview.Wrap(bundle), SourceArtifactFact: mustStoreTestSourceArtifactFact(fixture.bundleHash), RuntimeInstanceID: runtimeID})
	if err != nil {
		t.Fatal(err)
	}
	route := events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: uuid.NewString()}.Normalized()
	source, err := events.NewStaticFlowRoutingSource(route)
	if err != nil {
		t.Fatal(err)
	}
	producer, err := identity.AdmitExecutableNodeDeclaration("producer", "fan-out-producer")
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := db.QueryRowContext(ctx, `SELECT capsule FROM fan_out_intents WHERE run_id=$1`, fixture.runID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var capsule fanoutobligation.Capsule
	if err := json.Unmarshal(raw, &capsule); err != nil {
		t.Fatal(err)
	}
	capsule.NodeKey, capsule.ExecutionFlowID, capsule.EntityID = producer.Key(), "producer", route.EntityID
	capsule.Route, capsule.ProducerSource = runtimeflowidentity.StoredRoute("producer", "producer", "producer"), source
	raw, err = json.Marshal(capsule)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET capsule=$1 WHERE run_id=$2`, string(raw), fixture.runID); err != nil {
		t.Fatal(err)
	}
	planCtx := runtimedelivery.WithRoute(ctx, events.DeliveryRoute{Target: events.MustExistingEntityTarget(route)})
	serving, _, _, _ := grantedFanOutOwnerForTest(t, ctx, selected, fixture)
	key := fanoutobligation.IntentKey{RunID: fixture.runID, TriggeringDeliveryID: fixture.deliveryID, ElementRef: contracts.FanOutElementRef{FlowPath: fixture.flowPath, Family: "fan_out", SemanticPath: fixture.semanticPath}}
	intent, claim, found, err := serving.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "stop-race", BundleHash: fixture.bundleHash, Candidate: &key, Now: time.Now().UTC(), Lease: time.Minute})
	if err != nil || !found {
		t.Fatalf("claim=%v err=%v", found, err)
	}
	input, err := serving.LoadFanOutEvaluation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	group, err := serving.BeginFanOutPublicationGroup(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := group.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	plans := make([]runtimebus.EnginePublicationPlan, 0, 2)
	var durablePlans []runtimeengine.DurablePublicationPlan
	for ordinal := 0; ordinal < 2; ordinal++ {
		projection, err := fanoutobligation.PrepareOrdinalEmission(intent, input.Trigger, ordinal)
		if err != nil {
			t.Fatal(err)
		}
		event, err := projection.NewEvent(events.EventFacts{ID: uuid.NewString(), Type: "producer/mixed.none", Producer: events.ProducerClaim{Type: events.EventProducerNode, ID: producer.Key()}, Payload: []byte(`{}`), ChainDepth: capsule.ChainDepth + 1, Envelope: events.EnvelopeForSourceRoute(events.EventEnvelope{}, route), RoutingSource: source, CreatedAt: fixture.createdAt.Add(time.Duration(ordinal+1) * time.Second)})
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := bus.PrepareFanOutPublication(planCtx, group, ordinal, runtimeengine.EmitIntent{Event: event})
		if err != nil {
			t.Fatalf("prepare ordinal %d: %v", ordinal, err)
		}
		plan, ok := prepared.(runtimebus.EnginePublicationPlan)
		if !ok || !plan.PublicationCommand().Commit.RouteSettlement.NoDelivery() {
			t.Fatalf("ordinal %d lacks real no-route publication plan: %T", ordinal, prepared)
		}
		plans = append(plans, plan)
		durablePlans = append(durablePlans, plan)
	}
	if err := bus.SealFanOutPublications(planCtx, group, 2, durablePlans); err != nil {
		t.Fatal(err)
	}
	return fixture, ctx, bus, plans, serving, group, claim
}

type stopCommitEvidence struct {
	status, control                        string
	canceled, pending, outcomes, revisions int
	events                                 int
}

func readStopCommitEvidence(t *testing.T, db *sql.DB, runID string) stopCommitEvidence {
	t.Helper()
	var e stopCommitEvidence
	if err := db.QueryRow(`SELECT r.status,COALESCE(c.control_status,'') FROM runs r LEFT JOIN run_control_state c ON c.run_id=r.run_id WHERE r.run_id=$1`, runID).Scan(&e.status, &e.control); err != nil {
		t.Fatal(err)
	}
	for query, target := range map[string]*int{
		`SELECT COUNT(*) FROM events WHERE run_id=$1`:                                                             &e.events,
		`SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND status='canceled'`:                              &e.canceled,
		`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status IN ('pending','processing','retrying')`: &e.pending,
		`SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`:                                                   &e.outcomes,
		`SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1`:                                                 &e.revisions,
	} {
		if err := db.QueryRow(query, runID).Scan(target); err != nil {
			t.Fatal(err)
		}
	}
	return e
}
