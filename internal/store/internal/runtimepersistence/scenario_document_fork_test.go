package runtimepersistence

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/scenarioderivation"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/store/internal/backend/scenarioexecutionpersistence"
	"github.com/google/uuid"
)

func TestAuthoredScenarioProfileForkRetainsExactMaterializedDataBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			at := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
			ctx, fixture, _ := seedDeclaredForkFanOutFixtureWithBarrier(t, backend, authorActivityReceiptFixture{db: db, store: owner.(authorActivityReceiptStore)}, 0, at, true)
			sourceFact, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
			if !ok {
				t.Fatal("missing source fact")
			}
			identity, err := scenarioexecution.NewEffectiveSourceIdentity(sourceFact, "sha256:"+strings.Repeat("a", 64))
			if err != nil {
				t.Fatal(err)
			}
			declaration, found, err := scenarioderivation.ParseDeclaration([]byte(`
name: materialized-profile
derive: {flow: '.', input: request, payload: {generate: true}}
connector_responses: {tool: "${{'content': '${1 + 1}', 'items': [null, 7.0]}}"}
`), "tests/profile.yaml")
			if err != nil || !found {
				t.Fatalf("admission: %v %v", found, err)
			}
			profile, err := scenarioexecution.NewProfile(identity, declaration.Name, []scenarioexecution.ConnectorResponse{{ToolID: "tool", OutputSchemaDigest: "sha256:" + strings.Repeat("b", 64), Response: declaration.ConnectorResponses["tool"]}})
			if err != nil {
				t.Fatal(err)
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if postgres {
				err = scenarioexecutionpersistence.EnsurePostgres(ctx, tx, fixture.runID, profile, at)
			} else {
				err = scenarioexecutionpersistence.EnsureSQLite(ctx, tx, fixture.runID, profile, at)
			}
			if err != nil {
				_ = tx.Rollback()
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			pointID := uuid.NewString()
			point := eventtest.ExistingRunRootIngressWithRoutingSource(pointID, events.EventType("fork.barrier.armed"), "operator", "", []byte(`{}`), 0, fixture.runID, events.EventEnvelope{Scope: events.EventScopeGlobal}, eventtest.RootRoutingSource(fixture.runID), at.Add(time.Second))
			captureFanOutBarrierForkRevision(t, ctx, db, fixture.runID, postgres)
			if err := insertCanonicalEventRecordFixture(ctx, owner, point); err != nil {
				t.Fatal(err)
			}
			captureFanOutBarrierForkRevision(t, ctx, db, fixture.runID, postgres)
			forkOwner := owner.(interface {
				MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
				LoadScenarioExecutionProfile(context.Context, string) (scenarioexecution.Profile, bool, error)
			})
			request := runfork.RunForkMaterializeRequest{SourceRunID: fixture.runID, At: pointID, EffectiveSourceIdentity: identity, OriginalLoopCarriage: originalCarriageForRun(t, owner, fixture.runID)}
			different, _ := scenarioexecution.NewEffectiveSourceIdentity(sourceFact, "sha256:"+strings.Repeat("c", 64))
			bad := request
			bad.EffectiveSourceIdentity = different
			before := snapshotForkHistoricalExecutionTables(t, db, postgres)
			if _, err := forkOwner.MaterializeRunFork(ctx, bad); err == nil || !strings.Contains(err.Error(), "effective source mismatch") {
				t.Fatalf("foreign identity: %v", err)
			}
			if after := snapshotForkHistoricalExecutionTables(t, db, postgres); !reflect.DeepEqual(before, after) {
				t.Fatal("rejected fork mutated state")
			}
			fork, err := forkOwner.MaterializeRunFork(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			inherited, found, err := forkOwner.LoadScenarioExecutionProfile(ctx, fork.ForkRunID)
			if err != nil || !found {
				t.Fatalf("readback: %v %v", found, err)
			}
			if inherited.Digest() != profile.Digest() || !bytes.Equal(inherited.CanonicalBytes(), profile.CanonicalBytes()) {
				t.Fatal("fork changed admitted profile bytes")
			}
			if !bytes.Contains(inherited.CanonicalBytes(), []byte("${1 + 1}")) {
				t.Fatal("materialized witness rescanned")
			}
			if _, err := forkOwner.MaterializeRunFork(ctx, request); err != nil {
				t.Fatalf("replay: %v", err)
			}
		})
	}
}
