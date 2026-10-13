package runtimepersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

func TestRunForkProposedEffectStartCutExcludesLaterCardsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, offset := range []time.Duration{-time.Microsecond, 0, time.Microsecond} {
			t.Run(backend.name+"/"+offset.String(), func(t *testing.T) {
				opened := backend.open(t)
				f := newReceiverConfigActivationFixtureForStore(t, opened.store.(agentFixtureFlowStore), false,
					map[string]string{"schema.yaml": "name: pending-effect-start-cut\nstages:\n  operating: {}\n"}, nil, ownStoreTestAgentManager, nil)
				target, err := contracts.BootBundleIdentity(f.bundle)
				if err != nil {
					t.Fatal(err)
				}
				ctx, source, child := f.ctx, correlation.RunIDFromContext(f.ctx), uuid.NewString()
				cards := opened.store.(interface {
					decisioncard.Store
					decisioncard.ProposedEffectStore
					LoadRunHeader(context.Context, string) (operatorread.RunHeader, error)
				})
				header, err := cards.LoadRunHeader(ctx, source)
				if err != nil {
					t.Fatal(err)
				}
				requireRunFixtureForTest(t, ctx, f.store, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: child,
					Artifact: f.bundle.SourceArtifact, BundleHash: f.bundle.SourceArtifact.BundleHash()})
				for _, run := range []string{source, child} {
					constructHistoricalSourceFixture(t, correlation.WithRunID(ctx, run), f.store, sqliteFlowActivationRequest(f.bundle, ".", run, "", run))
				}
				card, effect := newProposedEffectTestCard(t, source, header.StartedAt.Add(offset), attemptgeneration.Generation{})
				effect.ReplyContextID = ""
				effect.EntityID, effect.FlowInstance = source, source
				effect.BundleHash, effect.WorkflowVersion = target.BundleHash, target.WorkflowVersion
				effect = effect.Canonical()
				value, err := effect.EffectValue()
				if err != nil {
					t.Fatal(err)
				}
				effect.EffectContentHash, err = canonicaljson.HashValue(value)
				if err != nil {
					t.Fatal(err)
				}
				card.Anchor, err = decisioncard.NewProposedEffectAnchor(decisioncard.ProposedEffectAnchor{
					RequestEventID: effect.RequestEventID, ActivityID: effect.ActivityID, Decision: "support_reply",
					Scope: decisioncard.Scope{Kind: decisioncard.ScopeEntity, FlowInstance: source, EntityID: source}, Source: eventtest.RootRoutingSource(source)})
				if err != nil {
					t.Fatal(err)
				}
				card.BundleHash, card.WorkflowVersion, card.EffectContentHash = effect.BundleHash, effect.WorkflowVersion, effect.EffectContentHash
				card, err = decisioncard.New(card)
				if err != nil {
					t.Fatal(err)
				}
				if err := cards.CreateProposedEffectCard(ctx, card, effect); err != nil {
					t.Fatal(err)
				}
				projection, err := projectRunForkEntityOwnership(source, child, source, source)
				if err != nil {
					t.Fatal(err)
				}
				correspondence, err := loopruntime.NewForkCorrespondence(nil, child, projection.Fork.EntityID)
				if err != nil {
					t.Fatal(err)
				}
				plan := runfork.RunForkPlan{SourceRunID: source, SourceRunStartedAt: &header.StartedAt,
					ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 1}}
				if err := runSelectedFixtureMutation(ctx, opened.store, "project pending effects at creation cut", func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
					if native, ok := opened.store.(*PostgresStore); ok {
						return native.runForkPostgresOwner.MaterializeRunForkProposedEffectCardsTx(ctx, attempt, plan, child, target, projection, correspondence, header.StartedAt.Add(time.Minute), nil)
					}
					return opened.store.(*SQLiteRuntimeStore).runForkSQLiteOwner.MaterializeRunForkProposedEffectCardsTx(ctx, attempt, plan, child, target, projection, correspondence, header.StartedAt.Add(time.Minute), nil)
				}); err != nil {
					t.Fatal(err)
				}
				projected, cursor, err := cards.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: child, Limit: 10})
				want := 1
				if offset > 0 {
					want = 0
				}
				if err != nil || cursor != "" || len(projected) != want {
					t.Fatalf("creation-cut pending cards=%+v, want count%d cursor=%q error=%v", projected, want, cursor, err)
				}
				if want == 1 {
					pending, err := cards.LoadProposedEffectContinuation(ctx, projected[0].CardID)
					if err != nil || projected[0].Status != decisioncard.StatusPending || pending.State != decisioncard.ProposedEffectPending ||
						pending.CardID == card.CardID || pending.RequestEventID == effect.RequestEventID || !pending.Input.Equal(effect.Input) {
						t.Fatalf("creation cut changed fresh held authority or input: %+v error=%v", pending, err)
					}
				}
			})
		}
	}
}
