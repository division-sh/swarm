package pipeline

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func VerifyGateReplacementKeepsEqualAndReplacesChangedBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, change := range []string{"source_only", "context", "route", "choice", "input", "decision", "payload"} {
			t.Run(backend+"/"+change, func(t *testing.T) {
				bundle := gateLifecycleBundle(t)
				fixture, pc, ctx, mutations := nativeLifecycleComponentForTest(t, backend, bundle, "drafting", open)
				fixture.UpgradeCardCadence = decisioncard.CadencePolicy{FirstReminderDelay: 11 * time.Minute, UrgencyDelay: 2 * time.Hour, InputDraftTTL: 5 * time.Minute, ReminderInterval: 3 * time.Hour}
				if _, err := executeNativeLifecycleTransitionForTest(t, fixture, mutations, pc, ctx, "draft.ready"); err != nil {
					t.Fatal(err)
				}
				old := nativeGateCardForTest(t, fixture, ctx)
				owner := testRunScopedWorkflowInstanceFromContext(ctx, old.RunID)
				before, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, owner)
				if err != nil || !found {
					t.Fatalf("read original header: %t %v", found, err)
				}
				if change == "route" {
					equal := gateLifecycleDocuments()
					equal["schema.yaml"] += "\n# retained equal intermediate B\n"
					if err := fixture.UpgradeSource(ctx, semanticview.Wrap(loadWorkflowTempBundle(t, equal))); err != nil {
						t.Fatal(err)
					}
				}
				files := gateLifecycleDocuments()
				files["schema.yaml"] += "\n# candidate issuance\n"
				switch change {
				case "context":
					files["schema.yaml"] = strings.Replace(files["schema.yaml"], "      decision:", "      context: {summary: \"Updated presentation\"}\n      decision:", 1)
				case "route":
					files["schema.yaml"] = strings.Replace(files["schema.yaml"], "advances_to: operating", "advances_to: drafting", 1)
				case "choice":
					files["schema.yaml"] = strings.Replace(files["schema.yaml"], "        approve:", "        reject: {advances_to: drafting}\n        approve:", 1)
				case "input":
					files["schema.yaml"] = strings.Replace(files["schema.yaml"], "        approve:", "        approve:\n          input: {note: {type: text, required: true}}", 1)
				case "decision":
					files["schema.yaml"] = strings.ReplaceAll(files["schema.yaml"], "launch_review", "updated_review")
				case "payload":
					files["events.yaml"] = strings.Replace(files["events.yaml"], "launch.approved:", "launch.approved:\n  note: text", 1)
					files["schema.yaml"] = strings.Replace(files["schema.yaml"], "emit: {event: launch.approved}", "emit: {event: launch.approved, fields: {note: \"updated\"}}", 1)
				}
				candidate := semanticview.Wrap(loadWorkflowTempBundle(t, files))
				commit, err := fixture.PrepareUpgradeSource(ctx, candidate)
				if err != nil {
					t.Fatal(err)
				}
				if result, err := commit.Commit(); err != nil || result.Replayed || result.OperationID != commit.OperationID || result.PreviousRevision != commit.PreviousRevision || result.CurrentRevision != commit.NextRevision {
					t.Fatalf("original upgrade: result=%+v %v", result, err)
				}
				after, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, owner)
				if err != nil || !found {
					t.Fatalf("read upgraded header: %t %v", found, err)
				}
				cards := gateReplacementCardsForTest(t, fixture, ctx)
				current, err := fixture.Cards.GetDecisionCard(ctx, old.CardID)
				if err != nil {
					t.Fatal(err)
				}
				if change == "source_only" || change == "context" {
					if len(cards) != 1 || !reflect.DeepEqual(old, current) || !reflect.DeepEqual(before, after) {
						t.Fatal("equal offer changed issuance or workflow state")
					}
					return
				}
				if len(cards) != 2 || current.Status != decisioncard.StatusSuperseded || current.SupersededReason != decisioncard.SupersededByUpdate {
					t.Fatalf("old issuance was not retired once: %+v", cards)
				}
				var successor decisioncard.Card
				for _, card := range cards {
					if card.CardID != old.CardID {
						successor = card
					}
				}
				if successor.Status != decisioncard.StatusPending || successor.Fields.Len() != 0 || !successor.DeferredUntil.IsZero() || successor.BundleHash == old.BundleHash || !successor.CreatedAt.After(old.CreatedAt) {
					t.Fatalf("successor did not start empty under candidate: %+v", successor)
				}
				if successor.EffectiveCadence != fixture.UpgradeCardCadence.Stamp(successor.CreatedAt) {
					t.Fatalf("candidate lost deployment cadence: %+v", successor.EffectiveCadence)
				}
				if current.BundleHash != old.BundleHash || !current.Snapshot.Context.Equal(old.Snapshot.Context) || current.CardContentHash != old.CardContentHash || !current.CreatedAt.Equal(old.CreatedAt) {
					t.Fatal("supersession rewrote immutable issuance")
				}
				if after.CurrentState != before.CurrentState || after.Life != before.Life || !after.EnteredStageAt.Equal(before.EnteredStageAt) || !reflect.DeepEqual(after.Fields, before.Fields) || !reflect.DeepEqual(after.Bookkeeping, before.Bookkeeping) {
					t.Fatal("replacement changed stage, clock, life or entity data")
				}
				carrier, err := workflowInstanceStateCarrier(after)
				if err != nil {
					t.Fatal(err)
				}
				activations, err := gateruntime.List(carrier.StateBuckets)
				if err != nil {
					t.Fatal(err)
				}
				open := 0
				for _, activation := range activations {
					if activation.Status == gateruntime.StatusOpen {
						open++
						if activation.CardID != successor.CardID {
							t.Fatal("current slot selected a different card")
						}
					}
				}
				if open != 1 {
					t.Fatalf("open activations=%d", open)
				}
				if change == "route" {
					scope, _ := authoractivity.ScopeFromContext(ctx)
					superseded, created := 0, 0
					for _, occurrence := range gateReplacementHistoryForTest(t, fixture, ctx) {
						if occurrence.Kind != authoractivity.KindCardLifecycle || occurrence.RunID != old.RunID {
							continue
						}
						if occurrence.Projection.CardID == old.CardID && occurrence.Transition == decisioncard.ChangeSuperseded {
							superseded++
							if occurrence.Scope != authoractivity.BundleScope(scope.RuntimeInstanceID, old.BundleHash) {
								t.Fatalf("A/equal-B/changed-C lost old attribution: %+v", occurrence)
							}
						}
						if occurrence.Projection.CardID == successor.CardID && occurrence.Transition == decisioncard.ChangeCreated {
							created++
							if occurrence.Scope != authoractivity.BundleScope(scope.RuntimeInstanceID, successor.BundleHash) {
								t.Fatalf("A/equal-B/changed-C lost candidate attribution: %+v", occurrence)
							}
						}
					}
					if superseded != 1 || created != 1 {
						t.Fatalf("replacement attribution cardinality=%d/%d", superseded, created)
					}
				}
				if result, err := commit.Commit(); err != nil || !result.Replayed || result.OperationID != commit.OperationID || !reflect.DeepEqual(cards, gateReplacementCardsForTest(t, fixture, ctx)) {
					t.Fatalf("acknowledged-success exact replay duplicated replacement: result=%+v %v", result, err)
				}
				availability, err := fixture.Store.(runbundle.AvailabilityStore).LoadRunBundleAvailability(ctx, old.RunID)
				if err != nil || availability.BundleHash != successor.BundleHash {
					t.Fatalf("source and replacement diverged: %+v %v", availability, err)
				}
				// Returning to A must not revive its immutable retired issuance.
				if err := fixture.UpgradeSource(ctx, semanticview.Wrap(bundle)); err != nil {
					t.Fatal(err)
				}
				returned := gateReplacementCardsForTest(t, fixture, ctx)
				if len(returned) != 3 {
					t.Fatalf("A-B-A did not issue a distinct third card: %+v", returned)
				}
				for _, card := range returned {
					if card.Status == decisioncard.StatusPending && (card.CardID == old.CardID || card.CardID == successor.CardID || card.BundleHash != old.BundleHash) {
						t.Fatalf("A-B-A revived a previous card: %+v", card)
					}
				}
			})
		}
	}
}

func gateReplacementCardsForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, ctx context.Context) []decisioncard.Card {
	t.Helper()
	var cards []decisioncard.Card
	for cursor := ""; ; {
		items, next, err := fixture.Cards.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: correlation.RunIDFromContext(ctx), Limit: 200, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range items {
			card, err := fixture.Cards.GetDecisionCard(ctx, item.CardID)
			if err != nil {
				t.Fatal(err)
			}
			cards = append(cards, card)
		}
		if next == "" {
			return cards
		}
		cursor = next
	}
}

func VerifyGateReplacementRollbackAcrossRunsBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			files := gateLifecycleDocuments()
			files["schema.yaml"] = strings.Replace(files["schema.yaml"], "        approve:", "        approve:\n          input:\n            first: {type: text, required: true}\n            second: {type: text, required: true}", 1)
			bundle := loadWorkflowTempBundle(t, files)
			fixture, pc, firstCtx, mutations := nativeLifecycleComponentForTest(t, backend, bundle, "drafting", open)
			runs := []string{correlation.RunIDFromContext(firstCtx), uuid.NewString()}
			slices.Sort(runs)
			contexts := map[string]context.Context{}
			old := map[string]decisioncard.Card{}
			for _, run := range runs {
				ctx := correlation.WithRunID(firstCtx, run)
				contexts[run] = ctx
				if run != correlation.RunIDFromContext(firstCtx) {
					if err := fixture.RequireRun(ctx, run); err != nil {
						t.Fatal(err)
					}
					if err := fixture.Construct(ctx, materializedWorkflowInstanceForSource(t, semanticview.Wrap(bundle), ctx, WorkflowInstance{InstanceID: run, StorageRef: run, EntityID: run, WorkflowName: ".", WorkflowVersion: bundle.WorkflowVersion(), CurrentState: "drafting", EnteredStageAt: time.Now().UTC(), Fields: map[string]any{}})); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := executeNativeLifecycleTransitionForTest(t, fixture, mutations, pc, ctx, "draft.ready"); err != nil {
					t.Fatal(err)
				}
				old[run] = nativeGateCardForTest(t, fixture, ctx)
			}
			first, last := runs[0], runs[1]
			at := time.Now().UTC()
			active, err := fixture.InputDrafts.BeginInputForTest(contexts[first], decisioncard.BeginInputRequest{CardID: old[first].CardID, Verdict: "approve", PrincipalID: "operator", Now: at, TTL: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			prefix, err := fixture.InputDrafts.AdvanceInputTextForTest(contexts[first], active.InputDraftID, "operator", "retained first", at)
			if err != nil || prefix.NextFieldIndex != 1 {
				t.Fatalf("admit genuine partial draft: %+v %v", prefix, err)
			}
			expired, err := fixture.InputDrafts.BeginInputForTest(contexts[last], decisioncard.BeginInputRequest{CardID: old[last].CardID, Verdict: "approve", PrincipalID: "operator", Now: at, TTL: time.Nanosecond})
			if err != nil || expired.Status != "active" {
				t.Fatalf("admit stored-active time-expired draft: %+v %v", expired, err)
			}
			preview, err := fixture.InputDrafts.PreviewInputTextForTest(contexts[first], active.InputDraftID, "operator", "next", at)
			if err != nil {
				t.Fatal(err)
			}
			files["schema.yaml"] = strings.Replace(files["schema.yaml"], "advances_to: operating", "advances_to: drafting", 1)
			candidate := semanticview.Wrap(loadWorkflowTempBundle(t, files))
			commit, err := fixture.PrepareUpgradeSource(firstCtx, candidate)
			if err != nil {
				t.Fatal(err)
			}
			before := map[string]WorkflowInstance{}
			for _, run := range runs {
				header, found, err := fixture.Persistence.LoadWorkflowInstance(contexts[run], testRunScopedWorkflowInstanceFromContext(contexts[run], run))
				if err != nil || !found {
					t.Fatalf("retain every original header: %t %v", found, err)
				}
				before[run] = header
			}
			changes := gateReplacementChangesForTest(t, fixture, firstCtx)
			history := gateReplacementHistoryForTest(t, fixture, firstCtx)
			revision, err := fixture.SourceSetRevision(firstCtx)
			if err != nil {
				t.Fatal(err)
			}
			if err := fixture.SetCardInsertFault(firstCtx, last, true); err != nil {
				t.Fatal(err)
			}
			faultInstalled := true
			t.Cleanup(func() {
				if !faultInstalled {
					return
				}
				if err := fixture.SetCardInsertFault(firstCtx, last, false); err != nil {
					t.Error(err)
				}
			})
			if _, err := commit.Commit(); err == nil || !strings.Contains(err.Error(), "workflow_gate_card_insert_cut") {
				t.Fatalf("second-run card insertion did not cause confirmed rollback: %v", err)
			}
			for _, run := range runs {
				ctx := contexts[run]
				cards := gateReplacementCardsForTest(t, fixture, ctx)
				header, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, run))
				if err != nil || !found || !reflect.DeepEqual(header, before[run]) || len(cards) != 1 || !reflect.DeepEqual(cards[0], old[run]) {
					t.Fatalf("rollback leaked card/slot/header in run %s: %+v %v", run, cards, err)
				}
				available, err := fixture.Store.(runbundle.AvailabilityStore).LoadRunBundleAvailability(ctx, run)
				if err != nil || available.BundleHash != old[run].BundleHash {
					t.Fatalf("rollback leaked run source: %+v %v", available, err)
				}
			}
			if !reflect.DeepEqual(changes, gateReplacementChangesForTest(t, fixture, firstCtx)) || fixture.Transactions().Active != 0 {
				t.Fatal("failed transaction left card/draft history or an owned transaction")
			}
			retainedRevision, err := fixture.SourceSetRevision(firstCtx)
			if err != nil || retainedRevision != revision || !reflect.DeepEqual(history, gateReplacementHistoryForTest(t, fixture, firstCtx)) {
				t.Fatalf("rollback changed source-set head or author history: %q %v", retainedRevision, err)
			}
			// Behavioral draft probes follow ALL rollback readbacks, never replace them.
			after, err := fixture.InputDrafts.PreviewInputTextForTest(contexts[first], active.InputDraftID, "operator", "next", at)
			if err != nil || !reflect.DeepEqual(preview, after) {
				t.Fatalf("rollback lost active partial draft: %+v %v", after, err)
			}
			continued, err := fixture.InputDrafts.AdvanceInputTextForTest(contexts[first], active.InputDraftID, "operator", "retained second", at)
			if err != nil || !continued.Complete || continued.Fields.Len() != 2 {
				t.Fatalf("original draft cannot continue: %+v %v", continued, err)
			}
			if count, err := pc.decisionDraftExpiry.ExpireDecisionCardInputDrafts(firstCtx, time.Now().UTC()); err != nil || count != 1 {
				t.Fatalf("time-expired stored-active draft was not retained: count=%d %v", count, err)
			}
			expiredChange := false
			for _, change := range gateReplacementChangesForTest(t, fixture, firstCtx) {
				id, _ := change.Payload.Lookup("input_draft_id")
				if change.CardID == old[last].CardID && change.ChangeType == decisioncard.ChangeDraftExpired && id.Interface() == expired.InputDraftID {
					expiredChange = true
				}
			}
			if !expiredChange {
				t.Fatal("expiry did not name the exact retained draft")
			}
			if err := fixture.SetCardInsertFault(firstCtx, last, false); err != nil {
				t.Fatal(err)
			}
			faultInstalled = false
			// EXACT frozen request is the final state-changing proof action.
			if result, err := commit.Commit(); err != nil || result.Replayed || result.OperationID != commit.OperationID || result.PreviousRevision != revision || result.CurrentRevision != commit.NextRevision {
				t.Fatalf("rollback left an operation completion or wrong revision: result=%+v %v", result, err)
			}
			finalRevision, err := fixture.SourceSetRevision(firstCtx)
			if err != nil || finalRevision != commit.NextRevision {
				t.Fatalf("final source-set head did not switch: %q %v", finalRevision, err)
			}
			for _, run := range runs {
				cards := gateReplacementCardsForTest(t, fixture, contexts[run])
				if len(cards) != 2 {
					t.Fatalf("final exact retry did not replace every run: %s %+v", run, cards)
				}
				created := 0
				for _, change := range gateReplacementChangesForTest(t, fixture, firstCtx) {
					if change.RunID == run && change.CardID != old[run].CardID && change.ChangeType == decisioncard.ChangeCreated {
						created++
					}
				}
				if created != 1 {
					t.Fatalf("replacement logical creation count=%d in %s", created, run)
				}
				available, err := fixture.Store.(runbundle.AvailabilityStore).LoadRunBundleAvailability(contexts[run], run)
				if err != nil || available.BundleHash != candidateBundleHashForTest(t, candidate) {
					t.Fatalf("final retry lost candidate source in %s: %+v %v", run, available, err)
				}
				gateReplacementCurrentForTest(t, fixture, contexts[run], old[run], cards, available.BundleHash)
			}
		})
	}
}

func candidateBundleHashForTest(t *testing.T, source semanticview.Source) string {
	t.Helper()
	bundle, found := semanticview.Bundle(source)
	if !found || bundle == nil {
		t.Fatal("replacement candidate lacks compiled source")
	}
	return bundle.SourceArtifact.BundleHash()
}

func gateReplacementCurrentForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, ctx context.Context, old decisioncard.Card, cards []decisioncard.Card, hash string) {
	t.Helper()
	anchor := mustStageGateAnchor(t, old)
	header, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, flowidentity.RunScopedFlowInstance{RunID: old.RunID, Route: anchor.Route})
	if err != nil || !found {
		t.Fatalf("final current header: %t %v", found, err)
	}
	carrier, err := workflowInstanceStateCarrier(header)
	if err != nil {
		t.Fatal(err)
	}
	activations, err := gateruntime.List(carrier.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	pending, retired, open := 0, 0, 0
	for _, card := range cards {
		if card.CardID == old.CardID && card.Status == decisioncard.StatusSuperseded && card.SupersededReason == decisioncard.SupersededByUpdate && card.BundleHash == old.BundleHash {
			retired++
		}
		if card.CardID != old.CardID && card.Status == decisioncard.StatusPending && card.BundleHash == hash && card.Fields.Len() == 0 {
			pending++
			for _, activation := range activations {
				if activation.Status == gateruntime.StatusOpen && activation.CardID == card.CardID && activation.BundleHash == hash {
					open++
				}
			}
		}
	}
	if pending != 1 || retired != 1 || open != 1 {
		t.Fatalf("final card/source/slot cut: pending=%d retired=%d open=%d", pending, retired, open)
	}
}

func VerifyGateReplacementPageBoundaryAndAtomicRollbackBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			files := gateLifecycleDocuments()
			files["reviews/manifest.yaml"] = "version: 1.0.0\nplatform_version: '*'\n"
			files["reviews/schema.yaml"] = "instance: ticket_id\n" + files["schema.yaml"]
			files["reviews/state.yaml"] = "ticket_id: text\n"
			files["reviews/events.yaml"] = files["events.yaml"]
			bundle := loadWorkflowTempBundle(t, files)
			fixture, pc, ctx, mutations := nativeLifecycleComponentForTest(t, backend, bundle, "drafting", open)
			if _, err := executeNativeLifecycleTransitionForTest(t, fixture, mutations, pc, ctx, "draft.ready"); err != nil {
				t.Fatal(err)
			}
			source := semanticview.Wrap(bundle)
			state, _ := entityruntime.ResolveForFlow(source, "reviews")
			for i := 0; i < 200; i++ {
				key := fmt.Sprintf("ticket-%03d", i)
				path := "reviews/" + key
				instance := materializedWorkflowInstanceForSource(t, source, ctx, WorkflowInstance{InstanceID: key, StorageRef: path, EntityID: FlowInstanceEntityID(path), WorkflowName: "reviews", WorkflowVersion: bundle.WorkflowVersion(), EntityType: state.EntityType, CurrentState: "awaiting_review", EnteredStageAt: time.Now().UTC(), Fields: map[string]any{"ticket_id": key}})
				if err := fixture.Construct(ctx, instance); err != nil {
					t.Fatal(err)
				}
				if err := applyTestInitialEntryEffect(ctx, pc, testRunScopedWorkflowInstanceFromContext(ctx, path).Route, instance.EntityID); err != nil {
					t.Fatal(err)
				}
			}
			before := gateReplacementCardsForTest(t, fixture, ctx)
			if len(before) != 201 {
				t.Fatalf("page-boundary setup has %d cards, want201", len(before))
			}
			headers := map[string]WorkflowInstance{}
			for _, card := range before {
				anchor := mustStageGateAnchor(t, card)
				header, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, flowidentity.RunScopedFlowInstance{RunID: card.RunID, Route: anchor.Route})
				if err != nil || !found {
					t.Fatalf("original page-spanning header: %t %v", found, err)
				}
				headers[card.CardID] = header
			}
			for _, file := range []string{"schema.yaml", "reviews/schema.yaml"} {
				files[file] = strings.Replace(files[file], "advances_to: operating", "advances_to: drafting", 1)
			}
			candidate := semanticview.Wrap(loadWorkflowTempBundle(t, files))
			fixture.OmitUpgradeArtifact = true
			attempt, err := fixture.PrepareUpgradeSource(ctx, candidate)
			if err != nil {
				t.Fatal(err)
			}
			changes := gateReplacementChangesForTest(t, fixture, ctx)
			history := gateReplacementHistoryForTest(t, fixture, ctx)
			var absent *runlifecycle.SourceArtifactUnavailableError
			if _, err := attempt.Commit(); !errors.As(err, &absent) || absent.BundleHash != candidateBundleHashForTest(t, candidate) {
				t.Fatalf("multi-card attempt did not reach candidate-artifact rollback: %v", err)
			}
			if !reflect.DeepEqual(before, gateReplacementCardsForTest(t, fixture, ctx)) || !reflect.DeepEqual(changes, gateReplacementChangesForTest(t, fixture, ctx)) || !reflect.DeepEqual(history, gateReplacementHistoryForTest(t, fixture, ctx)) {
				t.Fatal("multi-card rollback leaked a card, draft change or author story")
			}
			for _, card := range before {
				anchor := mustStageGateAnchor(t, card)
				header, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, flowidentity.RunScopedFlowInstance{RunID: card.RunID, Route: anchor.Route})
				if err != nil || !found || !reflect.DeepEqual(header, headers[card.CardID]) {
					t.Fatalf("multi-card rollback changed slot/life/fields: %s %v", card.CardID, err)
				}
			}
			if revision, err := fixture.SourceSetRevision(ctx); err != nil || revision != attempt.PreviousRevision {
				t.Fatalf("page-spanning rollback changed source-set revision: %s %v", revision, err)
			}
			// Remove only the missing-artifact fault; do not execute the new request.
			fixture.OmitUpgradeArtifact = false
			if _, err := fixture.PrepareUpgradeSource(ctx, candidate); err != nil {
				t.Fatal(err)
			}
			if result, err := attempt.Commit(); err != nil || result.Replayed || result.OperationID != attempt.OperationID || result.PreviousRevision != attempt.PreviousRevision || result.CurrentRevision != attempt.NextRevision {
				t.Fatalf("original frozen page-spanning request: %+v %v", result, err)
			}
			after := gateReplacementCardsForTest(t, fixture, ctx)
			if len(after) != 402 {
				t.Fatalf("source switch re-enumerated own successors: cards=%d", len(after))
			}
			for _, old := range before {
				anchor := mustStageGateAnchor(t, old)
				var pair []decisioncard.Card
				for _, card := range after {
					if mustStageGateAnchor(t, card).EntityID == anchor.EntityID {
						pair = append(pair, card)
					}
				}
				if len(pair) != 2 {
					t.Fatalf("page-spanning occurrence lost singular pair: %s %+v", old.CardID, pair)
				}
				gateReplacementCurrentForTest(t, fixture, ctx, old, pair, candidateBundleHashForTest(t, candidate))
			}
		})
	}
}

func VerifyGateReplacementRetainedMismatchBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, changed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/candidate_changed_%t", backend, changed), func(t *testing.T) {
				bundle := gateLifecycleBundle(t)
				fixture, pc, ctx, mutations := nativeLifecycleComponentForTest(t, backend, bundle, "drafting", open)
				if _, err := executeNativeLifecycleTransitionForTest(t, fixture, mutations, pc, ctx, "draft.ready"); err != nil {
					t.Fatal(err)
				}
				old := nativeGateCardForTest(t, fixture, ctx)
				files := gateLifecycleDocuments()
				files["schema.yaml"] = strings.Replace(files["schema.yaml"], "advances_to: operating", "advances_to: drafting", 1)
				currentBundle := loadWorkflowTempBundle(t, files)
				if _, err := fixture.PrepareUpgradeSource(ctx, semanticview.Wrap(currentBundle)); err != nil {
					t.Fatal(err)
				}
				root, err := currentBundle.RootedSource()
				if err != nil {
					t.Fatal(err)
				}
				fact, err := correlation.NewSourceArtifactFact(currentBundle.SourceArtifact.BundleHash())
				if err != nil {
					t.Fatal(err)
				}
				// Existing lifecycle owner composes the first vehicle's retained
				// mismatch; no execution under B or historical migration is claimed.
				if _, err := pc.workflowStore.runLifecycle.ReviseRunSource(ctx, runlifecycle.SourceRevisionRequest{RunID: old.RunID, Source: fact, RootAdmission: root.Admission()}); err != nil {
					t.Fatal(err)
				}
				candidate := semanticview.Wrap(bundle)
				if changed {
					files["schema.yaml"] = strings.ReplaceAll(files["schema.yaml"], "launch_review", "third_review")
					candidate = semanticview.Wrap(loadWorkflowTempBundle(t, files))
				}
				if err := fixture.UpgradeSource(ctx, candidate); err != nil {
					t.Fatalf("valid retained offer acquired a new historical-equality refusal: %v", err)
				}
				cards := gateReplacementCardsForTest(t, fixture, ctx)
				if !changed {
					if len(cards) != 1 || !reflect.DeepEqual(cards[0], old) {
						t.Fatal("candidate equal to issued offer did not preserve it")
					}
				} else {
					gateReplacementCurrentForTest(t, fixture, ctx, old, cards, candidateBundleHashForTest(t, candidate))
				}
			})
		}
	}
}

func gateReplacementHistoryForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, ctx context.Context) []authoractivity.Occurrence {
	t.Helper()
	var history []authoractivity.Occurrence
	var after int64
	for {
		page, err := fixture.Store.(authoractivity.Reader).ListAuthorActivity(ctx, authoractivity.ListOptions{AfterSequence: after, Limit: 200})
		if err != nil {
			t.Fatal(err)
		}
		history = append(history, page.Occurrences...)
		if page.NextCursor == 0 {
			return history
		}
		after = page.NextCursor
	}
}

func gateReplacementChangesForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, ctx context.Context) []decisioncard.Change {
	t.Helper()
	var changes []decisioncard.Change
	var after int64
	for {
		page, err := fixture.Cards.ListDecisionCardChanges(ctx, decisioncard.SubscriptionOptions{After: after, Limit: 200})
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			return changes
		}
		changes = append(changes, page...)
		after = page[len(page)-1].Sequence
	}
}

func TestGateReplacementIdentityAndReason(t *testing.T) {
	bundle := gateLifecycleBundle(t)
	offer, err := decisioncard.CompileGateOffer(semanticview.Wrap(bundle), ".", "awaiting_review")
	if err != nil {
		t.Fatal(err)
	}
	routes, err := offer.RoutesJSON()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	activation, err := gateruntime.New("run", "run", "entity", ".", "awaiting_review", "launch_review", bundle.SourceArtifact.BundleHash(), routes, "state:awaiting_review", now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	first, err := gateruntime.Replacement(activation, activation.DecisionID, "candidate", activation.RoutesJSON, now)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := gateruntime.Replacement(activation, activation.DecisionID, "candidate", activation.RoutesJSON, now.Add(time.Hour))
	if err != nil || retry.CardID != first.CardID || retry.ActivationID != first.ActivationID || first.CardID == activation.CardID || first.StartedByEvent != activation.StartedByEvent {
		t.Fatalf("replacement identity is not input-derived: %+v %+v %v", first, retry, err)
	}
	if got := decisioncard.SupersededMessage(decisioncard.SupersededByUpdate); got != "Replaced by a new card after an update. No action was taken." {
		t.Fatal(got)
	}
	if decisioncard.SupersededMessage("ordinary_exit") != "The flow moved on. No action was taken." {
		t.Fatal("ordinary supersession wording changed")
	}
}
