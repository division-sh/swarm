package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
)

func TestA9ConstructionSelectionRequiresExactIndexWithoutPlanner(t *testing.T) {
	source := nestedConnectionConstructionSource(t)
	root := flowidentity.Stored(source, ".", busInternalTestRunID, busInternalTestRunID, busInternalTestRunID, "")
	parent, err := flowidentity.KeyedChild(source, root, "parent", "stored-parent")
	if err != nil {
		t.Fatal(err)
	}
	parent.EntityID = eventtest.UUID("non-derived-parent")
	rootObservation := constructionIndexObservation(t, source, busInternalTestRunID, root, "")
	parentObservation := constructionIndexObservation(t, source, busInternalTestRunID, parent, "left")
	independent := errors.New("independent index failure")
	for _, test := range []struct {
		name    string
		reader  pipeline.FlowInstanceIndexReader
		wantErr error
		valid   bool
	}{
		{name: "genuine receipt without planner", reader: constructionIndexTestReader{observations: []pipeline.FlowInstanceObservation{rootObservation, parentObservation}}, valid: true},
		{name: "missing reader with compiled source"},
		{name: "absent receipt", reader: constructionIndexTestReader{observations: []pipeline.FlowInstanceObservation{rootObservation}}},
		{name: "foreign run", reader: constructionIndexTestReader{observations: []pipeline.FlowInstanceObservation{rootObservation, parentObservation}}},
		{name: "wrong same-key entity", reader: constructionIndexTestReader{observations: []pipeline.FlowInstanceObservation{rootObservation, parentObservation}}},
		{name: "wrong route", reader: constructionIndexTestReader{observations: []pipeline.FlowInstanceObservation{rootObservation, parentObservation}}},
		{name: "declaration as instance", reader: constructionIndexTestReader{observations: []pipeline.FlowInstanceObservation{rootObservation, parentObservation}}},
		{name: "contradictory parent receipt", reader: constructionIndexTestReader{err: &pipeline.FlowInstanceConstructionCorruption{Cause: errors.New("contradictory parent")}}},
		{name: "independent failure", reader: constructionIndexTestReader{err: independent}, wantErr: independent},
		{name: "cancellation", reader: constructionIndexTestReader{err: context.Canceled}, wantErr: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner := connectInstanceSelector{source: source, index: test.reader}
			instances, err := owner.constructionOwners(constructionIndexContext(t, source), busInternalTestRunID)
			if err != nil || len(instances) != 0 {
				t.Fatalf("compiled source became committed construction evidence: %+v %v", instances, err)
			}
			run, entity := busInternalTestRunID, parent.EntityID
			if test.name == "foreign run" {
				run = eventtest.UUID("foreign-run")
			}
			if test.name == "wrong same-key entity" {
				entity = eventtest.UUID("wrong-entity")
			}
			path := parent.InstancePath
			if test.name == "wrong route" {
				path = "other/stored-parent"
			}
			if test.name == "declaration as instance" {
				path = parent.TemplateID
			}
			actual, err := owner.constructionInstance(constructionIndexContext(t, source), run, parent.TemplateID, path, entity, instances)
			if test.valid {
				if err != nil || actual != parent {
					t.Fatalf("reader-only selection lost stored identity: %+v %v", actual, err)
				}
			} else if err == nil || actual != (flowidentity.Instance{}) || test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatalf("receipt refusal lost its boundary/error: %+v %v", actual, err)
			}
		})
	}
}

func TestA9MissingCreationPlannerCannotBorrowReceiptAuthority(t *testing.T) {
	source := connectRoutePlanCarriedKeyResolutionSource(t, contracts.FlowInputResolutionModeSelectOrCreate)
	plan := mustInstanceKeyConnectRoutePlan(t, source)
	root := flowidentity.Stored(source, ".", busInternalTestRunID, busInternalTestRunID, busInternalTestRunID, "")
	producer, err := flowidentity.KeylessChild(source, root, "producer")
	if err != nil {
		t.Fatal(err)
	}
	owner := connectInstanceSelector{source: source, index: constructionIndexTestReader{observations: []pipeline.FlowInstanceObservation{constructionIndexObservation(t, source, busInternalTestRunID, root, ""), constructionIndexObservation(t, source, busInternalTestRunID, producer, "")}}}
	event := eventtest.ExistingRunRootIngress(eventtest.UUID("no-creation-planner"), "producer/account.ready", "test", "", []byte(`{"account_id":"acct-1"}`), 0, busInternalTestRunID, events.EventEnvelope{}, time.Now().UTC())
	materialized, decision, handled, err := owner.Materialize(withConnectRoutePlanPreview(constructionIndexContext(t, source)), event, plan, map[string]string{"payload.account_id": "acct-1"})
	if err == nil || !handled || !materialized.Failure.Empty() || decision.Activation != nil || decision.Identity() != (flowidentity.Instance{}) {
		t.Fatalf("receipt authority authorized new construction: %+v %+v handled=%t err=%v", materialized, decision, handled, err)
	}
}

func TestParentLocalConstructionSelectionUsesRecordedAncestry(t *testing.T) {
	for _, authoredPin := range []bool{false, true} {
		t.Run(fmt.Sprintf("authored_input_%t", authoredPin), func(t *testing.T) {
			files := map[string]string{
				"manifest.yaml":              "version: 1.0.0\nplatform_version: '*'\n",
				"schema.yaml":                "stages: []\n",
				"parent/manifest.yaml":       "version: 1.0.0\nplatform_version: '*'\n",
				"parent/schema.yaml":         "stages: []\nconnect:\n  - event: work.done\n    from: child\n    to: .\n",
				"parent/nodes.yaml":          "collector:\n  work.done:\n    guard: _entity.id != \"\"\n",
				"parent/child/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
				"parent/child/schema.yaml":   "stages: []\noutputs: [work.done]\n",
				"parent/child/events.yaml":   "work.done:\n",
			}
			if authoredPin {
				files["parent/schema.yaml"] += "inputs: [work.done]\n"
			}
			source := semanticview.Wrap(loadTargetRouteTempBundle(t, files))
			graph := pinrouting.CompileConnectGraph(source)
			if issues := graph.Issues(); len(issues) != 0 || len(graph.Plans()) != 1 {
				t.Fatalf("parent-local graph plans=%+v issues=%+v", graph.Plans(), issues)
			}
			root := ConstructedFlowInstanceIdentityFixture(source, ".", "", busInternalTestRunID)
			parent := ConstructedFlowInstanceIdentityFixture(source, "parent", "", busInternalTestRunID)
			child := ConstructedFlowInstanceIdentityFixture(source, "parent/child", "", busInternalTestRunID)
			observations := []pipeline.FlowInstanceObservation{
				constructionIndexObservation(t, source, busInternalTestRunID, root, ""),
				constructionIndexObservation(t, source, busInternalTestRunID, parent, ""),
				constructionIndexObservation(t, source, busInternalTestRunID, child, ""),
			}
			independent := errors.New("parent index unavailable")
			joined := errors.Join(context.Canceled, independent)
			for _, test := range []struct {
				name   string
				index  constructionIndexTestReader
				runID  string
				entity string
				want   error
				valid  bool
			}{
				{name: "exact recorded parent", index: constructionIndexTestReader{observations: observations}, valid: true},
				{name: "missing structural parent", index: constructionIndexTestReader{observations: observations[1:]}},
				{name: "missing lexical owner", index: constructionIndexTestReader{observations: []pipeline.FlowInstanceObservation{observations[0], observations[2]}}},
				{name: "foreign run", index: constructionIndexTestReader{observations: observations}, runID: eventtest.UUID("foreign-local-run")},
				{name: "foreign source entity", index: constructionIndexTestReader{observations: observations}, entity: eventtest.UUID("foreign-local-entity")},
				{name: "independent failure", index: constructionIndexTestReader{err: independent}, want: independent},
				{name: "cancellation", index: constructionIndexTestReader{err: context.Canceled}, want: context.Canceled},
				{name: "joined cancellation and independent failure", index: constructionIndexTestReader{err: joined}, want: joined},
			} {
				t.Run(test.name, func(t *testing.T) {
					runID, entity := busInternalTestRunID, child.EntityID
					if test.runID != "" {
						runID = test.runID
					}
					if test.entity != "" {
						entity = test.entity
					}
					sender := eventtest.StaticFlowRoutingSource(child.TemplateID, child.InstancePath, entity)
					event := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID("parent-local-event"), "parent/child/work.done", "test", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, sender, time.Now().UTC())
					owner := connectInstanceSelector{source: source, index: test.index}
					actual, err := owner.constructionParent(constructionIndexContext(t, source), event, graph.Plans()[0])
					if test.valid {
						if err != nil || actual != root {
							t.Fatalf("parent-local structural parent=%+v err=%v, want %+v", actual, err, root)
						}
					} else if err == nil || actual != (flowidentity.Instance{}) || test.want != nil && !errors.Is(err, test.want) {
						t.Fatalf("parent-local refusal lost exact index authority: parent=%+v err=%v", actual, err)
					}
				})
			}
		})
	}
}

func nestedConnectionConstructionSource(t *testing.T) semanticview.Source {
	t.Helper()
	return loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyNestedKeyedConnectionSelection(t))
}

func TestA9NestedConnectionSelectionUsesEveryEdgeKeyAndConstructedParent(t *testing.T) {
	source := nestedConnectionConstructionSource(t)
	graph := pinrouting.CompileConnectGraph(source)
	if issues := graph.Issues(); len(issues) != 0 {
		t.Fatal(issues)
	}
	var parentPlan, leafPlan pinrouting.ConnectRoutePlan
	for _, plan := range graph.Plans() {
		switch plan.ReceiverEndpoint().Readback().FlowID {
		case "parent":
			parentPlan = plan
		case "parent/middle/leaf":
			leafPlan = plan
		}
	}
	root := flowidentity.Stored(source, ".", busInternalTestRunID, busInternalTestRunID, busInternalTestRunID, "")
	index := constructionIndexTestReader{observations: []pipeline.FlowInstanceObservation{constructionIndexObservation(t, source, busInternalTestRunID, root, "")}}
	parents := map[string]flowidentity.Instance{}
	leaves := map[string]flowidentity.Instance{}
	for _, parentKey := range []string{"left", "right"} {
		values := map[string]string{"payload.parent_key": parentKey, "payload.leaf_key": "same"}
		parentMaterial, failure := pinrouting.InstanceKeyMaterialForConnectRoutePlan(parentPlan, pinrouting.AdmitConnectRouteMatchValues(values))
		if !failure.Empty() {
			t.Fatal(failure)
		}
		parent, err := flowidentity.KeyedChild(source, root, "parent", connectMissingInstanceID(parentPlan.ReceiverKeyDigest(parentMaterial.Keys)))
		if err != nil {
			t.Fatal(err)
		}
		middle, err := flowidentity.KeylessChild(source, parent, "parent/middle")
		if err != nil {
			t.Fatal(err)
		}
		leafMaterial, failure := pinrouting.InstanceKeyMaterialForConnectRoutePlan(leafPlan, pinrouting.AdmitConnectRouteMatchValues(values))
		if !failure.Empty() {
			t.Fatal(failure)
		}
		leaf, err := flowidentity.KeyedChild(source, middle, "parent/middle/leaf", connectMissingInstanceID(leafPlan.ReceiverKeyDigest(leafMaterial.Keys)))
		if err != nil {
			t.Fatal(err)
		}
		for _, instance := range []flowidentity.Instance{parent, middle, leaf} {
			key := ""
			if instance.TemplateID == "parent" {
				key = parentKey
			}
			if instance.TemplateID == "parent/middle/leaf" {
				key = "same"
			}
			index.observations = append(index.observations, constructionIndexObservation(t, source, busInternalTestRunID, instance, key))
		}
		parents[parentKey], leaves[parentKey] = parent, leaf
	}
	owner := connectInstanceSelector{source: source, index: index}
	for _, parentKey := range []string{"left", "right"} {
		payload, err := json.Marshal(map[string]any{"parent_key": parentKey, "leaf_key": "same"})
		if err != nil {
			t.Fatal(err)
		}
		event := eventtest.ExistingRunRootIngress(eventtest.UUID(parentKey), "start", "test", "", payload, 0, busInternalTestRunID, events.EventEnvelope{}, time.Now().UTC())
		ctx := withConnectRoutePlanPreview(constructionIndexContext(t, source))
		values := map[string]string{"payload.parent_key": parentKey, "payload.leaf_key": "same"}
		_, selection, _, err := owner.Materialize(ctx, event, parentPlan, values)
		if err != nil || selection.Identity() != parents[parentKey] {
			t.Fatalf("parent edge selected another key: %+v err=%v", selection, err)
		}
		if err := selectConnectionConstruction(ctx, selection.Identity()); err != nil {
			t.Fatal(err)
		}
		materialized, leaf, _, err := owner.Materialize(ctx, event, leafPlan, values)
		if err != nil || !materialized.Failure.Empty() || leaf.Identity() != leaves[parentKey] {
			t.Fatalf("leaf edge lost selected parent: %+v %+v err=%v", materialized, leaf, err)
		}
	}
	if leaves["left"].EntityID == leaves["right"].EntityID {
		t.Fatal("same leaf key in two parents collapsed")
	}
	// Even exactly one existing parent/leaf cannot substitute for a compiled
	// ancestor selection. Keep the complete real root and one constructed branch.
	var singleBranch []pipeline.FlowInstanceObservation
	for _, observed := range index.observations {
		instance := observed.Identity()
		if instance == parents["right"] || instance == leaves["right"] || instance.ParentEntityID == parents["right"].EntityID {
			continue
		}
		singleBranch = append(singleBranch, observed)
	}
	if len(singleBranch) != 4 {
		t.Fatalf("sole-branch counterexample needs root, parent, middle and leaf: %+v", singleBranch)
	}
	owner.index = constructionIndexTestReader{observations: singleBranch}
	event := eventtest.ExistingRunRootIngress(eventtest.UUID("no-ancestor"), "start", "test", "", []byte(`{"parent_key":"left","leaf_key":"same"}`), 0, busInternalTestRunID, events.EventEnvelope{}, time.Now().UTC())
	if _, _, _, err := owner.Materialize(withConnectRoutePlanPreview(constructionIndexContext(t, source)), event, leafPlan, map[string]string{"payload.parent_key": "left", "payload.leaf_key": "same"}); err == nil {
		t.Fatal("sole existing receiver bypassed the missing ancestor edge")
	}
	proveLexicalAddressGeometries(t)
}

func lexicalAddressFixture(t *testing.T, crossAccount bool) string {
	t.Helper()
	return canonicalrouting.CopyLexicalAddressing(t, crossAccount)
}

func proveLexicalAddressGeometries(t *testing.T) {
	t.Helper()
	root := lexicalAddressFixture(t, false)
	source := loadConnectRoutePlanCanonicalSource(t, root)
	graph := pinrouting.CompileConnectGraph(source)
	if issues := graph.Issues(); len(issues) != 0 {
		t.Fatal(issues)
	}
	rootInstance := flowidentity.Stored(source, ".", busInternalTestRunID, busInternalTestRunID, busInternalTestRunID, "")
	index := constructionIndexTestReader{observations: []pipeline.FlowInstanceObservation{constructionIndexObservation(t, source, busInternalTestRunID, rootInstance, "")}}
	accounts, audits, tickets := map[string]flowidentity.Instance{}, map[string]flowidentity.Instance{}, map[string]flowidentity.Instance{}
	for _, key := range []string{"7", "8"} {
		account, err := flowidentity.KeyedChild(source, rootInstance, "account", key)
		if err != nil {
			t.Fatal(err)
		}
		audit, err := flowidentity.KeylessChild(source, account, "account/audit")
		if err != nil {
			t.Fatal(err)
		}
		ticket, err := flowidentity.KeyedChild(source, account, "account/ticket", "42")
		if err != nil {
			t.Fatal(err)
		}
		accounts[key], audits[key], tickets[key] = account, audit, ticket
		index.observations = append(index.observations, constructionIndexObservation(t, source, busInternalTestRunID, account, key), constructionIndexObservation(t, source, busInternalTestRunID, audit, ""), constructionIndexObservation(t, source, busInternalTestRunID, ticket, "42"))
	}
	for _, test := range []struct {
		name, event, receiver string
		source, want          flowidentity.Instance
		selectParent          bool
	}{
		{"C51_lexical_account", "ticket.done", "account/audit", tickets["7"], audits["7"], false},
		{"C52_root_selected_account", "root.audit", "account/audit", rootInstance, audits["7"], true},
		{"C53_explicit_other_account", "ticket.cross", "account", tickets["7"], accounts["8"], false},
	} {
		t.Run(test.name, func(t *testing.T) {
			testSource, testGraph := source, graph
			testIndex := index
			if test.name == "C53_explicit_other_account" {
				testSource = loadConnectRoutePlanCanonicalSource(t, lexicalAddressFixture(t, true))
				testGraph = pinrouting.CompileConnectGraph(testSource)
				if issues := testGraph.Issues(); len(issues) != 0 {
					t.Fatalf("frozen C53 root edge was not admitted: %+v", issues)
				}
				testIndex.observations = nil
				for _, observed := range index.observations {
					testIndex.observations = append(testIndex.observations, constructionIndexObservation(t, testSource, busInternalTestRunID, observed.Identity(), observed.InstanceKey()))
				}
			}
			owner := connectInstanceSelector{source: testSource, index: testIndex}
			resolver := connectRoutePlanResolver{lifecycle: owner}
			ctx := withConnectRoutePlanPreview(constructionIndexContext(t, testSource))
			routing := eventtest.ConcreteTemplateRoutingSource(test.source.TemplateID, test.source.InstancePath, test.source.EntityID)
			eventType := test.source.InstancePath + "/" + test.event
			if test.source.TemplateID == "." {
				routing = eventtest.RootRoutingSource(busInternalTestRunID)
				eventType = test.event
			}
			event := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID(test.name), events.EventType(eventType), "test", "", []byte(`{"account_id":"7","target_account_id":"8"}`), 0, busInternalTestRunID, events.EventEnvelope{}, routing, time.Now().UTC())
			values := map[string]string{"payload.account_id": "7", "payload.target_account_id": "8"}
			var target pinrouting.ConnectRoutePlan
			for _, plan := range testGraph.Plans() {
				if plan.SourceEndpoint().Readback().LocalEvent != test.event {
					continue
				}
				if test.selectParent && plan.ReceiverEndpoint().Readback().FlowID == "account" {
					_, parent, _, err := owner.Materialize(ctx, event, plan, values)
					if err != nil || parent.Identity() != accounts["7"] {
						t.Fatalf("root ancestor selection: %+v %v", parent, err)
					}
					if err := selectConnectionConstruction(ctx, parent.Identity()); err != nil {
						t.Fatal(err)
					}
				}
				if plan.ReceiverEndpoint().Readback().FlowID == test.receiver {
					target = plan
				}
			}
			materialized, selected, err := resolver.materializeConnectRoutePlan(ctx, event, target, values)
			if err != nil || !materialized.Failure.Empty() || selected.Identity() != test.want {
				t.Fatalf("exact edge selected another account: selected=%+v materialized=%+v want=%+v err=%v", selected, materialized, test.want, err)
			}
		})
	}
	t.Run("C08_missing_ancestor_key_teaching", func(t *testing.T) {
		path := filepath.Join(root, "events.yaml")
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(strings.Replace(string(raw), "root.audit:\n  account_id: text\n", "root.audit:\n", 1)), 0600); err != nil {
			t.Fatal(err)
		}
		repo := canonicalrouting.RepoRoot(t)
		_, err = contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
		if err == nil || !strings.Contains(err.Error(), "account_id") || !strings.Contains(err.Error(), "schema.yaml") {
			t.Fatalf("missing ancestor key lost located teaching: %v", err)
		}

		lexicalRoot := lexicalAddressFixture(t, false)
		_, err = contracts.LoadWorkflowContractBundleWithOverrides(repo, lexicalRoot, contracts.DefaultPlatformSpecFile(repo))
		if err != nil {
			t.Fatalf("lexical-key negative needs an admitted control: %v", err)
		}
		for label, body := range map[string]string{
			"account/ticket/events.yaml": "ticket.done:\n  account_id: text\n",
			"account/ticket/nodes.yaml":  "reporter:\n  opened:\n    emit: {event: ticket.done, fields: {account_id: payload.account_id}}\n",
		} {
			if err := os.WriteFile(filepath.Join(lexicalRoot, label), []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
		}
		_, err = contracts.LoadWorkflowContractBundleWithOverrides(repo, lexicalRoot, contracts.DefaultPlatformSpecFile(repo))
		if err != nil {
			t.Fatalf("typed lexical-key evidence needs an admitted control: %v", err)
		}
		if err := os.WriteFile(filepath.Join(lexicalRoot, "account", "nodes.yaml"), []byte("receiver:\n  root.audit: {}\n  ticket.done: {}\n"), 0600); err != nil {
			t.Fatal(err)
		}
		path = filepath.Join(lexicalRoot, "account", "schema.yaml")
		raw, err = os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		// The edge is account-local: its account is already the sender's
		// lexical owner, so payload cannot select a different account.
		row := "  - {event: ticket.done, from: ticket, to: ., resolution: select, key_from: payload.account_id}\n"
		if err := os.WriteFile(path, append(raw, []byte(row)...), 0600); err != nil {
			t.Fatal(err)
		}
		_, err = contracts.LoadWorkflowContractBundleWithOverrides(repo, lexicalRoot, contracts.DefaultPlatformSpecFile(repo))
		if err == nil || !strings.Contains(err.Error(), "bound by context") || !strings.Contains(err.Error(), "account/schema.yaml") {
			t.Fatalf("supplied key for lexical account lost located refusal: %v", err)
		}
	})
}

func TestA9ConnectionAncestorCandidatesAreResolvedOnlyByDependentPaths(t *testing.T) {
	for _, test := range []struct {
		name           string
		dependentEvent string
		competing      bool
		targeted       bool
		ambiguous      bool
		wantRoutes     int
	}{
		{"independent leaves beside dependent edge", "start", false, false, false, 4},
		{"nonmatching descendant with distinct parents", "other", true, false, false, 4},
		{"target filtered descendant with distinct parents", "start", true, true, false, 1},
		{"ambiguous required ancestor refuses before commit", "start", true, false, true, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyConnectionAncestorCandidates(t, test.dependentEvent, test.competing))
			store := &connectRoutePlanLifecycleStore{targetRouteMemoryStore: newTargetRouteMemoryStore()}
			eb, err := newScopedTestEventBus(store, EventBusOptions{
				ContractBundle: source,
				Durable: DurableDependencies{
					Instances:    store,
					RunLifecycle: &publicationRunPreflightTestStore{runID: busInternalTestRunID},
				},
				TemplateInstancePlanner: newTestFlowInstanceActivationOwner(store.Activate),
			})
			if err != nil {
				t.Fatal(err)
			}
			root := installConnectionSourceConstructionForRun(t, eb, source, ".", busInternalTestRunID)
			parent, err := flowidentity.KeyedChild(source, root, "parent", "stored-parent")
			if err != nil {
				t.Fatal(err)
			}
			middle, err := flowidentity.KeylessChild(source, parent, "parent/middle")
			if err != nil {
				t.Fatal(err)
			}
			leaf, err := flowidentity.KeyedChild(source, middle, "parent/middle/leaf", "stored-leaf")
			if err != nil {
				t.Fatal(err)
			}
			add := func(instance flowidentity.Instance, key string) {
				t.Helper()
				coordinate := testRunScopedFlowRoute(instance.Route())
				store.installConstructionReceipt(coordinate, pipeline.FlowConstructionPublicationEvidence{Identity: instance})
				store.installIndexObservation(constructionIndexObservation(t, source, busInternalTestRunID, instance, key))
			}
			add(parent, "parent-key")
			add(middle, "")
			add(leaf, "leaf-key")
			var workers []flowidentity.Instance
			for _, key := range []string{"parent-key", "leaf-key"} {
				worker, err := flowidentity.KeyedChild(source, root, "worker", key)
				if err != nil {
					t.Fatal(err)
				}
				add(worker, key)
				workers = append(workers, worker)
			}
			if test.competing {
				other, err := flowidentity.KeyedChild(source, root, "parent", "other-parent")
				if err != nil {
					t.Fatal(err)
				}
				add(other, "leaf-key")
			}
			before := make([]pipeline.FlowInstanceObservation, len(store.instanceObservations))
			for i, observed := range store.instanceObservations {
				before[i] = constructionIndexObservation(t, source, busInternalTestRunID, observed.Identity(), observed.InstanceKey())
			}
			beforeReceipts := maps.Clone(store.constructions)
			envelope := events.EventEnvelope{}
			if test.targeted {
				envelope = events.EnvelopeForTargetRoute(envelope, events.RouteIdentity{FlowID: "worker", FlowInstance: workers[0].InstancePath, EntityID: workers[0].EntityID})
			}
			event := eventtest.ExistingRunRootIngress(eventtest.UUID(test.name), "start", "test", "", []byte(`{"parent_key":"parent-key","leaf_key":"leaf-key"}`), 0, busInternalTestRunID, envelope, time.Now().UTC())
			check, err := eb.CheckPublishRecipientPlan(context.Background(), event)
			if test.ambiguous {
				if err == nil || !strings.Contains(err.Error(), "selected competing ancestors of parent") {
					t.Fatalf("required ancestor ambiguity was not refused: %+v %v", check, err)
				}
				if err := eb.Publish(context.Background(), event); err == nil || !strings.Contains(err.Error(), "selected competing ancestors of parent") {
					t.Fatalf("ambiguous publication was not refused: %v", err)
				}
			} else if err != nil || check.TargetFailure != "" || len(check.DeliveryRoutes) != test.wantRoutes {
				t.Fatalf("independent/dependent selections: %+v err=%v, want %d routes", check, err, test.wantRoutes)
			}
			if len(store.events) != 0 || len(store.routes) != 0 || len(store.activations) != 0 || !reflect.DeepEqual(before, store.instanceObservations) || !reflect.DeepEqual(beforeReceipts, store.constructions) {
				t.Fatal("planning or rejected ambiguity mutated publication/construction state")
			}
		})
	}
}

func TestA9ConnectionAncestorCandidatesDeduplicateOnlyExactOwners(t *testing.T) {
	source := nestedConnectionConstructionSource(t)
	root := flowidentity.Stored(source, ".", busInternalTestRunID, busInternalTestRunID, busInternalTestRunID, "")
	parent, err := flowidentity.KeyedChild(source, root, "parent", "stored-parent")
	if err != nil {
		t.Fatal(err)
	}
	ctx := withConnectRoutePlanPreview(context.Background())
	for range 2 {
		if err := selectConnectionConstruction(ctx, parent); err != nil {
			t.Fatal(err)
		}
	}
	preview := ctx.Value(connectRoutePlanPreviewRoutesKey{}).(*connectRoutePlanPreviewRoutes)
	if len(preview.selected["parent"]) != 1 {
		t.Fatal("same exact owner was registered twice")
	}
	otherRun := root
	otherRun.InstancePath, otherRun.EntityID = eventtest.UUID("other-run"), eventtest.UUID("other-run")
	other, err := flowidentity.KeyedChild(source, otherRun, "parent", "stored-parent")
	if err != nil {
		t.Fatal(err)
	}
	if err := selectConnectionConstruction(ctx, other); err != nil {
		t.Fatal(err)
	}
	owner := connectInstanceSelector{source: source}
	actual, err := owner.selectedConstructionChild(ctx, busInternalTestRunID, root, "parent", nil, preview.selected)
	if err != nil || actual != parent {
		t.Fatalf("foreign structural parent changed exact selection: %+v %v", actual, err)
	}
	for _, test := range []struct {
		name       string
		candidates []flowidentity.Instance
		want       flowidentity.Instance
		refused    bool
	}{
		{"no constructed provider receiver", nil, flowidentity.Instance{}, false},
		{"exact provider receiver", []flowidentity.Instance{parent}, parent, false},
		{"ambiguous provider execution dependency", []flowidentity.Instance{parent, other}, flowidentity.Instance{}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			actual, err := providerConstructionCandidate("parent", test.candidates)
			if (err != nil) != test.refused || actual != test.want {
				t.Fatalf("provider execution dependency lost uniqueness: %+v %v", actual, err)
			}
		})
	}
}
