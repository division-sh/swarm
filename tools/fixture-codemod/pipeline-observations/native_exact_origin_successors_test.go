package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

type publicationSuccessorEdit struct{ before, after string }

// Q2's diagnostic exclusion is settled in review 6183283144. This setup
// amendment reverses before the older publication edits, preserving their pins.
const rootForkPostCutBusinessSetup = `			if gate {
				// Branch isolation needs later business history, not runtime logs.
				var selected interface {
					PipelineObligations() pipelineobligation.Store
				} = rt.Postgres
				if rt.SQLite != nil {
					selected = rt.SQLite
				}
				advanceLifecycleForkSourceHistory(t, servedRuntimeProofAuthorActivityContext(t, rt.Runtime, rt.BundleHash), selected, seed.RunID)
			}
`

var exactOriginSuccessors = []struct {
	name, predecessor string
	priorSuccessor    bool
	edits             []publicationSuccessorEdit
}{
	{"TestServedCompiledTransitionSelectedCarrierEvidenceOnBothStores", "a4f998e7a74bd3679b029eb3cc5cfac11b6fdec524975b23118864a392a2852a", true, []publicationSuccessorEdit{
		// Reverse only the #2287/6148375122 presentation change; preserve the historical cut hash.
		{"path := fmt.Sprintf(\"nodes[\\\"controller\\\"].handlers[%q].rules[%d]\", \"work.\"+handler, index)\n", "path := fmt.Sprintf(\"nodes[\\\"controller\\\"].handlers[%q].rules[%d]\", \"work.\"+handler, index)\n\t\t\t\t\t\t\t\tdisplay := fmt.Sprintf(\"rules[%d]: when payload.choice == '%s'\", index, choice)\n"},
		{"selection.DisplayLabel() != choice ||", "selection.DisplayLabel() != display ||"},
		{"\t\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyLifecycleSelectedCarriers(t, reordered))", "\t\t\t\tprobe := newExactOriginPublicationProbe(t)\n\t\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyLifecycleSelectedCarriers(t, reordered))"},
		{"\t\t\t\t\t\t\t// One root input reaches all four flows through explicit", "\t\t\t\t\t\t\tif err := waitLifecycleCarrierSeedSettlement(t.Context(), probe, seed); err != nil {\n\t\t\t\t\t\t\t\tt.Fatal(err)\n\t\t\t\t\t\t\t}\n\t\t\t\t\t\t\t// One root input reaches all four flows through explicit"},
	}},
	{"TestServedForkSourceCompletionWithoutForkBothStores", "c39c80d0a1287e62369224647b85719fc1a2d4858963a312800e78522b0e01d1", false, []publicationSuccessorEdit{
		{"func TestServedForkSourceCompletionWithoutForkBothStores(t *testing.T) {\n\tfor _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {\n\t\tt.Run(string(backend), func(t *testing.T) {\n\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyForkDeliveryRouteEvidence(t))\n\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\"event_name\": \"parent.seeded\", \"bundle_hash\": rt.BundleHash,\n\t\t\t\t\"payload\": map[string]any{\"work_id\": \"no-fork-completion\"}, \"idempotency_key\": \"no-fork-seed\"})\n\t\t\twaitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)\n\t\t\tearly := readServedForkRecipientSourceDomain(t, rt, seed.RunID)\n\t\t\twaitForkReceiverSourceCompletion(t, rt, seed.RunID)\n\t\t\tsettled := readServedForkRecipientSourceDomain(t, rt, seed.RunID)\n\t\t\tcolumnsRows, err := rt.DB.Query(`SELECT * FROM event_deliveries WHERE 1=0`)\n\t\t\tif err != nil {\n\t\t\t\tt.Fatal(err)\n", "func TestServedForkSourceCompletionWithoutForkBothStores(t *testing.T) {\n\tfor _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {\n\t\tt.Run(string(backend), func(t *testing.T) {\n\t\t\tpublication := newExactOriginPublicationProbe(t)\n\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyForkDeliveryRouteEvidence(t))\n\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\"event_name\": \"parent.seeded\", \"bundle_hash\": rt.BundleHash,\n\t\t\t\t\"payload\": map[string]any{\"work_id\": \"no-fork-completion\"}, \"idempotency_key\": \"no-fork-seed\"})\n\t\t\twaitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)\n\t\t\tif err := waitExactOriginPublications(t.Context(), publication, seed.EventID); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tearlyRoot := storetest.ReadSemanticEventFixtureEvidence(t, t.Context(), rt.ReceiverStateReader, seed.RunID, seed.EventID)\n\t\t\tearly := readServedForkRecipientSourceDomain(t, rt, seed.RunID)\n\t\t\twaitForkReceiverSourceCompletion(t, rt, seed.RunID)\n\t\t\tsettled := readServedForkRecipientSourceDomain(t, rt, seed.RunID)\n\t\t\tsettledRoot := storetest.ReadSemanticEventFixtureEvidence(t, t.Context(), rt.ReceiverStateReader, seed.RunID, seed.EventID)\n\t\t\tif earlyRoot.PipelineReceiptCount != 1 || settledRoot.PipelineReceiptCount != 1 {\n\t\t\t\tt.Fatalf(\"joined root publication lost its settled receipt: %d->%d\", earlyRoot.PipelineReceiptCount, settledRoot.PipelineReceiptCount)\n\t\t\t}\n\t\t\tcolumnsRows, err := rt.DB.Query(`SELECT * FROM event_deliveries WHERE 1=0`)\n\t\t\tif err != nil {\n\t\t\t\tt.Fatal(err)\n"},
		{"\t\t\t\t}\n\t\t\t}\n\t\t\tsort.Strings(changed)\n\t\t\tt.Logf(\"NO FORK INVOKED: delivery-only quiescence -> pipeline completion changed=%v receipts=%d->%d\", changed, len(early[\"event_receipts\"]), len(settled[\"event_receipts\"]))\n\t\t\twaitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)\n\t\t\tif after := readServedForkRecipientSourceDomain(t, rt, seed.RunID); !reflect.DeepEqual(settled, after) {\n", "\t\t\t\t}\n\t\t\t}\n\t\t\tsort.Strings(changed)\n\t\t\t// Root handoff 0->1 is witnessed by the separate held-publication proof.\n\t\t\t// Child handoffs may still settle between these original snapshots.\n\t\t\tt.Logf(\"JOINED ROOT: receipts=%d->%d; root delta is zero, not positive handoff proof\", earlyRoot.PipelineReceiptCount, settledRoot.PipelineReceiptCount)\n\t\t\tt.Logf(\"NO FORK INVOKED: delivery-only quiescence -> pipeline completion changed=%v receipts=%d->%d\", changed, len(early[\"event_receipts\"]), len(settled[\"event_receipts\"]))\n\t\t\twaitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)\n\t\t\tif after := readServedForkRecipientSourceDomain(t, rt, seed.RunID); !reflect.DeepEqual(settled, after) {\n"},
	}},
	{"TestServedForkRootSourceIngressBothStores", "6402b2e0612aa31fca1b49cb85f08df34a78acb35c0d7af0bf10eedd1bf07717", false, []publicationSuccessorEdit{
		{"func TestServedForkRootSourceIngressBothStores(t *testing.T) {\n\tfor _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {\n\t\tt.Run(string(backend), func(t *testing.T) {\n\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyRootIngressServedFollowUp(t))\n\t\t\tvar started struct {\n\t\t\t\tRunID string `json:\"run_id\"`\n", "func TestServedForkRootSourceIngressBothStores(t *testing.T) {\n\tfor _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {\n\t\tt.Run(string(backend), func(t *testing.T) {\n\t\t\tpublication := newExactOriginPublicationProbe(t)\n\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyRootIngressServedFollowUp(t))\n\t\t\tvar started struct {\n\t\t\t\tRunID string `json:\"run_id\"`\n"},
		{"\t\t\trequireServedJSONRPCResult(t, rt.Endpoint, \"run.start\", map[string]any{\n\t\t\t\t\"bundle_hash\": rt.BundleHash, \"event_name\": \"item.received\", \"payload\": map[string]any{\"item_id\": \"ordinary-start\"},\n\t\t\t}, &started)\n\t\t\twaitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)\n\t\t\tpublished := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n\t\t\t\t\"bundle_hash\": rt.BundleHash, \"run_id\": started.RunID, \"event_name\": \"item.processed\",\n", "\t\t\trequireServedJSONRPCResult(t, rt.Endpoint, \"run.start\", map[string]any{\n\t\t\t\t\"bundle_hash\": rt.BundleHash, \"event_name\": \"item.received\", \"payload\": map[string]any{\"item_id\": \"ordinary-start\"},\n\t\t\t}, &started)\n\t\t\tvar receipt durabledata.OperationSummary\n\t\t\trequireServedJSONRPCResult(t, rt.Endpoint, \"data.show\", map[string]any{\n\t\t\t\t\"view\": \"operation\", \"detail\": \"summary\", \"operation_ref\": map[string]any{\"kind\": \"run_creation\", \"run_id\": started.RunID},\n\t\t\t}, &receipt)\n\t\t\tif receipt.RunCreation == nil || receipt.RunCreation.RunID != started.RunID || receipt.RunCreation.Outcome != \"created\" || receipt.RunCreation.EventID == \"\" {\n\t\t\t\tt.Fatal(\"run.start lacks exact acknowledged initial publication evidence\")\n\t\t\t}\n\t\t\tinitialEventID := receipt.RunCreation.EventID\n\t\t\twaitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)\n\t\t\tpublished := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n\t\t\t\t\"bundle_hash\": rt.BundleHash, \"run_id\": started.RunID, \"event_name\": \"item.processed\",\n"},
		{"\t\t\tif kind != \"root\" || source != (events.RouteIdentity{EntityID: started.RunID}) {\n\t\t\t\tt.Fatalf(\"ordinary API admission lost selected-root authority: %s %s\", kind, sourceRoute)\n\t\t\t}\n\t\t\tbefore := readServedForkRecipientSourceDomain(t, rt, started.RunID)\n\t\t\tparams := map[string]any{\"source_run_id\": started.RunID, \"fork_event_id\": published.EventID, \"allow_source_freeze\": true, \"idempotency_key\": \"root-source-fork\"}\n\t\t\tvar fork apiv1.RunForkExecutionResult\n", "\t\t\tif kind != \"root\" || source != (events.RouteIdentity{EntityID: started.RunID}) {\n\t\t\t\tt.Fatalf(\"ordinary API admission lost selected-root authority: %s %s\", kind, sourceRoute)\n\t\t\t}\n\t\t\tif err := waitExactOriginPublications(t.Context(), publication, initialEventID, published.EventID); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tbefore := readServedForkRecipientSourceDomain(t, rt, started.RunID)\n\t\t\tparams := map[string]any{\"source_run_id\": started.RunID, \"fork_event_id\": published.EventID, \"allow_source_freeze\": true, \"idempotency_key\": \"root-source-fork\"}\n\t\t\tvar fork apiv1.RunForkExecutionResult\n"},
	}},
	{"TestRunForkHistoricalIdentityPublicExecutionBothStores", "7ad3478d142208e03c1fe65145c03e7636e077d1837c636ecaa1cc6bdf78ca9e", false, []publicationSuccessorEdit{
		{"\t\t\t\treturn previous(owner)\n\t\t\t}\n\t\t\tt.Cleanup(func() { projectRuntimePersistenceForServe = previous })\n\t\t\troot := canonicalrouting.CopyForkReceiverBusinessMutationOwnership(t, false)\n\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)\n\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n", "\t\t\t\treturn previous(owner)\n\t\t\t}\n\t\t\tt.Cleanup(func() { projectRuntimePersistenceForServe = previous })\n\t\t\tpublication := newExactOriginPublicationProbe(t)\n\t\t\troot := canonicalrouting.CopyForkReceiverBusinessMutationOwnership(t, false)\n\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)\n\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n"},
		{"\t\t\t\t\"payload\": map[string]any{\"token\": \"receiver-proof\"}, \"idempotency_key\": \"historical-context-seed\",\n\t\t\t})\n\t\t\twaitForkReceiverSourceCompletion(t, rt, seed.RunID)\n\t\t\trequireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n\t\t\t\t\"event_name\": \"start.requested\", \"run_id\": seed.RunID, \"source_event_id\": seed.EventID,\n\t\t\t\t\"payload\": map[string]any{\"token\": \"receiver-proof\"}, \"idempotency_key\": \"historical-context-request\",\n\t\t\t})\n", "\t\t\t\t\"payload\": map[string]any{\"token\": \"receiver-proof\"}, \"idempotency_key\": \"historical-context-seed\",\n\t\t\t})\n\t\t\twaitForkReceiverSourceCompletion(t, rt, seed.RunID)\n\t\t\tstarted := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n\t\t\t\t\"event_name\": \"start.requested\", \"run_id\": seed.RunID, \"source_event_id\": seed.EventID,\n\t\t\t\t\"payload\": map[string]any{\"token\": \"receiver-proof\"}, \"idempotency_key\": \"historical-context-request\",\n\t\t\t})\n"},
		{"\t\t\t\t\"payload\": map[string]any{\"token\": \"receiver-proof\"}, \"idempotency_key\": \"historical-context-sibling\",\n\t\t\t})\n\t\t\twaitForkReceiverSourceCompletion(t, rt, sibling.RunID)\n\t\t\tfamily, ok := selected.RunFork()\n\t\t\tif !ok {\n\t\t\t\tt.Fatal(\"missing real selected fork owner\")\n", "\t\t\t\t\"payload\": map[string]any{\"token\": \"receiver-proof\"}, \"idempotency_key\": \"historical-context-sibling\",\n\t\t\t})\n\t\t\twaitForkReceiverSourceCompletion(t, rt, sibling.RunID)\n\t\t\tif err := waitExactOriginPublications(t.Context(), publication, seed.EventID, started.EventID, sibling.EventID); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tfamily, ok := selected.RunFork()\n\t\t\tif !ok {\n\t\t\t\tt.Fatal(\"missing real selected fork owner\")\n"},
	}},
	{"TestServedForkAccumulatorRetainedGenerationBothStores", "77001f0ba98703782b4216fe19bd812d179e540bb68799b3957ccb14f0b38519", false, []publicationSuccessorEdit{
		{"\t\t\t\t}\n\t\t\t\tt.Cleanup(func() { projectRuntimePersistenceForServe = previous })\n\t\t\t\troot := canonicalrouting.CopyForkLoopAccumulator(t, clearOnAdmit)\n\t\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)\n\t\t\t\tstarted := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n\t\t\t\t\t\"event_name\": \"work.requested\", \"bundle_hash\": rt.BundleHash,\n", "\t\t\t\t}\n\t\t\t\tt.Cleanup(func() { projectRuntimePersistenceForServe = previous })\n\t\t\t\troot := canonicalrouting.CopyForkLoopAccumulator(t, clearOnAdmit)\n\t\t\t\tpublication := newExactOriginPublicationProbe(t)\n\t\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)\n\t\t\t\tstarted := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n\t\t\t\t\t\"event_name\": \"work.requested\", \"bundle_hash\": rt.BundleHash,\n"},
		{"\t\t\t\tif len(initial) != wantInitial {\n\t\t\t\t\tt.Fatalf(\"first ordinary admission: %#v\", initial)\n\t\t\t\t}\n\t\t\t\trequireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n\t\t\t\t\t\"event_name\": \"review.retry\", \"run_id\": started.RunID, \"source_event_id\": started.EventID,\n\t\t\t\t\t\"payload\": map[string]any{\"revision_id\": first.RevisionID, \"token\": \"loop-notice-proof\"}, \"idempotency_key\": \"accumulator-repeat\",\n\t\t\t\t})\n", "\t\t\t\tif len(initial) != wantInitial {\n\t\t\t\t\tt.Fatalf(\"first ordinary admission: %#v\", initial)\n\t\t\t\t}\n\t\t\t\trepeated := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n\t\t\t\t\t\"event_name\": \"review.retry\", \"run_id\": started.RunID, \"source_event_id\": started.EventID,\n\t\t\t\t\t\"payload\": map[string]any{\"revision_id\": first.RevisionID, \"token\": \"loop-notice-proof\"}, \"idempotency_key\": \"accumulator-repeat\",\n\t\t\t\t})\n"},
		{"\t\t\t\tif err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='review/review.requested' AND CAST(payload AS TEXT) LIKE $2`, started.RunID, \"%\"+current.RevisionID+\"%\").Scan(&frontier); err != nil {\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}\n\t\t\t\tbefore := readServedForkRecipientSourceDomain(t, rt, started.RunID)\n\t\t\t\tfamily, ok := selected.RunFork()\n\t\t\t\tif !ok {\n", "\t\t\t\tif err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='review/review.requested' AND CAST(payload AS TEXT) LIKE $2`, started.RunID, \"%\"+current.RevisionID+\"%\").Scan(&frontier); err != nil {\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}\n\t\t\t\tif err := waitExactOriginPublications(t.Context(), publication, started.EventID, repeated.EventID); err != nil {\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}\n\t\t\t\tbefore := readServedForkRecipientSourceDomain(t, rt, started.RunID)\n\t\t\t\tfamily, ok := selected.RunFork()\n\t\t\t\tif !ok {\n"},
	}},
	{"TestServedForkLoopGenerationStateEffectBothStores", "ce42ac20645642f41d44044d4d7f958457e702c3114be35a85992a1a2affe5d1", false, []publicationSuccessorEdit{
		{"\t\t\tif err != nil || !found || role.LoopID() != \"revision\" || role.RevisionField() != \"revision_id\" {\n\t\t\t\tt.Fatalf(\"original input declaration: %+v %v %v\", role, found, err)\n\t\t\t}\n\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)\n\t\t\tstarted := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n\t\t\t\t\"event_name\": \"work.requested\", \"bundle_hash\": rt.BundleHash,\n", "\t\t\tif err != nil || !found || role.LoopID() != \"revision\" || role.RevisionField() != \"revision_id\" {\n\t\t\t\tt.Fatalf(\"original input declaration: %+v %v %v\", role, found, err)\n\t\t\t}\n\t\t\tpublication := newExactOriginPublicationProbe(t)\n\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)\n\t\t\tstarted := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n\t\t\t\t\"event_name\": \"work.requested\", \"bundle_hash\": rt.BundleHash,\n"},
		{"\t\t\t\tt.Fatalf(\"ordinary source loop: %+v\", source)\n\t\t\t}\n\t\t\tsourceRoute := requireForkLoopStateEffect(t, rt, started.RunID, frontier, source.RevisionID)\n\t\t\tsourceDomain := readServedForkRecipientSourceDomain(t, rt, started.RunID)\n\t\t\tsourceNotices := readForkReceiverNoticeDomain(t, rt, started.RunID)\n\t\t\tparams := map[string]any{\"source_run_id\": started.RunID, \"fork_event_id\": frontier, \"allow_source_freeze\": true, \"idempotency_key\": \"loop-notice-fork\"}\n", "\t\t\t\tt.Fatalf(\"ordinary source loop: %+v\", source)\n\t\t\t}\n\t\t\tsourceRoute := requireForkLoopStateEffect(t, rt, started.RunID, frontier, source.RevisionID)\n\t\t\tif err := waitExactOriginPublications(t.Context(), publication, started.EventID); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tsourceDomain := readServedForkRecipientSourceDomain(t, rt, started.RunID)\n\t\t\tsourceNotices := readForkReceiverNoticeDomain(t, rt, started.RunID)\n\t\t\tparams := map[string]any{\"source_run_id\": started.RunID, \"fork_event_id\": frontier, \"allow_source_freeze\": true, \"idempotency_key\": \"loop-notice-fork\"}\n"},
	}},
	{"TestSelectedForkSupplementalReceiverAcquisitionWithoutPostRevisionEmissionBothStores", "82e6f2479733ee12f720560d2f4115c974c296ac1b4e58571ad47424d304e307", false, []publicationSuccessorEdit{
		{"\t\t\tpolicy canonicalrouting.ForkReceiverPolicy\n\t\t}{{\"constructor_owned\", canonicalrouting.ForkReceiverConstructorOwned}} {\n\t\t\tt.Run(string(backend)+\"/\"+tc.name, func(t *testing.T) {\n\t\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyForkReceiverAcquisitionWithoutFinishedEmission(t, tc.policy))\n\t\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\"event_name\": \"start.seeded\", \"bundle_hash\": rt.BundleHash, \"payload\": map[string]any{\"token\": \"receiver-proof\"}, \"idempotency_key\": \"acquisition-seed\"})\n\t\t\t\twaitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)\n", "\t\t\tpolicy canonicalrouting.ForkReceiverPolicy\n\t\t}{{\"constructor_owned\", canonicalrouting.ForkReceiverConstructorOwned}} {\n\t\t\tt.Run(string(backend)+\"/\"+tc.name, func(t *testing.T) {\n\t\t\t\tpublication := newExactOriginPublicationProbe(t)\n\t\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyForkReceiverAcquisitionWithoutFinishedEmission(t, tc.policy))\n\t\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\"event_name\": \"start.seeded\", \"bundle_hash\": rt.BundleHash, \"payload\": map[string]any{\"token\": \"receiver-proof\"}, \"idempotency_key\": \"acquisition-seed\"})\n\t\t\t\twaitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)\n"},
		{"\t\t\t\tif sourceEvidence.Source.Kind() != events.RoutingSourceStaticFlow || sourceEvidence.Source.Route() != (events.RouteIdentity{FlowID: \"producer\", FlowInstance: \"producer\", EntityID: producer.ID}) {\n\t\t\t\t\tt.Fatalf(\"immutable source producer: %+v\", sourceEvidence)\n\t\t\t\t}\n\t\t\t\tbefore, companions := readServedForkRecipientSourceDomain(t, rt, seed.RunID), readForkReceiverCompanions(t, rt, seed.RunID)\n\t\t\t\tdefer func() {\n\t\t\t\t\tif !reflect.DeepEqual(before, readServedForkRecipientSourceDomain(t, rt, seed.RunID)) || !reflect.DeepEqual(companions, readForkReceiverCompanions(t, rt, seed.RunID)) {\n", "\t\t\t\tif sourceEvidence.Source.Kind() != events.RoutingSourceStaticFlow || sourceEvidence.Source.Route() != (events.RouteIdentity{FlowID: \"producer\", FlowInstance: \"producer\", EntityID: producer.ID}) {\n\t\t\t\t\tt.Fatalf(\"immutable source producer: %+v\", sourceEvidence)\n\t\t\t\t}\n\t\t\t\tif err := waitExactOriginPublications(t.Context(), publication, seed.EventID, started.EventID); err != nil {\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}\n\t\t\t\tbefore, companions := readServedForkRecipientSourceDomain(t, rt, seed.RunID), readForkReceiverCompanions(t, rt, seed.RunID)\n\t\t\t\tdefer func() {\n\t\t\t\t\tif !reflect.DeepEqual(before, readServedForkRecipientSourceDomain(t, rt, seed.RunID)) || !reflect.DeepEqual(companions, readForkReceiverCompanions(t, rt, seed.RunID)) {\n"},
	}},
	{"runForkReceiverOwnershipJourney", "ca2e7c34f5b8c4863fb3492254d3f82bb78b82e90a456e8cb4d3dfa875bc8506", false, []publicationSuccessorEdit{
		{"\t\t\treturn events.DeliveryRoute{}\n\t\t}\n\t}\n\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)\n\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n\t\t\"event_name\": seedName, \"bundle_hash\": rt.BundleHash,\n", "\t\t\treturn events.DeliveryRoute{}\n\t\t}\n\t}\n\tpublication := newExactOriginPublicationProbe(t)\n\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)\n\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n\t\t\"event_name\": seedName, \"bundle_hash\": rt.BundleHash,\n"},
		{"\tif started.RunID != seed.RunID {\n\t\tt.Fatal(\"second source ingress escaped the seeded run\")\n\t}\n\tvar frontier string\n\tif err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name=$2`, seed.RunID, prefix+\"producer/work.ready\").Scan(&frontier); err != nil {\n\t\tt.Fatalf(\"actual producer did not emit fork frontier: %v\", err)\n", "\tif started.RunID != seed.RunID {\n\t\tt.Fatal(\"second source ingress escaped the seeded run\")\n\t}\n\torigins := []string{seed.EventID, started.EventID}\n\tvar frontier string\n\tif err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name=$2`, seed.RunID, prefix+\"producer/work.ready\").Scan(&frontier); err != nil {\n\t\tt.Fatalf(\"actual producer did not emit fork frontier: %v\", err)\n"},
		{"\t\t\tt.Fatal(err)\n\t\t}\n\t\tfrontiers = append(frontiers, frontier)\n\t\tbeforeDuplicate := readServedForkRecipientSourceDomain(t, rt, seed.RunID)\n\t\tbeforeDuplicateNotices := readForkReceiverNoticeDomain(t, rt, seed.RunID)\n\t\tduplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, request)\n", "\t\t\tt.Fatal(err)\n\t\t}\n\t\tfrontiers = append(frontiers, frontier)\n\t\torigins = append(origins, second.EventID)\n\t\tif err := waitExactOriginPublications(t.Context(), publication, origins...); err != nil {\n\t\t\tt.Fatal(err)\n\t\t}\n\t\tbeforeDuplicate := readServedForkRecipientSourceDomain(t, rt, seed.RunID)\n\t\tbeforeDuplicateNotices := readForkReceiverNoticeDomain(t, rt, seed.RunID)\n\t\tduplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, request)\n"},
		{"\t\t\t\"payload\": map[string]any{\"token\": \"receiver-proof\"}, \"idempotency_key\": \"ownership-sibling-seed\"})\n\t\twaitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, siblingSeed.RunID)\n\t\twaitForkReceiverSourceCompletion(t, rt, siblingSeed.RunID)\n\t\trequireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\"event_name\": requestName, \"run_id\": siblingSeed.RunID, \"source_event_id\": siblingSeed.EventID,\n\t\t\t\"payload\": map[string]any{\"token\": \"receiver-proof\"}, \"idempotency_key\": \"ownership-sibling-request\"})\n\t\twaitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, siblingSeed.RunID)\n\t\twaitForkReceiverSourceCompletion(t, rt, siblingSeed.RunID)\n", "\t\t\t\"payload\": map[string]any{\"token\": \"receiver-proof\"}, \"idempotency_key\": \"ownership-sibling-seed\"})\n\t\twaitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, siblingSeed.RunID)\n\t\twaitForkReceiverSourceCompletion(t, rt, siblingSeed.RunID)\n\t\tsiblingRequest := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\"event_name\": requestName, \"run_id\": siblingSeed.RunID, \"source_event_id\": siblingSeed.EventID,\n\t\t\t\"payload\": map[string]any{\"token\": \"receiver-proof\"}, \"idempotency_key\": \"ownership-sibling-request\"})\n\t\twaitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, siblingSeed.RunID)\n\t\twaitForkReceiverSourceCompletion(t, rt, siblingSeed.RunID)\n"},
		{"\t\t\t}\n\t\t\trequireEffect(t, rt, siblingSeed.RunID, siblingFrontier, path, receiver.Path, \"existing_entity\", siblingRows[path].ID)\n\t\t}\n\t\tsiblingDomain, siblingCompanions := readServedForkRecipientSourceDomain(t, rt, siblingSeed.RunID), readForkReceiverCompanions(t, rt, siblingSeed.RunID)\n\t\tsiblingNotices := readForkReceiverNoticeDomain(t, rt, siblingSeed.RunID)\n\t\tdefer func() {\n", "\t\t\t}\n\t\t\trequireEffect(t, rt, siblingSeed.RunID, siblingFrontier, path, receiver.Path, \"existing_entity\", siblingRows[path].ID)\n\t\t}\n\t\tif err := waitExactOriginPublications(t.Context(), publication, siblingSeed.EventID, siblingRequest.EventID); err != nil {\n\t\t\tt.Fatal(err)\n\t\t}\n\t\tsiblingDomain, siblingCompanions := readServedForkRecipientSourceDomain(t, rt, siblingSeed.RunID), readForkReceiverCompanions(t, rt, siblingSeed.RunID)\n\t\tsiblingNotices := readForkReceiverNoticeDomain(t, rt, siblingSeed.RunID)\n\t\tdefer func() {\n"},
		{"\t\t\t}\n\t\t}()\n\t}\n\tbefore := readServedForkRecipientSourceDomain(t, rt, seed.RunID)\n\tbeforeCompanions := readForkReceiverCompanions(t, rt, seed.RunID)\n\tbeforeNotices := readForkReceiverNoticeDomain(t, rt, seed.RunID)\n", "\t\t\t}\n\t\t}()\n\t}\n\tif err := waitExactOriginPublications(t.Context(), publication, origins...); err != nil {\n\t\tt.Fatal(err)\n\t}\n\tbefore := readServedForkRecipientSourceDomain(t, rt, seed.RunID)\n\tbeforeCompanions := readForkReceiverCompanions(t, rt, seed.RunID)\n\tbeforeNotices := readForkReceiverNoticeDomain(t, rt, seed.RunID)\n"},
	}},
	{"TestSelectedForkIndependentReceiverBusinessMutationBothStores", "12233c9fe07584baa4444654f05e533f973edf2553a67aa3dac84f748f641a0d", false, []publicationSuccessorEdit{
		{"\tfor _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {\n\t\tfor _, entityless := range []bool{false, true} {\n\t\t\tt.Run(fmt.Sprintf(\"%s/entityless_producer=%t\", backend, entityless), func(t *testing.T) {\n\t\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyForkReceiverBusinessMutationOwnership(t, entityless))\n\t\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\"event_name\": \"start.seeded\", \"bundle_hash\": rt.BundleHash, \"payload\": map[string]any{\"token\": \"receiver-proof\"}, \"idempotency_key\": \"business-seed\"})\n\t\t\t\twaitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)\n", "\tfor _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {\n\t\tfor _, entityless := range []bool{false, true} {\n\t\t\tt.Run(fmt.Sprintf(\"%s/entityless_producer=%t\", backend, entityless), func(t *testing.T) {\n\t\t\t\tpublication := newExactOriginPublicationProbe(t)\n\t\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyForkReceiverBusinessMutationOwnership(t, entityless))\n\t\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\"event_name\": \"start.seeded\", \"bundle_hash\": rt.BundleHash, \"payload\": map[string]any{\"token\": \"receiver-proof\"}, \"idempotency_key\": \"business-seed\"})\n\t\t\t\twaitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)\n"},
		{"\t\t\t\t\tt.Fatalf(\"immutable producer evidence: %+v\", sourceEvidence)\n\t\t\t\t}\n\t\t\t\tsourceRoute := requireForkReceiverBusinessMutation(t, rt, seed.RunID, frontier, consumer.ID)\n\t\t\t\tbefore, companions := readServedForkRecipientSourceDomain(t, rt, seed.RunID), readForkReceiverCompanions(t, rt, seed.RunID)\n\t\t\t\tdefer func() {\n\t\t\t\t\tif !reflect.DeepEqual(before, readServedForkRecipientSourceDomain(t, rt, seed.RunID)) || !reflect.DeepEqual(companions, readForkReceiverCompanions(t, rt, seed.RunID)) {\n", "\t\t\t\t\tt.Fatalf(\"immutable producer evidence: %+v\", sourceEvidence)\n\t\t\t\t}\n\t\t\t\tsourceRoute := requireForkReceiverBusinessMutation(t, rt, seed.RunID, frontier, consumer.ID)\n\t\t\t\tif err := waitExactOriginPublications(t.Context(), publication, seed.EventID, started.EventID); err != nil {\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}\n\t\t\t\tbefore, companions := readServedForkRecipientSourceDomain(t, rt, seed.RunID), readForkReceiverCompanions(t, rt, seed.RunID)\n\t\t\t\tdefer func() {\n\t\t\t\t\tif !reflect.DeepEqual(before, readServedForkRecipientSourceDomain(t, rt, seed.RunID)) || !reflect.DeepEqual(companions, readForkReceiverCompanions(t, rt, seed.RunID)) {\n"},
	}},
	{"TestServedForkConnectedRecipientAuthorityOnBothStores", "1c948c9a01cd2436132cb1cf2fabbd4a43502c509b2342cdc7006335f7cab363", false, []publicationSuccessorEdit{
		{"\t\t\t\treturn previous(owner)\n\t\t\t}\n\t\t\tt.Cleanup(func() { projectRuntimePersistenceForServe = previous })\n\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyForkDeliveryRouteEvidence(t))\n\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n\t\t\t\t\"event_name\": \"parent.seeded\", \"bundle_hash\": rt.BundleHash,\n", "\t\t\t\treturn previous(owner)\n\t\t\t}\n\t\t\tt.Cleanup(func() { projectRuntimePersistenceForServe = previous })\n\t\t\tpublication := newExactOriginPublicationProbe(t)\n\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyForkDeliveryRouteEvidence(t))\n\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n\t\t\t\t\"event_name\": \"parent.seeded\", \"bundle_hash\": rt.BundleHash,\n"},
		{"\t\t\t}\n\t\t\trequireServedForkConsumerAuthority(t, rt, seed.RunID, plan.ForkPoint.EventID)\n\t\t\tt.Logf(\"source consumer settled before fork: source_run=%s source_event=%s\", seed.RunID, plan.ForkPoint.EventID)\n\t\t\tbefore := readServedForkRecipientSourceDomain(t, rt, seed.RunID)\n\t\t\tt.Logf(\"source entity-state rows run=%s: %v\", seed.RunID, before[\"entity_state\"])\n\t\t\tcheckSource := func() {\n", "\t\t\t}\n\t\t\trequireServedForkConsumerAuthority(t, rt, seed.RunID, plan.ForkPoint.EventID)\n\t\t\tt.Logf(\"source consumer settled before fork: source_run=%s source_event=%s\", seed.RunID, plan.ForkPoint.EventID)\n\t\t\tif err := waitExactOriginPublications(t.Context(), publication, seed.EventID); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tbefore := readServedForkRecipientSourceDomain(t, rt, seed.RunID)\n\t\t\tt.Logf(\"source entity-state rows run=%s: %v\", seed.RunID, before[\"entity_state\"])\n\t\t\tcheckSource := func() {\n"},
	}},
	{"TestSelectedForkPendingInputMixedCompletionBothStores", "680c1c2bd121be6f2ef620ec7a73d687c69ab9eba7a455d4bf3982951bf7a599", false, []publicationSuccessorEdit{
		{"\t\t\tprojectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence { selected = owner; return previous(owner) }\n\t\t\tt.Cleanup(func() { projectRuntimePersistenceForServe = previous })\n\t\t\troot := canonicalrouting.CopySelectedForkPendingInput(t, canonicalrouting.PendingInputMixedCompletion)\n\t\t\tchildStarted := make(chan struct{}, 1)\n\t\t\trelease := make(chan struct{})\n\t\t\tvar once sync.Once\n", "\t\t\tprojectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence { selected = owner; return previous(owner) }\n\t\t\tt.Cleanup(func() { projectRuntimePersistenceForServe = previous })\n\t\t\troot := canonicalrouting.CopySelectedForkPendingInput(t, canonicalrouting.PendingInputMixedCompletion)\n\t\t\tpublication := newExactOriginPublicationProbe(t)\n\t\t\tchildStarted := make(chan struct{}, 1)\n\t\t\trelease := make(chan struct{})\n\t\t\tvar once sync.Once\n"},
		{"\t\t\tif !found {\n\t\t\t\tt.Fatal(\"pending child disappeared from frontier\")\n\t\t\t}\n\t\t\tbefore := snapshotForkReceiverApplication(t, rt)\n\t\t\tresponse := requestServedJSONRPC(t, rt.Endpoint, \"run.fork\", map[string]any{\"source_run_id\": seed.RunID, \"fork_event_id\": marker.EventID, \"allow_source_freeze\": true, \"idempotency_key\": \"mixed-fork\"})\n\t\t\tif response.Error == nil {\n", "\t\t\tif !found {\n\t\t\t\tt.Fatal(\"pending child disappeared from frontier\")\n\t\t\t}\n\t\t\tif err := waitExactOriginPublications(t.Context(), publication, seed.EventID, input.EventID, marker.EventID); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tbefore := snapshotForkReceiverApplication(t, rt)\n\t\t\tresponse := requestServedJSONRPC(t, rt.Endpoint, \"run.fork\", map[string]any{\"source_run_id\": seed.RunID, \"fork_event_id\": marker.EventID, \"allow_source_freeze\": true, \"idempotency_key\": \"mixed-fork\"})\n\t\t\tif response.Error == nil {\n"},
	}},
	{"TestSelectedForkReceiverAcquisitionCapabilityRefusalBothStores", "1397ba1cdda1b3aa78c85213b39f270518f5ab9bc0f72708b6fed01beeea6459", false, []publicationSuccessorEdit{
		{"func TestSelectedForkReceiverAcquisitionCapabilityRefusalBothStores(t *testing.T) {\n\tfor _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {\n\t\tt.Run(string(backend)+\"/template_resolution_create\", func(t *testing.T) {\n\t\t\troot := canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{Mint: canonicalrouting.CreateMintUUID})\n\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)\n\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\"event_name\": \"validation.triggered\", \"bundle_hash\": rt.BundleHash,\n", "func TestSelectedForkReceiverAcquisitionCapabilityRefusalBothStores(t *testing.T) {\n\tfor _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {\n\t\tt.Run(string(backend)+\"/template_resolution_create\", func(t *testing.T) {\n\t\t\tpublication := newExactOriginPublicationProbe(t)\n\t\t\troot := canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{Mint: canonicalrouting.CreateMintUUID})\n\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)\n\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\"event_name\": \"validation.triggered\", \"bundle_hash\": rt.BundleHash,\n"},
		{"\t\t\t\tt.Fatalf(\"dynamic source did not settle: %d %v\", incomplete, err)\n\t\t\t}\n\t\t\trequireReceiverPublicReadback(t, rt, seed.RunID)\n\t\t\tbefore := snapshotForkReceiverApplication(t, rt)\n\t\t\tparams := map[string]any{\"source_run_id\": seed.RunID, \"fork_event_id\": frontier, \"allow_source_freeze\": true, \"idempotency_key\": \"capability-fork\"}\n\t\t\tfor attempt := 0; attempt < 2; attempt++ {\n", "\t\t\t\tt.Fatalf(\"dynamic source did not settle: %d %v\", incomplete, err)\n\t\t\t}\n\t\t\trequireReceiverPublicReadback(t, rt, seed.RunID)\n\t\t\tif err := waitExactOriginPublications(t.Context(), publication, seed.EventID); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tbefore := snapshotForkReceiverApplication(t, rt)\n\t\t\tparams := map[string]any{\"source_run_id\": seed.RunID, \"fork_event_id\": frontier, \"allow_source_freeze\": true, \"idempotency_key\": \"capability-fork\"}\n\t\t\tfor attempt := 0; attempt < 2; attempt++ {\n"},
	}},
	{"runServedRunForkBackendProof", "fa7a4017a018eb8ac2fc3a6200f7dc941fed42e1560a77c8a766e477f48c924a", false, []publicationSuccessorEdit{
		{"\trt := startServedControlProofRuntimeWithFixture(t, backend, func(t *testing.T) string {\n\t\treturn canonicalrouting.CopyRootIngressServedFollowUp(t)\n\t})\n\tstarted := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n\t\t\"bundle_hash\":     rt.BundleHash,\n\t\t\"event_name\":      \"item.received\",\n", "\trt := startServedControlProofRuntimeWithFixture(t, backend, func(t *testing.T) string {\n\t\treturn canonicalrouting.CopyRootIngressServedFollowUp(t)\n\t})\n\tpublication := rt.Probe.Raw()\n\tstarted := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n\t\t\"bundle_hash\":     rt.BundleHash,\n\t\t\"event_name\":      \"item.received\",\n"},
		{"\t\t}\n\t\ttime.Sleep(10 * time.Millisecond)\n\t}\n\tbeforeRefusal := snapshotForkReceiverApplication(t, rt)\n\t// A selected fork cannot acquire future ordinary ingress from the loaded\n\t// same-hash runtime. Retained controls still own its explicit terminal stop.\n", "\t\t}\n\t\ttime.Sleep(10 * time.Millisecond)\n\t}\n\tif err := waitExactOriginPublications(t.Context(), publication, started.EventID, published.EventID); err != nil {\n\t\tt.Fatal(err)\n\t}\n\tbeforeRefusal := snapshotForkReceiverApplication(t, rt)\n\t// A selected fork cannot acquire future ordinary ingress from the loaded\n\t// same-hash runtime. Retained controls still own its explicit terminal stop.\n"},
	}},
	{"TestExplicitTargetRestrictsConnectedSiblingsBothStores", "4b4d09d0c5ff107d27f7a6d6175a684a271a154db87f1d39440ec441264a7ba5", false, []publicationSuccessorEdit{
		{"\tfor _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {\n\t\tt.Run(string(backend), func(t *testing.T) {\n\t\t\troot := canonicalrouting.CopySelectedForkPendingInput(t, canonicalrouting.PendingInputConnectedSiblings)\n\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)\n\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n\t\t\t\t\"event_name\": \"work.seeded\", \"bundle_hash\": rt.BundleHash, \"payload\": map[string]any{\"seed\": true}, \"idempotency_key\": \"target-seed\",\n", "\tfor _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {\n\t\tt.Run(string(backend), func(t *testing.T) {\n\t\t\troot := canonicalrouting.CopySelectedForkPendingInput(t, canonicalrouting.PendingInputConnectedSiblings)\n\t\t\tpublication := newExactOriginPublicationProbe(t)\n\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)\n\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n\t\t\t\t\"event_name\": \"work.seeded\", \"bundle_hash\": rt.BundleHash, \"payload\": map[string]any{\"seed\": true}, \"idempotency_key\": \"target-seed\",\n"},
		{"\t\t\tif replayed.EventID != point.EventID || !reflect.DeepEqual(replayed.Deliveries, point.Deliveries) {\n\t\t\t\tt.Fatalf(\"idempotent replay = %+v, want %+v\", replayed, point)\n\t\t\t}\n\t\t\tbefore := snapshotForkReceiverApplication(t, rt)\n\t\t\tinvalid := map[string]any{\n\t\t\t\t\"event_name\": \"work.first\", \"run_id\": seed.RunID, \"payload\": map[string]any{\"token\": \"invalid\"},\n", "\t\t\tif replayed.EventID != point.EventID || !reflect.DeepEqual(replayed.Deliveries, point.Deliveries) {\n\t\t\t\tt.Fatalf(\"idempotent replay = %+v, want %+v\", replayed, point)\n\t\t\t}\n\t\t\tif err := waitExactOriginPublications(t.Context(), publication, seed.EventID, point.EventID); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tbefore := snapshotForkReceiverApplication(t, rt)\n\t\t\tinvalid := map[string]any{\n\t\t\t\t\"event_name\": \"work.first\", \"run_id\": seed.RunID, \"payload\": map[string]any{\"token\": \"invalid\"},\n"},
	}},
	{"TestSelectedForkPendingInputBothStores", "3bb22a890b6d963d761381078de53d418305061a1aa67dd0f07921f6c377f795", true, []publicationSuccessorEdit{
		{"\t\t\t\t\tvariant = canonicalrouting.PendingInputOrdinaryRoot\n\t\t\t\t}\n\t\t\t\troot := canonicalrouting.CopySelectedForkPendingInput(t, variant)\n\t\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)\n\t\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n\t\t\t\t\t\"event_name\": \"work.seeded\", \"bundle_hash\": rt.BundleHash, \"payload\": map[string]any{\"seed\": true}, \"idempotency_key\": \"input-seed\",\n", "\t\t\t\t\tvariant = canonicalrouting.PendingInputOrdinaryRoot\n\t\t\t\t}\n\t\t\t\troot := canonicalrouting.CopySelectedForkPendingInput(t, variant)\n\t\t\t\tpublication := newExactOriginPublicationProbe(t)\n\t\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)\n\t\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n\t\t\t\t\t\"event_name\": \"work.seeded\", \"bundle_hash\": rt.BundleHash, \"payload\": map[string]any{\"seed\": true}, \"idempotency_key\": \"input-seed\",\n"},
		{"\t\t\t\t\twantDone = 1\n\t\t\t\t}\n\t\t\t\tif mode == \"ordinary_child\" || mode == \"ordinary_root\" {\n\t\t\t\t\tbefore := snapshotForkReceiverApplication(t, rt)\n\t\t\t\t\tresponse := requireServedJSONRPCError(t, rt.Endpoint, \"event.publish\", params)\n\t\t\t\t\tif response.Data[\"code\"] != apiv1.EventNotDeclaredCode {\n", "\t\t\t\t\twantDone = 1\n\t\t\t\t}\n\t\t\t\tif mode == \"ordinary_child\" || mode == \"ordinary_root\" {\n\t\t\t\t\tif err := waitExactOriginPublications(t.Context(), publication, seed.EventID); err != nil {\n\t\t\t\t\t\tt.Fatal(err)\n\t\t\t\t\t}\n\t\t\t\t\tbefore := snapshotForkReceiverApplication(t, rt)\n\t\t\t\t\tresponse := requireServedJSONRPCError(t, rt.Endpoint, \"event.publish\", params)\n\t\t\t\t\tif response.Data[\"code\"] != apiv1.EventNotDeclaredCode {\n"},
		{"\t\t\t\tif mode == \"ambiguous_artifact\" {\n\t\t\t\t\t// Duplicate endpoint identity is rejected by canonical artifact\n\t\t\t\t\t// compilation, before a selectable artifact or fork can exist.\n\t\t\t\t\tbefore := snapshotForkReceiverApplication(t, rt)\n\t\t\t\t\tcandidate := canonicalrouting.CopySelectedForkPendingInput(t, canonicalrouting.PendingInputDuplicateEndpoint)\n\t\t\t\t\t_, err := contracts.LoadWorkflowContractBundleWithOverrides(repoRootForTest(), candidate, filepath.Join(repoRootForTest(), defaultPlatformSpecPath))\n", "\t\t\t\tif mode == \"ambiguous_artifact\" {\n\t\t\t\t\t// Duplicate endpoint identity is rejected by canonical artifact\n\t\t\t\t\t// compilation, before a selectable artifact or fork can exist.\n\t\t\t\t\tif err := waitExactOriginPublications(t.Context(), publication, seed.EventID, point.EventID); err != nil {\n\t\t\t\t\t\tt.Fatal(err)\n\t\t\t\t\t}\n\t\t\t\t\tbefore := snapshotForkReceiverApplication(t, rt)\n\t\t\t\t\tcandidate := canonicalrouting.CopySelectedForkPendingInput(t, canonicalrouting.PendingInputDuplicateEndpoint)\n\t\t\t\t\t_, err := contracts.LoadWorkflowContractBundleWithOverrides(repoRootForTest(), candidate, filepath.Join(repoRootForTest(), defaultPlatformSpecPath))\n"},
	}},
	{"TestSelectedForkPendingInputEvidenceBothStores", "c52dccdcde366fef321c3a5ff2c1b4497c244a30a9a0772e854b3569c57c6053", true, []publicationSuccessorEdit{
		{"\t\t\t\tprevious := projectRuntimePersistenceForServe\n\t\t\t\tprojectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence { selected = owner; return previous(owner) }\n\t\t\t\tt.Cleanup(func() { projectRuntimePersistenceForServe = previous })\n\t\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopySelectedForkPendingInput(t, canonicalrouting.PendingInputOriginal))\n\t\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\"event_name\": \"work.seeded\", \"bundle_hash\": rt.BundleHash, \"payload\": map[string]any{\"seed\": true}, \"idempotency_key\": \"seed\"})\n\t\t\t\twaitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, seed.EventID)\n", "\t\t\t\tprevious := projectRuntimePersistenceForServe\n\t\t\t\tprojectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence { selected = owner; return previous(owner) }\n\t\t\t\tt.Cleanup(func() { projectRuntimePersistenceForServe = previous })\n\t\t\t\tpublication := newExactOriginPublicationProbe(t)\n\t\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopySelectedForkPendingInput(t, canonicalrouting.PendingInputOriginal))\n\t\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\"event_name\": \"work.seeded\", \"bundle_hash\": rt.BundleHash, \"payload\": map[string]any{\"seed\": true}, \"idempotency_key\": \"seed\"})\n\t\t\t\twaitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, seed.EventID)\n"},
		{"\t\t\t\tif _, err := rt.DB.Exec(`UPDATE run_fork_fact_revisions SET fact=$1 WHERE run_id=$2 AND family='events' AND fact_key=$3 AND revision=$4`, string(changed), seed.RunID, point.EventID, revision); err != nil {\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}\n\t\t\t\tbefore := snapshotForkReceiverApplication(t, rt)\n\t\t\t\tdirect, ok := family.Availability().(forkHistoricalSelectedStore)\n\t\t\t\tif !ok {\n", "\t\t\t\tif _, err := rt.DB.Exec(`UPDATE run_fork_fact_revisions SET fact=$1 WHERE run_id=$2 AND family='events' AND fact_key=$3 AND revision=$4`, string(changed), seed.RunID, point.EventID, revision); err != nil {\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}\n\t\t\t\tif err := waitExactOriginPublications(t.Context(), publication, seed.EventID, point.EventID); err != nil {\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}\n\t\t\t\tbefore := snapshotForkReceiverApplication(t, rt)\n\t\t\t\tdirect, ok := family.Availability().(forkHistoricalSelectedStore)\n\t\t\t\tif !ok {\n"},
	}},
	{"runLifecycleRootForkPolicy", "78cc599416adac0be060849d04d71b6dc9c88b70ce48560a489d130a57ae5d67", true, []publicationSuccessorEdit{
		{"\t\t\tbefore := lifecycleStoredSnapshot(t, rt, seed.RunID)\n", rootForkPostCutBusinessSetup + "\t\t\tbefore := lifecycleStoredSnapshot(t, rt, seed.RunID)\n"},
		{"\tfor _, backend := range servedparity.RequiredBackends {\n\t\tt.Run(string(backend), func(t *testing.T) {\n\t\t\troot := canonicalrouting.CopyLifecycleForkSource(t, gate)\n\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)\n\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\"event_name\": \"work.requested\", \"bundle_hash\": rt.BundleHash, \"payload\": map[string]any{\"seed\": true}, \"idempotency_key\": \"fork-seed\"})\n\t\t\tstage := \"waiting\"\n\t\t\tif gate {\n\t\t\t\tstage = \"review\"\n", "\tfor _, backend := range servedparity.RequiredBackends {\n\t\tt.Run(string(backend), func(t *testing.T) {\n\t\t\troot := canonicalrouting.CopyLifecycleForkSource(t, gate)\n\t\t\tpublication := newExactOriginPublicationProbe(t)\n\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)\n\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\"event_name\": \"work.requested\", \"bundle_hash\": rt.BundleHash, \"payload\": map[string]any{\"seed\": true}, \"idempotency_key\": \"fork-seed\"})\n\t\t\torigins := []string{seed.EventID}\n\t\t\tstage := \"waiting\"\n\t\t\tif gate {\n\t\t\t\tstage = \"review\"\n"},
		{"\t\t\tif gate {\n\t\t\t\tparentCard = lifecycleGateDecisionParams(t, rt, seed.RunID, \"approve\")\n\t\t\t} else {\n\t\t\t\trequireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\"event_name\": \"loop.start\", \"run_id\": seed.RunID, \"source_event_id\": seed.EventID, \"payload\": map[string]any{\"seed\": true}, \"idempotency_key\": \"fork-loop-start\"})\n\t\t\t\tstage = \"drafting\"\n\t\t\t\trequireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, stage)\n\t\t\t}\n", "\t\t\tif gate {\n\t\t\t\tparentCard = lifecycleGateDecisionParams(t, rt, seed.RunID, \"approve\")\n\t\t\t} else {\n\t\t\t\tloopStart := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\"event_name\": \"loop.start\", \"run_id\": seed.RunID, \"source_event_id\": seed.EventID, \"payload\": map[string]any{\"seed\": true}, \"idempotency_key\": \"fork-loop-start\"})\n\t\t\t\torigins = append(origins, loopStart.EventID)\n\t\t\t\tstage = \"drafting\"\n\t\t\t\trequireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, stage)\n\t\t\t}\n"},
		{"\t\t\t}\n\t\t\tfrontier := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\"event_name\": frontierEvent, \"run_id\": seed.RunID, \"source_event_id\": seed.EventID, \"payload\": frontierPayload, \"idempotency_key\": \"fork-frontier\"})\n\t\t\twaitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, frontier.EventID)\n\t\t\tbefore := lifecycleStoredSnapshot(t, rt, seed.RunID)\n\t\t\tparams := map[string]any{\"source_run_id\": seed.RunID, \"fork_event_id\": frontier.EventID, \"allow_source_freeze\": true, \"idempotency_key\": \"lifecycle-fork\"}\n\t\t\tvar fork, duplicate apiv1.RunForkExecutionResult\n", "\t\t\t}\n\t\t\tfrontier := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\"event_name\": frontierEvent, \"run_id\": seed.RunID, \"source_event_id\": seed.EventID, \"payload\": frontierPayload, \"idempotency_key\": \"fork-frontier\"})\n\t\t\twaitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, frontier.EventID)\n\t\t\torigins = append(origins, frontier.EventID)\n\t\t\tif err := waitExactOriginPublications(t.Context(), publication, origins...); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tbefore := lifecycleStoredSnapshot(t, rt, seed.RunID)\n\t\t\tparams := map[string]any{\"source_run_id\": seed.RunID, \"fork_event_id\": frontier.EventID, \"allow_source_freeze\": true, \"idempotency_key\": \"lifecycle-fork\"}\n\t\t\tvar fork, duplicate apiv1.RunForkExecutionResult\n"},
	}},
	{"TestServedCompiledTransitionStaticForkEvidenceOnBothStores", "6df62196f3d2ad74314adb1131dd96ce73010dd1aabfe1bd2ff7232a3af0abc5", true, []publicationSuccessorEdit{
		{"selected.DisplayLabel() != \"alpha\" ||", "selected.DisplayLabel() != \"rules[0]: when payload.choice == 'alpha'\" ||"},
		{"func TestServedCompiledTransitionStaticForkEvidenceOnBothStores(t *testing.T) {\n\tfor _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {\n\t\tt.Run(string(backend), func(t *testing.T) {\n\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyLifecycleSelectedCarriers(t, false))\n\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n\t\t\t\t\"event_name\": \"work.seeded\", \"bundle_hash\": rt.BundleHash, \"payload\": map[string]any{\"seed\": true}, \"idempotency_key\": \"static-fork-seed\",\n", "func TestServedCompiledTransitionStaticForkEvidenceOnBothStores(t *testing.T) {\n\tfor _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {\n\t\tt.Run(string(backend), func(t *testing.T) {\n\t\t\tpublication := newExactOriginPublicationProbe(t)\n\t\t\trt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyLifecycleSelectedCarriers(t, false))\n\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\n\t\t\t\t\"event_name\": \"work.seeded\", \"bundle_hash\": rt.BundleHash, \"payload\": map[string]any{\"seed\": true}, \"idempotency_key\": \"static-fork-seed\",\n"},
		{"\t\t\t\t\"event_name\": \"work.first\", \"run_id\": seed.RunID, \"payload\": map[string]any{\"choice\": \"alpha\"}, \"idempotency_key\": \"static-fork-frontier\",\n\t\t\t})\n\t\t\twaitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, frontier.EventID)\n\t\t\tbefore := lifecycleStoredSnapshot(t, rt, seed.RunID)\n\t\t\tparams := map[string]any{\"source_run_id\": seed.RunID, \"fork_event_id\": frontier.EventID, \"allow_source_freeze\": true, \"idempotency_key\": \"static-fork\"}\n\t\t\tfork := requireSelectedForkExecutionRPCResult(t, rt.Endpoint, params)\n", "\t\t\t\t\"event_name\": \"work.first\", \"run_id\": seed.RunID, \"payload\": map[string]any{\"choice\": \"alpha\"}, \"idempotency_key\": \"static-fork-frontier\",\n\t\t\t})\n\t\t\twaitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, frontier.EventID)\n\t\t\tif err := waitExactOriginPublications(t.Context(), publication, seed.EventID, frontier.EventID); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tbefore := lifecycleStoredSnapshot(t, rt, seed.RunID)\n\t\t\tparams := map[string]any{\"source_run_id\": seed.RunID, \"fork_event_id\": frontier.EventID, \"allow_source_freeze\": true, \"idempotency_key\": \"static-fork\"}\n\t\t\tfork := requireSelectedForkExecutionRPCResult(t, rt.Endpoint, params)\n"},
	}},
	{"TestServedCompiledTransitionTemplateSiblingForkCapabilityRefusalOnBothStores", "df45d133f38aef275c97c0f9ef43bea14af5494339d2d46c1739cdd753b9b630", true, []publicationSuccessorEdit{
		{"func TestServedCompiledTransitionTemplateSiblingForkCapabilityRefusalOnBothStores(t *testing.T) {\n\tfor _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {\n\t\tt.Run(string(backend), func(t *testing.T) {\n\t\t\trt := startLifecycleTemplateRuntime(t, backend, canonicalrouting.CopyLifecycleNestedTemplates(t))\n\t\t\trunID, sourceEvent, siblings := prepareLifecycleTemplateSiblings(t, rt)\n\t\t\twaitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, runID)\n\t\t\trequireServedOKJSONRPC(t, rt.Endpoint, \"run.pause\", map[string]any{\"run_id\": runID, \"idempotency_key\": \"template-pause\"})\n\t\t\tfrontier := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\"event_name\": \"work.observed\", \"run_id\": runID, \"source_event_id\": sourceEvent, \"payload\": map[string]any{\"seed\": true}, \"idempotency_key\": \"template-frontier\"})\n\t\t\twaitServedEventPublishedLog(t, rt.Endpoint, runID, frontier.EventID)\n\t\t\tbefore := snapshotForkReceiverApplication(t, rt)\n\t\t\tparams := map[string]any{\"source_run_id\": runID, \"fork_event_id\": frontier.EventID, \"allow_source_freeze\": true, \"idempotency_key\": \"template-fork\"}\n\t\t\tfor attempt := 0; attempt < 2; attempt++ {\n", "func TestServedCompiledTransitionTemplateSiblingForkCapabilityRefusalOnBothStores(t *testing.T) {\n\tfor _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {\n\t\tt.Run(string(backend), func(t *testing.T) {\n\t\t\tpublication := newExactOriginPublicationProbe(t)\n\t\t\trt := startLifecycleTemplateRuntime(t, backend, canonicalrouting.CopyLifecycleNestedTemplates(t))\n\t\t\trunID, sourceEvent, siblings := prepareLifecycleTemplateSiblings(t, rt)\n\t\t\twaitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, runID)\n\t\t\trequireServedOKJSONRPC(t, rt.Endpoint, \"run.pause\", map[string]any{\"run_id\": runID, \"idempotency_key\": \"template-pause\"})\n\t\t\tfrontier := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\"event_name\": \"work.observed\", \"run_id\": runID, \"source_event_id\": sourceEvent, \"payload\": map[string]any{\"seed\": true}, \"idempotency_key\": \"template-frontier\"})\n\t\t\twaitServedEventPublishedLog(t, rt.Endpoint, runID, frontier.EventID)\n\t\t\torigins := []string{frontier.EventID}\n\t\t\tfor _, sibling := range siblings {\n\t\t\t\tif len(sibling.publications) != 5 {\n\t\t\t\t\tt.Fatalf(\"sibling %s did not retain all five acknowledged commands: %v\", sibling.side, sibling.publications)\n\t\t\t\t}\n\t\t\t\torigins = append(origins, sibling.publications...)\n\t\t\t}\n\t\t\tif err := waitExactOriginPublications(t.Context(), publication, origins...); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tbefore := snapshotForkReceiverApplication(t, rt)\n\t\t\tparams := map[string]any{\"source_run_id\": runID, \"fork_event_id\": frontier.EventID, \"allow_source_freeze\": true, \"idempotency_key\": \"template-fork\"}\n\t\t\tfor attempt := 0; attempt < 2; attempt++ {\n"},
	}},
	{"mailboxCompletionNotice", "c59b4b333f04bc91ed0c253e961272a73bef041cccb8832dd33e1a4f061a4319", false, []publicationSuccessorEdit{
		{"func mailboxCompletionNotice(t *testing.T, f cursorMailboxFixture) string {\n\tt.Helper()\n\tseed := requireServedEventPublishRPCResult(t, f.rt.Endpoint, map[string]any{\"event_name\": \"notice.requested\", \"run_id\": f.base.RunID, \"source_event_id\": f.eventID, \"payload\": map[string]any{\"seed\": true}, \"idempotency_key\": \"notice-seed-\" + uuid.NewString()})\n\twaitServedRunDeliveryQuiescence(t, f.rt.ReadRunDeliveries, f.base.RunID)\n", "func mailboxCompletionNotice(t *testing.T, f cursorMailboxFixture, publicationOut ...*servedEventPublishRPCResult) string {\n\tt.Helper()\n\tseed := requireServedEventPublishRPCResult(t, f.rt.Endpoint, map[string]any{\"event_name\": \"notice.requested\", \"run_id\": f.base.RunID, \"source_event_id\": f.eventID, \"payload\": map[string]any{\"seed\": true}, \"idempotency_key\": \"notice-seed-\" + uuid.NewString()})\n\twaitServedRunDeliveryQuiescence(t, f.rt.ReadRunDeliveries, f.base.RunID)\n"},
		{"\tif summary != \"Observed notice\" || from != \"observer\" {\n\t\tt.Fatalf(\"wrong real notice producer: summary=%q from=%q\", summary, from)\n\t}\n\treturn id\n}", "\tif summary != \"Observed notice\" || from != \"observer\" {\n\t\tt.Fatalf(\"wrong real notice producer: summary=%q from=%q\", summary, from)\n\t}\n\tfor _, out := range publicationOut {\n\t\t*out = seed\n\t}\n\treturn id\n}"},
	}},
}

func TestExactOriginPublicationSuccessorsPreserveReviewedCuts(t *testing.T) {
	for _, target := range exactOriginSuccessors {
		t.Run(target.name, func(t *testing.T) {
			var rows []recipe
			if err := json.Unmarshal(recipeBytes, &rows); err != nil {
				t.Fatal(err)
			}
			var selected *recipe
			for i := range rows {
				if rows[i].Function == target.name {
					if selected != nil {
						t.Fatal("ambiguous originating-publication row")
					}
					selected = &rows[i]
				}
			}
			if selected == nil || selected.Successor == "" || selected.Removed || selected.Mechanical != nil || selected.Family != "native-served-delivery-quiescence-caller" {
				t.Fatal("originating-publication row changed historical migration identity")
			}
			actual := selectedCausalObservationBody(t, selected.File, selected.Function)
			currentRow := *selected
			prior, actual := requireCatchUp2672MasterPair(t, currentRow, actual)
			selected = &prior
			got, err := canonicalFunction(actual)
			want, wantErr := canonicalFunction(selected.Successor)
			if err != nil || wantErr != nil || got != want {
				t.Fatal("current source differs from reviewed successor")
			}
			restored := selected.Successor
			if target.name == "TestSelectedForkReceiverAcquisitionCapabilityRefusalBothStores" {
				// A48d11e6069, reviewed in6174528631: failure-only logging;
				// removing that exact line must recover the prior full successor.
				const diagnostic = "\t\t\t\t\tt.Logf(\"capability refusal snapshot delta:\\n%s\", forkReceiverCapabilityRefusalSnapshotDelta(before, after))\n"
				if strings.Count(restored, diagnostic) != 1 {
					t.Fatal("missing or repeated reviewed capability diagnostic")
				}
				restored = strings.Replace(restored, diagnostic, "", 1)
				canonical, err := canonicalFunction(restored)
				if err != nil || fmt.Sprintf("%x", sha256.Sum256([]byte(canonical))) != "07154c247422a609a09000b3d9fd44b893f5431f2c246586df98f6d2106b34d5" {
					t.Fatal("capability diagnostic changed its exact prior successor")
				}
				const failure = "\t\t\t\tif after := snapshotForkReceiverApplication(t, rt); !reflect.DeepEqual(before, after) {\n"
				mutants := map[string]string{
					"missing_diagnostic":   restored,
					"duplicate_diagnostic": strings.Replace(actual, diagnostic, diagnostic+diagnostic, 1),
					"outside_failure":      strings.Replace(restored, failure, diagnostic+failure, 1),
					"changed_diagnostic":   strings.Replace(actual, diagnostic, strings.Replace(diagnostic, "(before, after)", "(after, before)", 1), 1),
				}
				for name, mutant := range mutants {
					t.Run(name, func(t *testing.T) {
						if mutant == actual {
							t.Fatal("capability diagnostic hostile control did not change source")
						}
						guard := *selected
						guard.Before, guard.After, guard.Successor = actual, actual, ""
						if _, changed, err := rewriteFunction(guard.File, []byte("package probe\n"+mutant), guard); err == nil || changed {
							t.Fatal("unreviewed capability diagnostic accepted")
						}
					})
				}
			}
			for name, mutant := range exactOriginOrderingMutants(t, actual) {
				t.Run(name, func(t *testing.T) {
					if mutant == actual {
						t.Fatal("hostile ordering control did not change source")
					}
					guard := *selected
					guard.Before, guard.After, guard.Successor = actual, actual, ""
					if _, changed, err := rewriteFunction(guard.File, []byte("package probe\n"+mutant), guard); err == nil || changed {
						t.Fatal("changed publication ordering or business assertion accepted")
					}
				})
			}
			for index, change := range target.edits {
				if strings.Count(restored, change.after) != 1 {
					t.Fatal("ambiguous or missing finite publication edit")
				}
				restored = strings.Replace(restored, change.after, change.before, 1)
				mutationSource := actual
				if target.name == "runLifecycleRootForkPolicy" && index > 0 {
					// The setup overlaps the older snapshot edit. Remove it only
					// while deriving that edit's unchanged negative controls.
					mutationSource = strings.Replace(actual, rootForkPostCutBusinessSetup, "", 1)
				}
				mutants := map[string]string{
					"missing":        strings.Replace(mutationSource, change.after, change.before, 1),
					"duplicate":      strings.Replace(mutationSource, change.after, change.after+change.after, 1),
					"wrong_identity": strings.Replace(mutationSource, change.after, strings.ReplaceAll(change.after, ".EventID", ".RunID"), 1),
				}
				if strings.Contains(change.after, "origins...") {
					mutants["wrong_origin_set"] = strings.Replace(mutationSource, change.after, strings.ReplaceAll(change.after, "origins...", "[]string{\"foreign\"}..."), 1)
				}
				for name, mutant := range mutants {
					label := fmt.Sprintf("edit_%d/%s", index, name)
					if target.name == "runLifecycleRootForkPolicy" {
						if index == 0 {
							label = "source_history/" + name
						} else {
							const snapshot = "\t\t\tbefore := lifecycleStoredSnapshot(t, rt, seed.RunID)\n"
							mutant = strings.Replace(mutant, snapshot, rootForkPostCutBusinessSetup+snapshot, 1)
							label = fmt.Sprintf("edit_%d/%s", index-1, name)
						}
					}
					if mutant == actual {
						continue
					}
					t.Run(label, func(t *testing.T) {
						guard := *selected
						guard.Before, guard.After, guard.Successor = actual, actual, ""
						if _, changed, err := rewriteFunction(guard.File, []byte("package probe\n"+mutant), guard); err == nil || changed {
							t.Fatal("hostile originating-publication edit accepted")
						}
					})
				}
			}
			canonical, err := canonicalFunction(restored)
			if err != nil || fmt.Sprintf("%x", sha256.Sum256([]byte(canonical))) != target.predecessor {
				t.Fatal("publication successor changed setup, held work or business/refusal assertions beyond finite wiring")
			}
			predecessor, err := canonicalFunction(selected.After)
			if !target.priorSuccessor && (err != nil || predecessor != canonical) {
				t.Fatal("historical After changed")
			}
			if files, changes, err := prepareFiles("../../..", []recipe{currentRow}); err != nil || len(files) != 0 || len(changes) != 0 {
				t.Fatalf("current publication successor is not inert: %v", err)
			}
		})
	}
}

func exactOriginOrderingMutants(t *testing.T, source string) map[string]string {
	t.Helper()
	if strings.HasPrefix(source, "func TestServedCompiledTransitionSelectedCarrierEvidenceOnBothStores(") {
		const gate = "\t\t\t\t\t\t\tif err := waitLifecycleCarrierSeedSettlement(t.Context(), probe, seed); err != nil {\n\t\t\t\t\t\t\t\tt.Fatal(err)\n\t\t\t\t\t\t\t}\n"
		const setup = "\t\t\t\tprobe := newExactOriginPublicationProbe(t)\n"
		const selected = "\t\t\t\t\t\t\tselected := requireServedEventPublishRPCResult(t, rt.Endpoint, params)\n"
		if strings.Count(source, gate) != 1 || strings.Count(source, setup) != 1 || strings.Count(source, selected) != 1 {
			t.Fatal("missing exact selected-carrier prerequisite")
		}
		withoutGate := strings.Replace(source, gate, "", 1)
		withoutSetup := strings.Replace(source, setup, "", 1)
		return map[string]string{
			"wait_after_selected":     strings.Replace(withoutGate, selected, selected+gate, 1),
			"omitted_seed":            withoutGate,
			"wrong_seed":              strings.Replace(source, "waitLifecycleCarrierSeedSettlement(t.Context(), probe, seed)", "waitLifecycleCarrierSeedSettlement(t.Context(), probe, selected)", 1),
			"late_probe_installation": strings.Replace(withoutSetup, gate, setup+gate, 1),
			"business_assertion":      strings.Replace(source, "history[0].From != \"active\"", "history[0].From != \"waiting\"", 1),
			"unrelated_setup":         strings.Replace(source, "{\n", "{\n\tunreviewedSetup()\n", 1),
		}
	}
	if strings.HasPrefix(source, "func mailboxCompletionNotice(") {
		return map[string]string{
			"omitted_receipt":    strings.Replace(source, "*out = seed", "*out = servedEventPublishRPCResult{}", 1),
			"unrelated_receipt":  strings.Replace(source, "*out = seed", "*out = servedEventPublishRPCResult{EventID: f.eventID}", 1),
			"business_assertion": strings.Replace(source, "from != \"observer\"", "from != \"foreign\"", 1),
		}
	}
	const prefix = "package probe\n"
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "successor.go", prefix+source, 0)
	if err != nil {
		t.Fatal(err)
	}
	var gate *ast.IfStmt
	var snapshot *ast.AssignStmt
	var originCall *ast.CallExpr
	ast.Inspect(file, func(node ast.Node) bool {
		if statement, ok := node.(*ast.IfStmt); ok && gate == nil {
			if assignment, ok := statement.Init.(*ast.AssignStmt); ok && len(assignment.Rhs) == 1 {
				if call, ok := assignment.Rhs[0].(*ast.CallExpr); ok {
					if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "waitExactOriginPublications" {
						gate, originCall = statement, call
					}
				}
			}
		}
		if assignment, ok := node.(*ast.AssignStmt); ok && gate != nil && snapshot == nil && assignment.Pos() > gate.End() {
			for _, expression := range assignment.Rhs {
				call, ok := expression.(*ast.CallExpr)
				if !ok {
					continue
				}
				if name, ok := call.Fun.(*ast.Ident); ok {
					switch name.Name {
					case "readServedForkRecipientSourceDomain", "snapshotForkReceiverApplication", "lifecycleStoredSnapshot":
						snapshot = assignment
					}
				}
			}
		}
		return true
	})
	if gate == nil || snapshot == nil || len(originCall.Args) < 3 {
		t.Fatal("missing exact origin gate and its dependent snapshot")
	}
	offset := func(position token.Pos) int { return set.Position(position).Offset - len(prefix) }
	start, end, observed := offset(gate.Pos()), offset(gate.End()), offset(snapshot.End())
	mutants := map[string]string{
		"wait_after_snapshot": source[:start] + source[end:observed] + "\n" + source[start:end] + source[observed:],
		"unrelated_setup":     strings.Replace(source, "{\n", "{\n\tunreviewedSetup()\n", 1),
		"business_assertion":  strings.Replace(source, "t.Fatal(", "t.Skip(", 1),
	}
	callStart, callEnd := offset(originCall.Pos()), offset(originCall.End())
	ctx, probe := originCall.Args[0], originCall.Args[1]
	withoutOrigins := "waitExactOriginPublications(" + source[offset(ctx.Pos()):offset(ctx.End())] + ", " + source[offset(probe.Pos()):offset(probe.End())] + ")"
	mutants["omitted_required_origin"] = source[:callStart] + withoutOrigins + source[callEnd:]
	if strings.Contains(source, "publication := newExactOriginPublicationProbe(t)\n") {
		const setup = "publication := newExactOriginPublicationProbe(t)\n"
		without := strings.Replace(source, setup, "", 1)
		mutants["late_probe_installation"] = strings.Replace(without, source[start:end], setup+source[start:end], 1)
	}
	if strings.Contains(source, "releaseOnce.Do(func() { close(release) })") {
		mutants["early_held_worker_release"] = source[:start] + "releaseOnce.Do(func() { close(release) })\n" + source[start:]
	}
	return mutants
}

func reviewedMilestoneRow(t *testing.T, row recipe) recipe {
	t.Helper()
	actual := selectedCausalObservationBody(t, row.File, row.Function)
	row, _, err := catchUp2672MasterPair(row, actual)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range exactOriginSuccessors {
		if target.name != row.Function || !target.priorSuccessor {
			continue
		}
		for _, change := range target.edits {
			if strings.Count(row.Successor, change.after) != 1 {
				t.Fatal("missing exact-origin amendment in milestone composition")
			}
			row.Successor = strings.Replace(row.Successor, change.after, change.before, 1)
		}
		canonical, err := canonicalFunction(row.Successor)
		if err != nil || fmt.Sprintf("%x", sha256.Sum256([]byte(canonical))) != target.predecessor {
			t.Fatal("milestone composition lost its reviewed predecessor")
		}
	}
	return row
}
