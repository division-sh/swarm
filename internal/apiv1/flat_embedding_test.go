package apiv1

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestFlatEmbeddingPublicEventPublicationBothStores(t *testing.T) {
	root := t.TempDir()
	for name, body := range map[string]string{
		"schema.yaml": "name: review\npins:\n  inputs: [work.ready]\n",
		"types.yaml":  "types:\n  Marker:\n    marker: {type: text, pattern: '^[a-z]+$'}\n    key: text\n    description: text\n",
		"events.yaml": "work.ready:\n  _description: Embedded public input\n  _key: marker\n  _embed: [Marker]\n",
		"nodes.yaml":  "observer:\n  execution_type: system_node\n  subscribes_to: [work.ready]\n  event_handlers:\n    work.ready:\n      guard: {id: received, check: payload.marker != ''}\n",
	} {
		writeRunCompletionFixtureFile(t, filepath.Join(root, name), body)
	}
	repo := filepath.Join("..", "..")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			var selected canonicalEventPublishProofStore
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
			} else {
				selected = storetest.StartPostgresRuntimeStore(t)
			}
			bus, err := newScopedAPITestEventBus(t, selected, runStartTestEventBusOptions(source))
			if err != nil {
				t.Fatal(err)
			}
			handler := eventPublishTestHandlerWithStores(t, selected, selected, selected, bus, source)
			before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			for _, payload := range []string{
				`{"marker":1,"key":"business","description":"business"}`,
				`{"marker":"123","key":"business","description":"business"}`,
				`{"marker":"ready","description":"business"}`,
				`{"marker":"ready","key":"business","description":"business","extra":true}`,
			} {
				response := rpcCall(t, handler, eventPublishBody("", bundle.SourceArtifact.BundleHash(), "work.ready", payload, "", "embedded-invalid"))
				if response.Error == nil || asMap(t, response.Error.Data)["code"] != PayloadValidationFailedCode {
					t.Fatalf("invalid embedded public payload accepted/refused elsewhere: payload=%s response=%+v", payload, response)
				}
				after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("invalid public payload changed application storage: err=%v before=%+v after=%+v", err, before, after)
				}
			}
			response := rpcCall(t, handler, eventPublishBody("", bundle.SourceArtifact.BundleHash(), "work.ready", `{"marker":"ready","key":"ordinary-key","description":"ordinary-description"}`, "", "embedded-valid"))
			if response.Error != nil {
				t.Fatalf("valid embedded input: %+v", response.Error)
			}
			result := asMap(t, response.Result)
			id := stringValue(t, result["event_id"], "embedded event_id")
			run := stringValue(t, result["run_id"], "embedded run_id")
			prepared, found, err := selected.(runtimebus.PreparedPublishEventReader).LoadPreparedPublishEvent(ctx, id)
			if err != nil || !found {
				t.Fatalf("missing public input: found=%t err=%v", found, err)
			}
			event := prepared.Event.Event()
			var payload map[string]any
			if err := json.Unmarshal(event.Payload(), &payload); err != nil || payload["key"] != "ordinary-key" || payload["description"] != "ordinary-description" || event.EntityID() != runtimeflowidentity.EntityID(run) {
				t.Fatalf("business fields changed envelope identity: %+v payload=%+v err=%v", event, payload, err)
			}
		})
	}
}
