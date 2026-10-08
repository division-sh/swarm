package serveapp

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestA9ReleaseKeyedIngressConstructionBothStores(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "swarm")
	build := exec.Command("go", "build", "-race", "-o", binary, "./cmd/swarm")
	build.Dir = repoRootForTest()
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build A9 release executable: %v\n%s", err, output)
	}
	for _, backend := range servedparity.RequiredBackends {
		for _, nested := range []bool{false, true} {
			name := "root_creation"
			if nested {
				name = "nested_declaration"
			}
			t.Run(string(backend)+"/"+name, func(t *testing.T) {
				secret := a9SetRawIngressCredential(t)
				var root, alias string
				if nested {
					root, alias = canonicalrouting.CopyNestedDeclarationLocalRawIngress(t), "nested-shop.branch"
				} else {
					root, alias = canonicalrouting.CopyKeyedRootRawIngressCreationEvent(t), "keyed-shop"
				}
				rt := startLifecycleReleaseProcess(t, binary, backend, root, "SWARM_CREDENTIALS_FILE="+os.Getenv("SWARM_CREDENTIALS_FILE"))
				endpoint := strings.TrimSuffix(rt.Endpoint, "/v1/rpc") + "/webhooks/" + alias + "/partner"
				const body = `{"delivery_id":"compiled-binary"}`
				code, raw := a9PostSignedRawIngress(t, endpoint, secret, body)
				if code != 202 {
					t.Fatalf("release ingress=%d %s", code, raw)
				}
				receipt := a9AliasReceipt(t, raw)
				runID, ok := receipt["run_id"].(string)
				ids, eventsOK := receipt["event_ids"].([]any)
				if !ok || !eventsOK || len(ids) != 1 {
					t.Fatalf("release receipt=%+v", receipt)
				}
				id, ok := ids[0].(string)
				if !ok {
					t.Fatal("release receipt omitted event identity")
				}
				if nested {
					event := a9RequireNestedIngressSettlement(t, rt.Endpoint, id)
					for _, delivery := range event.Deliveries {
						if delivery.Target.FlowID != "branch/parent" && delivery.Target.FlowID != "branch/parent/middle/leaf" {
							t.Fatalf("release delivery escaped nested declaration scope: %+v", event)
						}
						var entity operatorread.OperatorEntityFull
						requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": delivery.Target.EntityID}, &entity)
						if entity.Fields["seen"] != float64(1) {
							t.Fatalf("release nested consumer=%+v", entity)
						}
					}
				} else {
					a9RequireRootIngressSettlement(t, rt.Endpoint, runID, id)
					created := servedClockEvents(t, rt.Endpoint, runID, "root.created")
					if len(created) != 1 || created[0].SourceEventID != id {
						t.Fatalf("release creation publication=%+v", created)
					}
					a9RequireRootIngressSettlement(t, rt.Endpoint, runID, created[0].EventID)
					var entity operatorread.OperatorEntityFull
					requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": runID}, &entity)
					if entity.Fields["processed_count"] != float64(1) || entity.Fields["creation_count"] != float64(1) {
						t.Fatalf("release root consumer=%+v", entity)
					}
				}
				code, duplicate := a9PostSignedRawIngress(t, endpoint, secret, body)
				if code != 200 || !reflect.DeepEqual(receipt, a9AliasReceipt(t, duplicate)) {
					t.Fatalf("release duplicate lost receipt=%d %s", code, duplicate)
				}
				t.Log("proof_surface=race-instrumented public verify/serve binary, authenticated webhook, HTTP event/entity readback, exact receipt retry, joined process shutdown; not retained-source restart or live-provider qualification")
			})
		}
	}
}
