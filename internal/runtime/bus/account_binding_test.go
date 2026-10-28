package bus

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestAuthenticatedAccountSelectsOnlyItsConstructedHolder(t *testing.T) {
	for _, provider := range []string{"telegram", "whatsapp"} {
		t.Run(provider, func(t *testing.T) {
			accounts := []string{"420079", "420080", "420081"}
			eventName := "text_message"
			if provider == "whatsapp" {
				accounts, eventName = []string{"+15550100", "+15550200", "+15550300"}, "message"
			}
			flows := []string{"fixed", "keyed", "dynamic"}
			members := map[string][]byte{"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
				"schema.yaml": []byte("stages:\n  active: {}\n"), "entities.yaml": []byte("root_state: {}\n")}
			for index, flow := range flows {
				members[flow+"/manifest.yaml"] = []byte("version: 1.0.0\nplatform_version: '*'\n")
				members[flow+"/schema.yaml"] = []byte("stages:\n  active: {}\npins:\n  inputs: [inbound." + provider + "." + eventName + "]\n")
				members[flow+"/entities.yaml"] = []byte("desk_state: {}\n")
				if flow == "fixed" {
					members[flow+"/policy.yaml"] = []byte(fmt.Sprintf("operator_account: %q\n", accounts[index]))
				} else {
					members[flow+"/entities.yaml"] = []byte("desk_state:\n  operator_account: text\n")
					if flow == "keyed" {
						members[flow+"/schema.yaml"] = append(members[flow+"/schema.yaml"], []byte("instance: operator_account\n")...)
					}
				}
				for _, role := range []string{"manifest", "trigger", "connector", "channel"} {
					body, err := os.ReadFile(filepath.Join("..", "..", "..", "packs", "providers", provider, role+".yaml"))
					if err != nil {
						t.Fatal(err)
					}
					members[flow+"/"+provider+"/"+role+".yaml"] = body
				}
			}
			artifact, err := sourceartifact.AdmitMembers(members)
			if err != nil {
				t.Fatal(err)
			}
			rooted, _ := sourceartifactfixture.Rooted(t, "two-accounts", artifact)
			bundle, err := contracts.LoadWorkflowContractBundleFromRootedSource(filepath.Join("..", "..", ".."), rooted, "", contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
			if err != nil {
				t.Fatal(err)
			}
			source := semanticview.Wrap(bundle)
			runID := eventtest.UUID("account-selection:" + provider)
			root := flowidentity.Stored(source, ".", runID, runID, runID, "")
			reader := constructionIndexTestReader{}
			for index, flow := range flows {
				var identity flowidentity.Instance
				key := ""
				if flow == "keyed" {
					key = accounts[index]
					identity, err = flowidentity.KeyedChild(source, root, flow, key)
				} else {
					identity, err = flowidentity.KeylessChild(source, root, flow)
				}
				if err != nil {
					t.Fatal(err)
				}
				reader.observations = append(reader.observations, constructionIndexObservation(t, source, runID, identity, key))
			}
			observe := func(original pipeline.FlowInstanceObservation, fields map[string]any, terminated bool) pipeline.FlowInstanceObservation {
				instance, err := original.WorkflowInstance()
				if err != nil {
					t.Fatal(err)
				}
				instance.Fields = fields
				if terminated {
					instance.TerminatedAt = instance.UpdatedAt
				}
				receipt, _, err := original.NativeConstruction()
				if err != nil {
					t.Fatal(err)
				}
				readiness, _ := original.Readiness()
				readiness.InstanceTerminatedAt = instance.TerminatedAt
				run := runlifecycle.Snapshot{RunID: runID, State: runlifecycle.StateRunning, Origin: runlifecycle.DeploymentRunOrigin(),
					BundleHash: original.SourceFact().BundleHash(), RootReference: rooted.Admission().Reference(), StartedAt: instance.CreatedAt}
				observed, err := pipeline.AdmitNativeFlowInstanceObservation(original.Selection(), instance, run, original.RunRevision(), receipt, readiness)
				if err != nil {
					t.Fatal(err)
				}
				return observed
			}
			for index := range reader.observations {
				reader.observations[index] = observe(reader.observations[index], map[string]any{"operator_account": accounts[index]}, false)
			}
			for index, flow := range flows {
				routing, err := events.NewExternalIngressRoutingSource(flow, events.RoutingSourceAuthorityProviderAdmissionPlan)
				if err != nil {
					t.Fatal(err)
				}
				event := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID("account-input:"+flow),
					events.EventType("inbound."+provider+"."+eventName), "", "", nil, 0, runID, events.EventEnvelope{},
					routing, time.Now().UTC())
				for _, account := range accounts {
					for _, kind := range []provideroutput.Kind{provideroutput.KindRaw, provideroutput.KindNormalized} {
						ctx := context.WithValue(constructionIndexContext(t, source), authenticatedProviderPublicationKey{},
							authenticatedProviderPublication{eventID: event.ID(), source: event.RoutingSource().Route(), kind: kind, account: account})
						projection := selectedRunTargetOwnerProjection{source: source, context: ctx, instances: reader}
						selected, scoped, err := projection.selectedAccountInstance(ctx, event)
						want := flowidentity.Instance{}
						if account == accounts[index] {
							want = reader.observations[index].Identity()
						}
						if err != nil || !scoped || selected != want {
							t.Fatalf("flow=%s account=%s selection=%+v scoped=%t err=%v", flow, account, selected, scoped, err)
						}
						projection.instances = constructionIndexTestReader{}
						if selected, _, err := projection.selectedAccountInstance(ctx, event); err != nil || selected != (flowidentity.Instance{}) {
							t.Fatalf("unconstructed account acquired a receiver: %+v %v", selected, err)
						}
						projection.instances = constructionIndexTestReader{observations: append([]pipeline.FlowInstanceObservation{reader.observations[index]}, reader.observations[index])}
						if account == accounts[index] {
							if _, _, err := projection.selectedAccountInstance(ctx, event); err == nil {
								t.Fatal("competing account holders were broadcast or first-selected")
							}
							for _, terminated := range []bool{false, true} {
								fields := map[string]any{"operator_account": account}
								if !terminated {
									if flow != "dynamic" {
										continue
									}
									fields = nil
								}
								projection.instances = constructionIndexTestReader{observations: []pipeline.FlowInstanceObservation{observe(reader.observations[index], fields, terminated)}}
								if selected, _, err := projection.selectedAccountInstance(ctx, event); err != nil || selected != (flowidentity.Instance{}) {
									t.Fatalf("unassigned/terminated account acquired a receiver: terminated=%t selected=%+v err=%v", terminated, selected, err)
								}
							}
						}
					}
				}
			}
		})
	}
}
