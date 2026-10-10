package pipeline_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func workflowHandlerNativeFixture(t *testing.T, backend string, source semanticview.Source, mutations, claims uint64) pipeline.WorkflowHandlerNativeFixtureForTest {
	t.Helper()
	fixture, _, _, probe := openWorkflowHandlerNativeFixture(t, backend, source)
	t.Cleanup(func() {
		counts := probe.Snapshot()
		if counts.ByOperation[storetest.TransactionWorkflowMutation].WriteCommits != mutations ||
			counts.ByOperation[storetest.TransactionDeliveryClaim].WriteCommits != claims || counts.Active != 0 {
			t.Errorf("handler cohort escaped selected mutation/claim ownership: %+v, want mutations=%d claims=%d", counts, mutations, claims)
		}
	})
	return fixture
}

func openWorkflowHandlerNativeFixture(t *testing.T, backend string, source semanticview.Source) (pipeline.WorkflowHandlerNativeFixtureForTest, timerReplaySelectedStore, func() timerReplaySelectedStore, *storetest.TransactionCollector) {
	t.Helper()
	selected, _, reopen := openTimerReplayNativeStore(t, backend)
	return workflowHandlerNativeFixtureFromSelected(t, source, selected, reopen)
}

func workflowHandlerNativeFixtureFromSelected(t *testing.T, source semanticview.Source, selected timerReplaySelectedStore, reopen func() timerReplaySelectedStore) (pipeline.WorkflowHandlerNativeFixtureForTest, timerReplaySelectedStore, func() timerReplaySelectedStore, *storetest.TransactionCollector) {
	t.Helper()
	bundle, found := semanticview.Bundle(source)
	if !found || bundle.SourceArtifact == nil {
		t.Fatal("handler fixture requires its exact admitted source artifact")
	}
	fact := sourceartifactfixture.FactFor(bundle.SourceArtifact)
	persistence := pipeline.NewWorkflowPersistence(selected)
	ctx := withLiveGateExecution(testAuthorActivityContextForSource(t, context.Background(), fact))
	probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
	t.Cleanup(func() {
		counts := probe.Snapshot()
		if counts.Active != 0 {
			t.Errorf("native handler fixture still has active selected transactions: %+v", counts)
		}
	})
	return pipeline.WorkflowHandlerNativeFixtureForTest{
		Persistence: persistence, Context: ctx,
		FreshProjection: func() pipeline.WorkflowPersistence {
			return pipeline.NewWorkflowPersistence(selected)
		},
		Runs: selected.(interface {
			runtimerunlifecycle.OperationOwner
			runtimerunlifecycle.CandidateStore
			LoadRunOrigin(context.Context, string) (runtimerunlifecycle.RunOrigin, error)
		}),
		RequireRun: func(ctx context.Context, run string) error {
			return storetest.MaterializeRun(ctx, selected, storetest.RunFixture{RunID: run, Origin: storetest.ScenarioSetupOrigin(), Artifact: bundle.SourceArtifact, BundleHash: fact.BundleHash(), StartedAt: time.Now().UTC()})
		},
		Construct: func(ctx context.Context, instance pipeline.WorkflowInstance) error {
			command, err := flowactivationfixture.Command(ctx, instance, pipeline.WorkflowLifecycleMutationPlan{}, instance.CreatedAt)
			if err != nil {
				return err
			}
			committed, err := selected.(runtimebus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(ctx, command)
			if err != nil {
				return err
			}
			if !committed.Acknowledged || !committed.Created {
				return fmt.Errorf("handler fixture did not acknowledge native construction")
			}
			return nil
		},
		NewCoordinator: func(options pipeline.PipelineCoordinatorOptions) *pipeline.PipelineCoordinator {
			pc, _ := workflowHandlerNativeCoordinator(t, selected, options, fact)
			return pc
		},
		Publish: func(ctx context.Context, event events.Event, route events.DeliveryRoute) {
			storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{route}, pipelineobligation.ScopeSubscribed)
		},
		PublishDirect: func(ctx context.Context, event events.Event) {
			storetest.CommitSemanticEvent(t, ctx, selected, event)
		},
		PublishedEvent: func(ctx context.Context, id string) (events.Event, error) {
			value, found, err := selected.(runtimebus.PreparedPublishEventReader).LoadPreparedPublishEvent(ctx, id)
			if err != nil {
				return events.Event{}, err
			}
			if !found {
				return events.Event{}, fmt.Errorf("missing committed handler emission %s", id)
			}
			if err := value.Validate(); err != nil {
				return events.Event{}, err
			}
			return value.Event.Event(), nil
		},
		PhysicalCounts: func(ctx context.Context) (pipeline.WorkflowEnginePhysicalCountsForTest, error) {
			counts, err := storetest.ReadWorkflowEnginePhysicalCounts(ctx, selected)
			return pipeline.WorkflowEnginePhysicalCountsForTest{EntityStates: counts.EntityStates, ConstructedHeaders: counts.ConstructedHeaders, MutationJournal: counts.MutationJournal}, err
		},
		ConflictingEntityType: func(ctx context.Context, run, entity string) (int64, error) {
			return storetest.SetWorkflowProjectionConflictingEntityType(ctx, selected, run, entity)
		},
		MissingHeader: func(ctx context.Context, run, path string) (int64, error) {
			return storetest.RemoveWorkflowProjectionHeader(ctx, selected, run, path)
		},
		MissingFields: func(ctx context.Context, run, entity string) (int64, error) {
			return storetest.RemoveWorkflowProjectionFields(ctx, selected, run, entity)
		},
		Draining: func(ctx context.Context, run, path string) (int64, error) {
			return storetest.SetWorkflowProjectionDraining(ctx, selected, run, path)
		},
		Terminated: func(ctx context.Context, run, path string, at time.Time) (int64, error) {
			return storetest.SetWorkflowProjectionTerminated(ctx, selected, run, path, at)
		},
		ApplicationStorage: func(ctx context.Context) (json.RawMessage, error) {
			value, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
			if err != nil {
				return nil, err
			}
			return json.Marshal(value)
		},
	}, selected, reopen, probe
}

func TestExecuteNodeContractHandlerUsesTypedEnvelopeIdentityOverPayload(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyExecuteNodeContractHandlerUsesTypedEnvelopeIdentityOverPayloadForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 1, 1)
			})
		})
	}
}

func TestExecuteNodeContractHandlerPersistsArithmeticDataAccumulationExpression(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyExecuteNodeContractHandlerPersistsArithmeticDataAccumulationExpressionForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 1, 1)
			})
		})
	}
}

func TestExecuteNodeContractHandlerFailsClosedOnDataAccumulationCELRuntimeError(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyExecuteNodeContractHandlerFailsClosedOnDataAccumulationCELRuntimeErrorForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 0, 1)
			})
		})
	}
}

func TestExecuteNodeContractHandlerPersistsExplicitAbsenceDecision(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyExecuteNodeContractHandlerPersistsExplicitAbsenceDecisionForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 1, 1)
			})
		})
	}
}

func TestIssue2564EvaluatedSnapshotMustFenceCommitBothStores(t *testing.T) {
	pipeline.VerifyIssue2564EvaluatedSnapshotMustFenceCommitBothStoresForTest(t, func(t *testing.T, backend string, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
		return workflowHandlerNativeFixture(t, backend, source, 1, 0)
	})
}

func TestExecuteNodeContractHandlerUsesConstructedEntityIdentityForWritesAndEmit(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyExecuteNodeContractHandlerUsesConstructedEntityIdentityForWritesAndEmitForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 1, 1)
			})
		})
	}
}

func TestRetainedNodeContractHandlerUsesRuntimeEnginePath(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var opened pipeline.WorkflowHandlerNativeFixtureForTest
			var runID string
			pipeline.VerifyRetainedNodeContractHandlerUsesRuntimeEnginePathForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				fixture := workflowHandlerNativeFixture(t, backend, source, 1, 1)
				opened = fixture
				requireRun := fixture.RequireRun
				fixture.RequireRun = func(ctx context.Context, run string) error {
					if err := requireRun(ctx, run); err != nil {
						return err
					}
					runID = run
					before, err := fixture.Runs.LoadRunOrigin(ctx, run)
					if err != nil || !reflect.DeepEqual(before, runtimerunlifecycle.ScenarioSetupRunOrigin()) {
						t.Fatalf("native scenario origin=%+v err=%v", before, err)
					}
					counts, err := fixture.PhysicalCounts(ctx)
					if err != nil {
						return err
					}
					wrong := eventtest.RunCreatingRootIngressWithRoutingSource(
						"00000000-0000-0000-0000-000000000001", events.EventType("custom.trigger"), "", "", nil, 0, run, "",
						events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: ".", FlowInstance: run}), eventtest.StaticFlowRoutingSource(".", run, ""), time.Unix(1, 0).UTC(),
					)
					bound, err := eventfixture.BindPayload(wrong)
					if err != nil {
						return err
					}
					admitted, err := events.AdmitForPublish(bound, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
					if err != nil {
						return err
					}
					if admitted.RunDisposition() != events.AdmittedRunCreateAuthorized {
						t.Fatal("wrong-constructor control no longer creates work")
					}
					if err := storetest.EnsureRunForAdmittedEvent(ctx, fixture.Runs, admitted, wrong.CreatedAt()); err == nil || !strings.Contains(err.Error(), "run lifecycle origin conflict") {
						t.Fatalf("wrong run-creating constructor bypassed native admission: %v", err)
					}
					after, err := fixture.Runs.LoadRunOrigin(ctx, run)
					if err != nil || !reflect.DeepEqual(after, before) {
						t.Fatalf("refused creation changed scenario origin: %+v -> %+v/%v", before, after, err)
					}
					afterCounts, err := fixture.PhysicalCounts(ctx)
					if err != nil || afterCounts != counts {
						t.Fatalf("refused creation reached workflow mutation: %+v -> %+v/%v", counts, afterCounts, err)
					}
					if _, err := fixture.PublishedEvent(ctx, wrong.ID()); err == nil || !strings.Contains(err.Error(), "missing committed handler emission ") {
						t.Fatalf("refused creation persisted its event: %v", err)
					}
					return nil
				}
				return fixture
			})
			origin, err := opened.Runs.LoadRunOrigin(opened.Context, runID)
			if err != nil || !reflect.DeepEqual(origin, runtimerunlifecycle.ScenarioSetupRunOrigin()) {
				t.Fatalf("handler changed immutable scenario origin: %+v/%v", origin, err)
			}
		})
	}
}

func TestExecuteNodeContractHandlerDefersCommittedEmissions(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyExecuteNodeContractHandlerDefersCommittedEmissionsForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 1, 1)
			})
		})
	}
}

func TestExecuteNodeContractHandlerAppliesEmitFieldsToEmittedEvent(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyExecuteNodeContractHandlerAppliesEmitFieldsToEmittedEventForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 1, 1)
			})
		})
	}
}

func TestExecuteNodeContractHandlerOnSuccessRulesEmitsBothInOrder(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyExecuteNodeContractHandlerOnSuccessRulesEmitsBothInOrderForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 1, 1)
			})
		})
	}
}

func TestExecuteNodeContractHandlerRulesEmitTemplatePublishesOneMergedEvent(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyExecuteNodeContractHandlerRulesEmitTemplatePublishesOneMergedEventForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 1, 1)
			})
		})
	}
}

func TestExecuteNodeContractHandler_UsesEmitFieldsAsOnlyBusinessPayloadSource(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyExecuteNodeContractHandler_UsesEmitFieldsAsOnlyBusinessPayloadSourceForTest(t, func(t *testing.T, source semanticview.Source, refused bool) pipeline.WorkflowHandlerNativeFixtureForTest {
				var mutations uint64 = 1
				if refused {
					mutations = 0
				}
				return workflowHandlerNativeFixture(t, backend, source, mutations, 1)
			})
		})
	}
}

func TestExecuteNodeContractHandler_GuardEscalateUsesOnlyRuntimeOwnedEnvelope(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyExecuteNodeContractHandler_GuardEscalateUsesOnlyRuntimeOwnedEnvelopeForTest(t, func(t *testing.T, source semanticview.Source, refused bool) pipeline.WorkflowHandlerNativeFixtureForTest {
				var mutations uint64 = 1
				if refused {
					mutations = 0
				}
				return workflowHandlerNativeFixture(t, backend, source, mutations, 1)
			})
		})
	}
}

func TestExecuteNodeContractHandler_GuardEscalateObjectFieldsUseExplicitPayloadOnly(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyExecuteNodeContractHandler_GuardEscalateObjectFieldsUseExplicitPayloadOnlyForTest(t, func(t *testing.T, source semanticview.Source, refused bool) pipeline.WorkflowHandlerNativeFixtureForTest {
				var mutations uint64 = 1
				if refused {
					mutations = 0
				}
				return workflowHandlerNativeFixture(t, backend, source, mutations, 1)
			})
		})
	}
}

func TestExecuteNodeContractHandler_RejectsUndeclaredBusinessPayloadAcrossImmediateEmitSites(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyExecuteNodeContractHandler_RejectsUndeclaredBusinessPayloadAcrossImmediateEmitSitesForTest(t, func(t *testing.T, source semanticview.Source, refused bool) pipeline.WorkflowHandlerNativeFixtureForTest {
				var mutations uint64 = 1
				if refused {
					mutations = 0
				}
				return workflowHandlerNativeFixture(t, backend, source, mutations, 1)
			})
		})
	}
}

func TestExistingOwnerExecutionSemanticsPersistOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyExistingOwnerExecutionSemanticsPersistOnSQLiteAndPostgresForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 3, 3)
			})
		})
	}
}

func TestPipelineCompiledTransitionRejectsContradictoryEvidenceOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyPipelineCompiledTransitionRejectsContradictoryEvidenceOnBothStoresForTest(t, backend, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 1, 1)
			})
		})
	}
}

func TestPipelineTransitionRejectsCoherentEventSubstitutionOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyPipelineTransitionRejectsCoherentEventSubstitutionOnBothStoresForTest(t, backend, func(t *testing.T, source semanticview.Source, variant string) pipeline.WorkflowHandlerNativeFixtureForTest {
				var mutations uint64
				if variant == "accepted_control" || variant == "direct_execution_control" {
					mutations = 1
				}
				return workflowHandlerNativeFixture(t, backend, source, mutations, 0)
			})
		})
	}
}

func TestAcceptedLifecycleConsumerRejectsUnownedTransitionOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyAcceptedLifecycleConsumerRejectsUnownedTransitionOnBothStoresForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 0, 0)
			})
		})
	}
}

func TestPipelinePreclaimFailurePreservesErrorAndReturnsExactCarrier(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyPipelinePreclaimFailurePreservesErrorAndReturnsExactCarrierForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 0, 0)
			})
		})
	}
}

func TestPipelineCoordinatorInterceptSkipsNodeWithoutPersistedDeliveryAuthority(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyPipelineCoordinatorInterceptSkipsNodeWithoutPersistedDeliveryAuthorityForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 0, 0)
			})
		})
	}
}

func TestEntityLastFieldClearAndColdReloadBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyEntityLastFieldClearAndColdReloadBothStoresForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 1, 1)
			})
		})
	}
}

func TestSelectedHandlerSparsePresenceWriteAndEmitBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifySelectedHandlerSparsePresenceWriteAndEmitBothStoresForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 1, 1)
			})
		})
	}
}

func TestSelectedHandlerSparseEqualityMutationsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifySelectedHandlerSparseEqualityMutationsBothStoresForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 1, 1)
			})
		})
	}
}

func TestPipelineEngineStateRepoLoadStateMissingEntityDoesNotMaterializeDefaults(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyPipelineEngineStateRepoLoadStateMissingEntityDoesNotMaterializeDefaultsForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 0, 0)
			})
		})
	}
}

func TestPipelineEngineMutationOwnerRoundTripsTypedCarrier(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyPipelineEngineMutationOwnerRoundTripsTypedCarrierForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 1, 0)
			})
		})
	}
}

func TestPipelineEngineEvaluatorQueryEntitiesUsesExecutingFlowID(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyPipelineEngineEvaluatorQueryEntitiesUsesExecutingFlowIDForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 0, 0)
			})
		})
	}
}

func TestPipelineExpressionPreservesExactExecutionFlowOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyPipelineExpressionPreservesExactExecutionFlowOnBothStoresForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 0, 0)
			})
		})
	}
}

func TestDeliveryTargetApplicationRejectsMissingExactExistingTargetWithoutMutation(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyDeliveryTargetApplicationRejectsMissingExactExistingTargetWithoutMutationForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 0, 0)
			})
		})
	}
}

func TestDeliveryTargetApplicationCarriesConstructedScenarioPreStateThroughMutationOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyDeliveryTargetApplicationCarriesConstructedScenarioPreStateThroughMutationOnSQLiteAndPostgresForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 1, 1)
			})
		})
	}
}

func TestCompositionReceiverInitializationAndRepeatedBusinessWritesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyCompositionReceiverInitializationAndRepeatedBusinessWritesBothStoresForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 2, 2)
			})
		})
	}
}

func TestPipelineEngineMutationOwnerRejectsWrongRunRootAddressBeforeMutationOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyPipelineEngineMutationOwnerRejectsWrongRunRootAddressBeforeMutationOnBothStoresForTest(t, backend, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 0, 0)
			})
		})
	}
}

func TestWorkflowEngineFirstMaterializationRejectsMissingOrContradictoryEntityContractOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyWorkflowEngineFirstMaterializationRejectsMissingOrContradictoryEntityContractOnBothStoresForTest(t, backend, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 0, 0)
			})
		})
	}
}

func TestWorkflowEngineMutationRejectsEntityContractDriftOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyWorkflowEngineMutationRejectsEntityContractDriftOnBothStoresForTest(t, backend, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 0, 0)
			})
		})
	}
}

func TestDeliveryTargetApplicationReloadsCurrentScopedStateOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyDeliveryTargetApplicationReloadsCurrentScopedStateOnSQLiteAndPostgresForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 1, 0)
			})
		})
	}
}

func TestDeliveryTargetApplicationPreservesCompositionTargetOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyDeliveryTargetApplicationPreservesCompositionTargetOnSQLiteAndPostgresForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 0, 0)
			})
		})
	}
}

func TestDeliveryTargetApplicationConsumesDeclarationBoundJoinTargetWithoutPayloadSelectorOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyDeliveryTargetApplicationConsumesDeclarationBoundJoinTargetWithoutPayloadSelectorOnBothStoresForTest(t, backend, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 0, 0)
			})
		})
	}
}

func TestPipelineEngineMutationOwnerRejectsForeignFlowWrite(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyPipelineEngineMutationOwnerRejectsForeignFlowWriteForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowHandlerNativeFixtureForTest {
				return workflowHandlerNativeFixture(t, backend, source, 0, 0)
			})
		})
	}
}
