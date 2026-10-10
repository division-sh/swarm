package runtime_test

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

type runtimeLogSelectedForTest interface {
	runtimepkg.RuntimeLogPersistence
	deliverylifecycle.Store
	runlifecycle.OperationOwner
	runlifecycle.CandidateStore
	sourceartifactfixture.Writer
	storetest.DeliveryPublicationFixtureStore
}

func nativeRuntimeLogFixture(t *testing.T, backend string, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
	t.Helper()
	var selected runtimeLogSelectedForTest
	capacity := startupownership.SQLiteFanOutCapacity()
	if backend == "postgres" {
		selected = storetest.StartPostgresRuntimeStore(t)
		var err error
		capacity, err = startupownership.PostgreSQLFanOutCapacity(storetest.ObserveSelectedPool(t, selected).MaxOpenConnections, 0)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		selected = storetest.StartSQLiteRuntimeStore(t)
	}
	fact := sourceartifactfixture.FactFor(artifact)
	ctx := correlation.WithSourceArtifactFact(context.Background(), fact)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope("00000000-0000-4000-8000-000000001331", fact.BundleHash()))
	if err := sourceartifactfixture.EnsureArtifact(ctx, selected, artifact); err != nil {
		t.Fatal(err)
	}
	return runtimepkg.RuntimeLogNativeFixtureForTest{
		Persistence: selected, Deliveries: selected, Runs: selected, Context: ctx, FanOutCapacity: capacity,
		RequireRun: func(ctx context.Context, run string) error {
			return storetest.MaterializeRun(ctx, selected, storetest.RunFixture{RunID: run, Origin: storetest.ScenarioSetupOrigin(), Artifact: artifact, BundleHash: artifact.BundleHash(), StartedAt: time.Now().UTC()})
		},
		RunSnapshot: func(ctx context.Context, run string) (runlifecycle.Snapshot, error) {
			return storetest.ReadRuntimeLogRunSnapshot(ctx, selected, run)
		},
		LatestLog: func(ctx context.Context) (events.Event, error) {
			return storetest.ReadLatestRuntimeLogRecord(ctx, selected)
		},
		StartupDecision: func(ctx context.Context) (events.Event, error) {
			return storetest.ReadLatestStartupRecoveryDecisionRecord(ctx, selected)
		},
		RunPresence: func(ctx context.Context, run string) (bool, error) {
			count, err := storetest.ReadNotifyRunPresence(ctx, selected, run)
			return count == 1, err
		},
		RemoveArtifact: func(ctx context.Context, hash string) (int64, error) {
			return storetest.RemoveRuntimeLogFixtureSourceArtifact(ctx, selected, hash)
		},
		AppendFault: func(ctx context.Context, runID string, enabled bool) error {
			return storetest.SetRuntimeLogFixtureAppendFault(ctx, selected, runID, enabled)
		},
		PublishSubject: func(ctx context.Context, event events.Event) {
			storetest.CommitSemanticEvent(t, ctx, selected, event)
		},
		PublishDelivery: func(ctx context.Context, event events.Event, routes []events.DeliveryRoute, authority deliverylifecycle.ExecutionAuthority) events.Event {
			storetest.CommitNativeDeliveryPublication(t, ctx, selected, storetest.AdmitNativeDeliveryEvent(t, event), routes, authority, nil)
			return storetest.LoadCanonicalEventRecord(t, ctx, selected, event.ID())
		},
		Physical: func(ctx context.Context) runtimepkg.RuntimeLogPhysicalForTest {
			rows := storetest.ObserveActivityResultPublicationStorage(t, ctx, selected)
			return runtimepkg.RuntimeLogPhysicalForTest{Runs: rows.Runs, Events: rows.Events, Deliveries: rows.Deliveries, Entities: rows.Entities, Receipts: rows.Receipts}
		},
	}
}

func TestRuntimeLoggerNativeAppendRollback(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeLoggerNativeAppendRollbackForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}

func TestRuntimeLoggerNativeCrossRunLineageRefusal(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeLoggerNativeCrossRunLineageRefusalForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}

func TestRuntimeLogNativeIdentityReplayAndConflict(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeLogNativeIdentityReplayAndConflictForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}

func TestRuntimeLoggerNativeArtifactLossCuts(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeLoggerNativeArtifactLossCutsForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}

func TestRuntimeLoggerNativeCancellationRefusesMutation(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeLoggerNativeCancellationRefusesMutationForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}

func TestRuntimeStart_RecoveryDisabledEmitsDeniedDecisionForActiveSchedules(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeStart_RecoveryDisabledEmitsDeniedDecisionForActiveSchedulesForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}

func TestRuntimeStart_RecoveryDisabledAllowsAndLogsManagerSnapshotWork(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeStart_RecoveryDisabledAllowsAndLogsManagerSnapshotWorkForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}

func TestRuntimeStart_RecoveryEnabledEmitsAllowedDecisionSummary(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeStart_RecoveryEnabledEmitsAllowedDecisionSummaryForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}

func TestRuntimeStart_WorkflowOnlyRecoveryUsesFamilyAwareBootAndRestorationDetail(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeStart_WorkflowOnlyRecoveryUsesFamilyAwareBootAndRestorationDetailForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}

func TestRuntimeStart_RecoveryFailureEmitsDegradedDecisionSummary(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeStart_RecoveryFailureEmitsDegradedDecisionSummaryForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}

func TestRuntimeStart_DynamicFlowReadinessFinalizationFailureIsBootFatal(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeStart_DynamicFlowReadinessFinalizationFailureIsBootFatalForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}

func TestRuntimeStart_RecoveryInspectionAndManagerHydrationFailureIsBootFatal(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeStart_RecoveryInspectionAndManagerHydrationFailureIsBootFatalForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}

func TestRuntimeLogger_Log_StampsSourceArtifactFactOnRunRow(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeLogger_Log_StampsSourceArtifactFactOnRunRowForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}

func TestRuntimeLogSetupRejectsDeletedPersistedSourceArtifactFact(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeLogSetupRejectsDeletedPersistedSourceArtifactFactForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}

func TestRuntimeLogger_Log_PersistsCanonicalRunOwnershipFromContext(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeLogger_Log_PersistsCanonicalRunOwnershipFromContextForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}

func TestRuntimeLogger_Log_DoesNotInferRunOwnershipFromDetailPayload(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeLogger_Log_DoesNotInferRunOwnershipFromDetailPayloadForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}

func TestRuntimeLogger_Log_DerivesLineageFromPersistedSubjectEvent(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeLogger_Log_DerivesLineageFromPersistedSubjectEventForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}

func TestRuntimeLogger_Log_DoesNotDeriveLineageFromUnpersistedSubjectEvent(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeLogger_Log_DoesNotDeriveLineageFromUnpersistedSubjectEventForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}

func TestRuntimeLogger_Log_PersistsTypedRuntimeLineage(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimepkg.VerifyRuntimeLogger_Log_PersistsTypedRuntimeLineageForTest(t, func(t *testing.T, artifact *sourceartifact.AdmittedSourceArtifact) runtimepkg.RuntimeLogNativeFixtureForTest {
				return nativeRuntimeLogFixture(t, backend, artifact)
			})
		})
	}
}
