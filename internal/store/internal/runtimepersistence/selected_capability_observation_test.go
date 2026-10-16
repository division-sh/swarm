package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

type selectedCapabilityReadCase struct {
	name                 string
	read                 func(context.Context, any, string) (any, error)
	empty                any
	absent, postgresOnly bool
}

func selectedCapabilityReadCases() []selectedCapabilityReadCase {
	return []selectedCapabilityReadCase{
		{"delivery_rollback_cut", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadDeliveryRollbackStorageForTest(ctx, owner, id)
		}, DeliveryRollbackStorage{}, false, false},
		{"delivery_exact_dead_letter", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadExactDeliveryDeadLetterStorageForTest(ctx, owner, id, 1)
		}, ExactDeliveryDeadLetterStorage{}, true, false},
		{"delivery_attempt_failure", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadDeliveryAttemptFailureStorageForTest(ctx, owner, id, 1)
		}, []byte(nil), true, false},
		{"delivery_activity_failure", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadLatestDeliveryActivityFailureStorageForTest(ctx, owner, id)
		}, []byte(nil), true, false},
		{"delivery_attempt_history", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadDeliveryConformanceAttemptsStorageForTest(ctx, owner, id)
		}, []DeliveryConformanceAttemptStorage(nil), false, false},
		{"delivery_matching_open", func(ctx context.Context, owner any, id string) (any, error) {
			return CountMatchingOpenDeliveryAttemptStorageForTest(ctx, owner, id, 1)
		}, 0, false, false},
		{"delivery_exact_selection", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadDeliveryConformanceSelectionStorageForTest(ctx, owner, id)
		}, PublicationRuleSelectionStorage{}, true, false},
		{"delivery_selection_count", func(ctx context.Context, owner any, id string) (any, error) {
			return CountDeliveryHandlerSelectionsStorageForTest(ctx, owner, id)
		}, 0, false, false},
		{"provider_operations", func(ctx context.Context, owner any, id string) (any, error) {
			return CountSelectedProviderOperationsStorageForTest(ctx, owner, id, 1)
		}, 0, false, true},
		{"provider_capabilities", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadSelectedCompletionCapabilitiesStorageForTest(ctx, owner, id, "anthropic_api", 1, "owner", id)
		}, []SelectedCompletionCapabilityStorage(nil), false, true},
		{"preparation", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadSelectedPreparationStorageForTest(ctx, owner, id)
		}, SelectedPreparationStorage{}, true, false},
		{"startup_counts", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadSelectedClaudeStartupCountsStorageForTest(ctx, owner, id, id)
		}, SelectedClaudeStartupCounts{}, false, false},
		{"startup_surface", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadSelectedClaudeStartupSurfaceStorageForTest(ctx, owner, id, id)
		}, "", true, false},
		{"turn_surface", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadSelectedClaudeTurnCapabilityStorageForTest(ctx, owner, id, id)
		}, SelectedClaudeTurnCapabilityStorage{}, true, false},
		{"failure_diagnostics", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadSelectedClaudeAttemptDiagnosticsStorageForTest(ctx, owner, id)
		}, []SelectedClaudeAttemptDiagnosticStorage(nil), false, false},
		{"model_aliases", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadSelectedCompletionModelAliasStorageForTest(ctx, owner, id, "claude_cli")
		}, SelectedCompletionModelAliasStorage{}, false, false},
		{"provider_failure", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadSelectedExecutionFailureStorageForTest(ctx, owner, id)
		}, SelectedExecutionFailureStorage{}, true, false},
		{"active_selected_attempts", func(ctx context.Context, owner any, id string) (any, error) {
			return CountSelectedExecutionActiveAttemptsStorageForTest(ctx, owner, id)
		}, 0, false, false},
		{"root_flow_headers", func(ctx context.Context, owner any, id string) (any, error) {
			return CountSelectedExecutionRootFlowHeadersStorageForTest(ctx, owner, id)
		}, 0, false, false},
		{"root_entity_rows", func(ctx context.Context, owner any, id string) (any, error) {
			return CountSelectedExecutionRootEntityRowsStorageForTest(ctx, owner, id)
		}, 0, false, false},
		{"event_receipts", func(ctx context.Context, owner any, id string) (any, error) {
			return CountEventReceiptStorageForTest(ctx, owner, id)
		}, 0, false, false},
		{"event_mapping", func(ctx context.Context, owner any, id string) (any, error) {
			return CountSelectedExecutionEventMappingStorageForTest(ctx, owner, id, id, id, id)
		}, 0, false, false},
		{"node_failure", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadSelectedNodeDeliveryFailureStorageForTest(ctx, owner, id, id)
		}, SelectedNodeDeliveryFailureStorage{}, true, false},
		{"subscriber_deliveries", func(ctx context.Context, owner any, id string) (any, error) {
			return CountRunEventSubscriberDeliveriesStorageForTest(ctx, owner, id, id, "source-agent-that-must-not-route")
		}, 0, false, false},
		{"fork_event", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadSelectedExecutionEventStorageForTest(ctx, owner, id)
		}, SelectedExecutionEventStorage{}, true, false},
		{"follow_ups", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadSelectedExecutionFollowUpCountsForTest(ctx, owner, id, id)
		}, SelectedExecutionFollowUpCounts{}, false, false},
		{"run_payloads", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadSelectedExecutionPayloadsStorageForTest(ctx, owner, id)
		}, [][]byte(nil), false, false},
		{"agent_follow_ups", func(ctx context.Context, owner any, id string) (any, error) {
			return CountSelectedAgentFollowUpStorageForTest(ctx, owner, id, id)
		}, 0, false, false},
		{"named_agent", func(ctx context.Context, owner any, id string) (any, error) {
			return CountRunNamedAgentStorageForTest(ctx, owner, id, "test-agent")
		}, 0, false, false},
		{"agent_deliveries", func(ctx context.Context, owner any, id string) (any, error) {
			return CountRunEventAgentDeliveriesStorageForTest(ctx, owner, id, id, "test-agent")
		}, 0, false, false},
		{"delivered_agent_attempts", func(ctx context.Context, owner any, id string) (any, error) {
			return CountDeliveredAgentAttemptsStorageForTest(ctx, owner, id, "test-agent")
		}, 0, false, false},
		{"latest_receipt_failure", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadLatestReceiptFailureStorageForTest(ctx, owner)
		}, "", true, false},
		{"latest_dead_letter_failure", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadLatestDeadLetterFailureStorageForTest(ctx, owner)
		}, "", true, false},
		{"latest_selected_agent_failure", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadPostgresSelectedAgentDeliveryFailureStorageForTest(ctx, owner, id)
		}, "", true, true},
		{"delivery_attempt_array", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadPostgresRunDeliveryAttemptArrayStorageForTest(ctx, owner, id)
		}, "", false, true},
		{"delivery_timeline", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadPostgresSelectedForkDeliveryTimelineStorageForTest(ctx, owner, id)
		}, "", false, true},
		{"route_timeline", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadPostgresSelectedForkRouteTimelineStorageForTest(ctx, owner, id)
		}, "", false, true},
		{"event_timeline", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadPostgresSelectedForkEventTimelineStorageForTest(ctx, owner, id)
		}, "", false, true},
		{"runtime_log_array", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadPostgresRunRuntimeLogArrayStorageForTest(ctx, owner, id)
		}, "", false, true},
		{"typed_local_runtime_diagnostics", func(ctx context.Context, owner any, id string) (any, error) {
			return CountPostgresSelectedForkLocalRuntimeDiagnosticsStorageForTest(ctx, owner, id, id, "owner")
		}, 0, false, true},
		{"cancelled_branch_evidence", func(ctx context.Context, owner any, id string) (any, error) {
			return CountPostgresSelectedForkCancelledBranchEvidenceStorageForTest(ctx, owner, id, id, id, "policy")
		}, 0, false, true},
		{"source_session_union", func(ctx context.Context, owner any, id string) (any, error) {
			return CountForkOrSourceSessionStorageForTest(ctx, owner, id, id)
		}, 0, false, false},
		{"source_audit_union", func(ctx context.Context, owner any, id string) (any, error) {
			return CountForkOrSourceConversationAuditStorageForTest(ctx, owner, id, id)
		}, 0, false, false},
		{"source_turn_union", func(ctx context.Context, owner any, id string) (any, error) {
			return CountForkOrSourceTurnStorageForTest(ctx, owner, id, id)
		}, 0, false, false},
		{"active_source_subscriber", func(ctx context.Context, owner any, id string) (any, error) {
			return CountForkSourceSubscriberActiveDeliveriesStorageForTest(ctx, owner, id)
		}, 0, false, false},
		{"source_timer_status", func(ctx context.Context, owner any, id string) (any, error) {
			return ReadSelectedForkSourceTimerStatusForTest(ctx, owner, id, id)
		}, "", true, false},
	}
}

func TestSelectedCapabilityReadPortsKeepIndependentCutsAndStrictAbsenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			before := competingRevisionPairSnapshot(t, f)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, tc := range selectedCapabilityReadCases() {
				t.Run(tc.name, func(t *testing.T) {
					id := state.Identity.RunID
					if tc.name == "root_flow_headers" || tc.name == "root_entity_rows" {
						id = uuid.NewString()
					}
					got, err := tc.read(f.ctx, f.store, id)
					refused := backend == "sqlite" && tc.postgresOnly
					if (err != nil) != (tc.absent || refused) || !reflect.DeepEqual(got, selectedCapabilityEmptyResult(tc, refused)) {
						t.Fatalf("%s borrowed unrelated construction or changed absence: %+v %v", tc.name, got, err)
					}
					if tc.absent && !refused && !errors.Is(err, sql.ErrNoRows) {
						t.Fatalf("%s changed exact missing-row error: %v", tc.name, err)
					}
				})
			}
			wantBegun, wantCommitted := int64(46), int64(32)
			if backend == "sqlite" {
				wantBegun, wantCommitted = 36, 23
			}
			if counts := probe.Snapshot(); counts.Active != 0 || int64(counts.Total.Begun) != wantBegun || int64(counts.Total.ReadCommits) != wantCommitted || counts.Total.WriteCommits != 0 {
				t.Fatalf("capability evidence joined separate cuts or wrote storage: %+v", counts)
			}
			restore()
			for _, tc := range selectedCapabilityReadCases() {
				if tc.name != "root_flow_headers" && tc.name != "root_entity_rows" {
					continue
				}
				// The native keyless root has a header but no entity row; its child
				// has both. Neither read may borrow the child's physical evidence.
				want := 1
				if tc.name == "root_entity_rows" {
					want = 0
				}
				got, err := tc.read(f.ctx, f.store, state.Identity.RunID)
				if err != nil || got != want {
					t.Fatalf("%s did not distinguish the real root from its child: %+v %v", tc.name, got, err)
				}
			}
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("capability observations changed complete history or clocks")
			}
		})
	}
}

func selectedCapabilityEmptyResult(tc selectedCapabilityReadCase, refused bool) any {
	if !refused && (tc.name == "delivery_attempt_array" || tc.name == "runtime_log_array") {
		return "[]"
	}
	return tc.empty
}

func TestSelectedCapabilityReadPortsRefuseRawInvalidCancelledAndClosedOwnersBothStores(t *testing.T) {
	refuse := func(ctx context.Context, owner any, id string, cause error) {
		for _, tc := range selectedCapabilityReadCases() {
			got, err := tc.read(ctx, owner, id)
			if err == nil || cause != nil && !errors.Is(err, cause) || !reflect.DeepEqual(got, tc.empty) {
				t.Fatalf("unavailable %s returned capability evidence: %+v %v", tc.name, got, err)
			}
		}
	}
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		refuse(context.Background(), owner, uuid.NewString(), nil)
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			refuse(f.ctx, f.store, "invalid", nil)
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			for _, tc := range selectedCapabilityReadCases() {
				if backend == "sqlite" && tc.postgresOnly {
					continue
				}
				got, err := tc.read(cancelled, f.store, state.Identity.RunID)
				if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, tc.empty) {
					t.Fatalf("cancelled %s returned evidence: %+v %v", tc.name, got, err)
				}
			}
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			refuse(f.ctx, f.store, state.Identity.RunID, nil)
		})
	}
}
