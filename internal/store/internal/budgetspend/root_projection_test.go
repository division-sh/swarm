package budgetspendstore

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestS1BudgetProjectionRowsRequireExactRoot(t *testing.T) {
	root, _ := sourceartifactfixture.Rooted(t, "budget", sourceartifactfixture.Artifact())
	columns := []string{"run_id", "entity_id", "bundle_hash", "source_root_directory", "flow_template", "flow_instance", "current_state"}
	readFailure := errors.New("budget projection row read failed")
	for _, cell := range []struct {
		name string
		hash any
		root any
		err  error
	}{
		{"valid", root.Artifact().BundleHash(), "budget", nil},
		{"missing_root", root.Artifact().BundleHash(), "", nil},
		{"whitespace_root", root.Artifact().BundleHash(), "budget ", nil},
		{"noncanonical_root", root.Artifact().BundleHash(), "budget.v2", nil},
		{"null_root", root.Artifact().BundleHash(), nil, nil},
		{"missing_hash", "", "budget", nil},
		{"malformed_hash", "hash7", "budget", nil},
		{"read_error_after_valid_prefix", root.Artifact().BundleHash(), "budget", readFailure},
	} {
		t.Run(cell.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Error(err)
				}
			})
			rows := sqlmock.NewRows(columns).
				AddRow("run-prefix", "entity-prefix", root.Artifact().BundleHash(), "budget", "child", "child/key", "Ready").
				AddRow("run-selected", "entity-selected", cell.hash, cell.root, "child", "child/key", "Ready")
			if cell.err != nil {
				rows.RowError(1, cell.err)
			}
			mock.ExpectQuery("SELECT budget_projection_fixture").WillReturnRows(rows)
			mock.ExpectClose()
			native, err := db.QueryContext(context.Background(), "SELECT budget_projection_fixture")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := native.Close(); err != nil {
					t.Error(err)
				}
			})
			got, err := scanBudgetProjectionTargets(native)
			if cell.name != "valid" {
				if err == nil || got != nil || cell.err != nil && !errors.Is(err, cell.err) {
					t.Fatalf("invalid target disclosed a partial projection or lost its error: %+v, %v", got, err)
				}
				return
			}
			want := []budgetspend.ProjectionTarget{
				{RunID: "run-prefix", EntityID: "entity-prefix", RootReference: root.Admission().Reference(), FlowTemplate: "child", FlowInstance: "child/key", Stage: "Ready"},
				{RunID: "run-selected", EntityID: "entity-selected", RootReference: sourceartifact.RootAdmissionReference{BundleHash: root.Artifact().BundleHash(), RootDirectory: "budget"}, FlowTemplate: "child", FlowInstance: "child/key", Stage: "Ready"},
			}
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("budget targets changed exact source/root, flow or stage: %+v, %v", got, err)
			}
		})
	}
}
