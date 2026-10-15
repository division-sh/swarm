package startupownership

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
)

func TestSourceSetHeadDecoderBindsStoredRevision(t *testing.T) {
	empty, err := agenttopology.EmptySourceSetPlan()
	if err != nil {
		t.Fatal(err)
	}
	full, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{
		BundleHash: "bundle-v2:sha256:" + strings.Repeat("2", 64),
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, dialect := range []string{"sqlite", "postgres"} {
		for _, lock := range []bool{false, true} {
			mode := "passive"
			if lock {
				mode = "locked"
			}
			for _, state := range []string{"valid_empty", "valid_full", "absent", "corrupt_plan", "revision_mismatch", "query_error"} {
				t.Run(dialect+"/"+mode+"/"+state, func(t *testing.T) {
					proveSourceSetHeadDecoder(t, dialect == "sqlite", lock, state, empty, full)
				})
			}
		}
	}
}

func proveSourceSetHeadDecoder(t *testing.T, sqlite, lock bool, state string, empty, full agenttopology.SourceSetPlan) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	query := `SELECT revision,plan FROM agent_topology_source_set_head WHERE singleton_id = 1`
	if !sqlite && lock {
		query += ` FOR SHARE`
	}
	expected := mock.ExpectQuery("^" + regexp.QuoteMeta(query) + "$")
	want := full
	if state == "valid_empty" {
		want = empty
	}
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	rows := sqlmock.NewRows([]string{"revision", "plan"})
	queryErr := errors.New("source-set decoder query control")
	switch state {
	case "query_error":
		expected.WillReturnError(queryErr)
	case "absent":
		expected.WillReturnRows(rows)
	case "corrupt_plan":
		expected.WillReturnRows(rows.AddRow(want.Revision, []byte(`{}`)))
	case "revision_mismatch":
		expected.WillReturnRows(rows.AddRow(strings.Repeat("0", 64), raw))
	default:
		expected.WillReturnRows(rows.AddRow(want.Revision, raw))
	}
	var actual agenttopology.SourceSetPlan
	var found bool
	if lock {
		actual, found, err = loadSourceSetTx(context.Background(), tx, sqlite)
	} else {
		actual, found, err = loadSourceSetModeTx(context.Background(), tx, sqlite, false)
	}
	mock.ExpectRollback()
	if rollbackErr := tx.Rollback(); rollbackErr != nil {
		t.Fatal(rollbackErr)
	}
	if mockErr := mock.ExpectationsWereMet(); mockErr != nil {
		t.Fatal(mockErr)
	}
	switch state {
	case "valid_empty", "valid_full":
		if err != nil || !found || !reflect.DeepEqual(actual, want) {
			t.Fatalf("exact stored plan lost: %+v found=%t err=%v", actual, found, err)
		}
	case "absent":
		if err != nil || found || !reflect.DeepEqual(actual, agenttopology.SourceSetPlan{}) {
			t.Fatalf("absent head acquired empty-plan evidence: %+v found=%t err=%v", actual, found, err)
		}
	default:
		if err == nil || found || !reflect.DeepEqual(actual, agenttopology.SourceSetPlan{}) {
			t.Fatalf("invalid head exposed a decoded plan: %+v found=%t err=%v", actual, found, err)
		}
		if state == "query_error" && !errors.Is(err, queryErr) {
			t.Fatalf("query failure was replaced by absence: %v", err)
		}
		if state == "revision_mismatch" && !strings.Contains(err.Error(), "stored revision differs from decoded plan") {
			t.Fatalf("stored/decoded revision disagreement lost its exact reason: %v", err)
		}
	}
}
