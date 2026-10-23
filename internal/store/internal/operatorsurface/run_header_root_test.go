package operatorsurface

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

type runHeaderValues []any

func (row runHeaderValues) Scan(dest ...any) error {
	if len(row) != len(dest) {
		return fmt.Errorf("header column count: got %d, want %d", len(dest), len(row))
	}
	for i, value := range row {
		target := reflect.ValueOf(dest[i]).Elem()
		if value == nil {
			target.SetZero()
		} else {
			target.Set(reflect.ValueOf(value))
		}
	}
	return nil
}

func TestS1RunHeaderScannersPreserveExactRootAndLifecycle(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			query, scan := sqliteRunHeaderSelectSQL(), scanSQLiteRunHeader
			if backend == "postgres" {
				query, scan = runHeaderSelectSQL(), scanRunHeader
			}
			if !strings.Contains(query, "r.source_root_directory,") {
				t.Fatal("header projection lost the exact persisted root")
			}
			for _, root := range []string{"shop-a", "shop-b", "", " shop-a", "shop-a ", "../shop-a", "shop/a"} {
				t.Run(root, func(t *testing.T) {
					row := validRunHeaderValues(backend)
					row[3] = root
					header, err := scan(row)
					valid := root == "shop-a" || root == "shop-b"
					if valid {
						if err != nil || header.BundleHash != sourceartifactfixture.BundleHash || header.Origin.Kind() != runlifecycle.OriginDeployment {
							t.Fatalf("exact historical header: %+v, %v", header, err)
						}
					} else if err == nil || !reflect.DeepEqual(header, operatorread.RunHeader{}) || !strings.Contains(err.Error(), "snapshot root:") {
						t.Fatalf("invalid root was normalized or inferred: %+v, %v", header, err)
					}
				})
			}
			for _, change := range []struct {
				name  string
				index int
				value any
			}{
				{"origin", 4, "unknown"},
				{"standing_relation", 13, 1},
				{"negative_event_count", 16, -1},
				{"negative_entity_count", 15, -1},
				{"failed_without_failure", 1, "failed"},
				{"completed_without_end", 1, "completed"},
				{"forked_without_continuation", 1, "forked"},
				{"running_with_continuation", 19, "22222222-2222-4222-8222-222222222222"},
			} {
				t.Run(change.name, func(t *testing.T) {
					row := validRunHeaderValues(backend)
					row[change.index] = change.value
					if header, err := scan(row); err == nil || !reflect.DeepEqual(header, operatorread.RunHeader{}) {
						t.Fatalf("root carrier bypassed %s validation: %+v, %v", change.name, header, err)
					}
				})
			}
		})
	}
}

func validRunHeaderValues(backend string) runHeaderValues {
	at := time.Date(2026, 10, 16, 1, 0, 0, 0, time.UTC)
	var endedAt any = sql.NullTime{}
	var failure any = []byte(nil)
	if backend == "sqlite" {
		endedAt, failure = nil, nil
	}
	return runHeaderValues{
		"11111111-1111-4111-8111-111111111111", "running", sourceartifactfixture.BundleHash, "shop-a",
		string(runlifecycle.OriginDeployment), "", "", "", int64(0), "", "", int64(0), "",
		0, 0, 0, 0, at, endedAt, "", failure, "",
	}
}

func TestS1RunHeaderLifecyclePreservesExactHash(t *testing.T) {
	header := operatorread.RunHeader{
		RunID: "11111111-1111-4111-8111-111111111111", Status: "running",
		Origin: runlifecycle.DeploymentRunOrigin(), StartedAt: time.Now().UTC(),
	}
	root := sourceartifact.RootAdmissionReference{BundleHash: " " + sourceartifactfixture.BundleHash, RootDirectory: "shop-a"}
	if err := validateRunHeaderLifecycle(header, root); err == nil {
		t.Fatal("header adapter trimmed persisted identity before canonical validation")
	}
}
