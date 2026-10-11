package generationauthority

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
)

func TestProcessGenerationReadRequiresExactSingleton(t *testing.T) {
	identity := processbinding.LeaseIdentity{
		LeaseID: "a0d4c1fb-76c5-46a7-b87f-d995f7cb44e5", OwnerID: "serve:reader",
		BootID: "de77e55d-dc99-4842-b4c6-bd1faaf7e9dc", RuntimeInstanceID: "fe33216e-d66f-431c-a9ef-9c8f14b295ed",
		Backend: processbinding.PostgresLeaseBackend,
	}
	query := `SELECT generation, CAST(lease_id AS TEXT), owner_id, CAST(boot_id AS TEXT),
		CAST(runtime_instance_id AS TEXT), backend FROM runtime_process_generation WHERE singleton_id = 1`
	columns := []string{"generation", "lease_id", "owner_id", "boot_id", "runtime_instance_id", "backend"}
	for _, backend := range []string{processbinding.PostgresLeaseBackend, processbinding.SQLiteLeaseBackend} {
		for _, scenario := range []string{"valid", "uninitialized", "missing", "negative", "fractional", "partial_initial", "partial_current", "noncanonical", "backend", "crossed_backend", "read_error"} {
			t.Run(backend+"/"+scenario, func(t *testing.T) {
				db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				values := []driver.Value{int64(7), identity.LeaseID, identity.OwnerID, identity.BootID, identity.RuntimeInstanceID, backend}
				rows := sqlmock.NewRows(columns)
				readErr := errors.New("selected snapshot read failed")
				switch scenario {
				case "uninitialized":
					values = []driver.Value{int64(0), nil, nil, nil, nil, nil}
				case "negative":
					values[0] = int64(-1)
				case "fractional":
					values[0] = 1.5
				case "partial_initial":
					values = []driver.Value{int64(0), identity.LeaseID, nil, nil, nil, nil}
				case "partial_current":
					values[3] = nil
				case "noncanonical":
					values[1] = strings.ToUpper(identity.LeaseID)
				case "backend":
					values[5] = "cached_owner"
				case "crossed_backend":
					values[5] = processbinding.PostgresLeaseBackend
					if backend == processbinding.PostgresLeaseBackend {
						values[5] = processbinding.SQLiteLeaseBackend
					}
				}
				if scenario == "read_error" {
					mock.ExpectQuery(query).WillReturnError(readErr)
				} else {
					if scenario != "missing" {
						rows.AddRow(values...)
					}
					mock.ExpectQuery(query).WillReturnRows(rows)
				}
				stamp, err := ReadProcessGeneration(context.Background(), db, backend == processbinding.SQLiteLeaseBackend)
				switch scenario {
				case "valid":
					want := processbinding.LeaseStamp{LeaseIdentity: identity, Generation: 7}
					want.Backend = backend
					if err != nil || stamp != want {
						t.Fatalf("stamp=%+v err=%v, want=%+v", stamp, err, want)
					}
				case "uninitialized":
					if err != nil || stamp != (processbinding.LeaseStamp{}) {
						t.Fatalf("uninitialized stamp=%+v err=%v", stamp, err)
					}
				case "missing":
					if !errors.Is(err, ErrProcessGenerationMissing) {
						t.Fatalf("missing singleton cause=%v", err)
					}
				case "read_error":
					if !errors.Is(err, readErr) {
						t.Fatalf("snapshot read cause lost: %v", err)
					}
				default:
					if err == nil || stamp != (processbinding.LeaseStamp{}) {
						t.Fatalf("malformed row admitted: stamp=%+v err=%v", stamp, err)
					}
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
