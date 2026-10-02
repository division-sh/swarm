package releasee2e

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testpostgres"
)

func TestGoldenPostgresStoreCapacityAdmission(t *testing.T) {
	const (
		childEnv = "SWARM_INTERNAL_GOLDEN_CAPACITY_CHILD"
		dsnEnv   = "SWARM_INTERNAL_GOLDEN_CAPACITY_DSN"
		closed   = "capacity_refusal_admin_connections=0"
	)
	if os.Getenv(childEnv) == "1" {
		dsn := os.Getenv(dsnEnv)
		admitted := false
		t.Run("fixture", func(t *testing.T) {
			goldenPostgresStore(t, dsn)
			admitted = true
		})
		if admitted {
			t.Fatal("golden fixture admitted insufficient server capacity")
		}
		// Observe closure while the child is alive, not merely after process exit.
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var connections int
		err = pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
			err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity
WHERE backend_type = 'client backend' AND usename = current_user AND pid <> pg_backend_pid()`).Scan(&connections)
			return connections == 0, err
		})
		if err != nil {
			t.Fatalf("admin connections after failed fixture = %d: %v", connections, err)
		}
		t.Log(closed)
		return
	}

	probe := testpostgres.StartCapacityProbe(t, testpostgres.RequiredMaxConnections)
	source, err := url.Parse(probe.DSN)
	if err != nil {
		t.Fatal(err)
	}
	password, ok := source.User.Password()
	if !ok || password == "" {
		t.Fatal("private capacity probe must carry a credential-redaction sentinel")
	}
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, populated := range []bool{false, true} {
		name := "empty host"
		if populated {
			name = "existing control and template"
		}
		t.Run(name, func(t *testing.T) {
			if populated {
				probe.Restart(t, testpostgres.RequiredMaxConnections)
				connection, err := testpostgres.ParseConnection(probe.DSN)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
				defer cancel()
				manager, err := testpostgres.NewManager(ctx, connection)
				if err != nil {
					t.Fatalf("seed control database: %v", err)
				}
				sandbox, err := manager.Acquire(ctx, true)
				if err != nil {
					t.Fatalf("seed canonical template: %v", err)
				}
				if err := sandbox.Release(ctx); err != nil {
					t.Fatalf("release seed sandbox: %v", err)
				}
				var control, template bool
				for _, resource := range probe.DatabaseInventory(t) {
					control = control || strings.HasPrefix(resource, "mas_control_v1_")
					template = template || strings.HasPrefix(resource, "mas_template_v1_")
					if strings.HasPrefix(resource, "mas_test_v1_") {
						t.Fatal("seed sandbox survived release")
					}
				}
				if !control || !template {
					t.Fatal("populated refusal proof requires real control and template databases")
				}
			}
			for _, capacity := range []int{100, testpostgres.RequiredMaxConnections - 1} {
				t.Run(fmt.Sprint(capacity), func(t *testing.T) {
					probe.Restart(t, capacity)
					before := probe.DatabaseInventory(t)
					logBefore := probe.Log(t)
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					cmd := exec.CommandContext(ctx, testBinary,
						"-test.run=^TestGoldenPostgresStoreCapacityAdmission$", "-test.v", "-test.timeout=20s")
					cmd.Env = append(os.Environ(), childEnv+"=1", dsnEnv+"="+probe.DSN)
					output, runErr := cmd.CombinedOutput()
					text := string(output)
					if strings.Contains(text, password) || strings.Contains(text, probe.DSN) {
						t.Fatal("golden refusal leaked the probe DSN or credential sentinel")
					}
					var exitErr *exec.ExitError
					if ctx.Err() != nil || !errors.As(runErr, &exitErr) || exitErr.ExitCode() != 1 {
						t.Fatalf("golden refusal child exit = %v, context = %v\n%s", runErr, ctx.Err(), output)
					}
					for _, want := range []string{
						"admit host PostgreSQL:",
						fmt.Sprintf("max_connections=%d, need >= %d", capacity, testpostgres.RequiredMaxConnections),
						"ask the administrator",
						closed,
					} {
						if !strings.Contains(text, want) {
							t.Fatalf("golden refusal child missing %q\n%s", want, text)
						}
					}
					if after := probe.DatabaseInventory(t); !slices.Equal(before, after) {
						t.Fatalf("golden refusal changed database inventory or metadata: before=%q after=%q", before, after)
					}
					logAfter := probe.Log(t)
					if !strings.HasPrefix(logAfter, logBefore) {
						t.Fatal("private server statement log lost its refusal baseline")
					}
					statements := strings.ToUpper(strings.TrimPrefix(logAfter, logBefore))
					if !strings.Contains(statements, "SHOW MAX_CONNECTIONS") {
						t.Fatal("golden refusal did not observe actual server capacity")
					}
					for _, write := range []string{"CREATE ", "ALTER ", "DROP ", "COMMENT ", "INSERT ", "UPDATE ", "DELETE ", "TRUNCATE ", "GRANT ", "REVOKE "} {
						if strings.Contains(statements, write) {
							t.Fatalf("golden refusal issued %q before admission\n%s", write, statements)
						}
					}
				})
			}
		})
	}
	for _, capacity := range []int{testpostgres.RequiredMaxConnections, testpostgres.RequiredMaxConnections + 1} {
		t.Run(fmt.Sprintf("adequate %d", capacity), func(t *testing.T) {
			probe.Restart(t, capacity)
			before := probe.DatabaseInventory(t)
			var diagnostic *sql.DB
			t.Run("fixture create and cleanup", func(t *testing.T) {
				store := goldenPostgresStore(t, probe.DSN)
				diagnostic = store.diagnosticDB
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				var name string
				if err := diagnostic.QueryRowContext(ctx, `SELECT current_database()`).Scan(&name); err != nil {
					t.Fatalf("query actual golden fixture database: %v", err)
				}
				if !strings.HasPrefix(name, "swarm_golden_") {
					t.Fatalf("golden fixture database = %q, want isolated golden database", name)
				}
				want := append(slices.Clone(before), name+"\x00")
				slices.Sort(want)
				if during := probe.DatabaseInventory(t); !slices.Equal(during, want) {
					t.Fatalf("golden create inventory = %q; want %q", during, want)
				}
			})
			if after := probe.DatabaseInventory(t); !slices.Equal(before, after) {
				t.Fatalf("golden cleanup changed baseline inventory or left its database: before=%q after=%q", before, after)
			}
			if diagnostic == nil || diagnostic.Stats().OpenConnections != 0 {
				t.Fatal("golden fixture cleanup did not close its diagnostic connection")
			}
		})
	}
}
