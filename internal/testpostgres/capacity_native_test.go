package testpostgres_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testpostgres"
)

func capacityConnection(t *testing.T, dsn string) testpostgres.Connection {
	t.Helper()
	connection, err := testpostgres.ParseConnection(dsn)
	if err != nil {
		t.Fatal(err)
	}
	return connection
}

func assertCapacityRefusal(t *testing.T, manager *testpostgres.Manager, err error, have int) {
	t.Helper()
	var capacity *testpostgres.CapacityError
	if manager != nil || !errors.As(err, &capacity) || capacity.Have != have || capacity.Need != 300 {
		t.Fatalf("capacity %d: manager admitted=%t, error=%v", have, manager != nil, err)
	}
	if strings.Contains(err.Error(), "capacity-secret-sentinel") {
		t.Fatal("capacity refusal exposed credentials")
	}
}

func assertNoCapacityMutation(t *testing.T, probe *testpostgres.CapacityProbe, before []string, logOffset int) {
	t.Helper()
	if after := probe.DatabaseInventory(t); !reflect.DeepEqual(before, after) {
		t.Fatalf("refusal changed database names or metadata:\nbefore=%q\nafter=%q", before, after)
	}
	statements := probe.Log(t)[logOffset:]
	if !strings.Contains(statements, "SHOW max_connections") {
		t.Fatal("admission did not observe capacity")
	}
	if regexp.MustCompile(`(?im)(?:statement:|execute [^:]*:)\s*(?:CREATE|ALTER|DROP|INSERT|UPDATE|DELETE|COMMENT|TRUNCATE|DO|CALL)\b`).MatchString(statements) {
		t.Fatalf("refusal executed resource mutations:\n%s", statements)
	}
}

func TestManagerCapacityAdmissionNative(t *testing.T) {
	probe := testpostgres.StartCapacityProbe(t, 100)
	connection := capacityConnection(t, probe.DSN)
	for _, capacity := range []int{100, 299} {
		t.Run(strconv.Itoa(capacity), func(t *testing.T) {
			if capacity != 100 {
				probe.Restart(t, capacity)
			}
			before, offset := probe.DatabaseInventory(t), len(probe.Log(t))
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			manager, err := testpostgres.NewManager(ctx, connection)
			assertCapacityRefusal(t, manager, err, capacity)
			assertNoCapacityMutation(t, probe, before, offset)
			t.Setenv(testpostgres.SourceEnv, probe.DSN)
			manager, err = testpostgres.ManagerFromEnvironment(ctx)
			assertCapacityRefusal(t, manager, err, capacity)
			if !strings.Contains(err.Error(), "no Docker fallback") {
				t.Fatalf("host refusal lost source authority: %v", err)
			}
			assertNoCapacityMutation(t, probe, before, offset)
		})
	}
	for _, capacity := range []int{300, 301} {
		t.Run(strconv.Itoa(capacity), func(t *testing.T) {
			probe.Restart(t, capacity)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			manager, err := testpostgres.NewManager(ctx, connection)
			if err != nil {
				t.Fatal(err)
			}
			for _, template := range []bool{false, true} {
				sandbox, err := manager.Acquire(ctx, template)
				if err != nil {
					t.Fatal(err)
				}
				var value int
				if err := sandbox.DB.QueryRowContext(ctx, "SELECT 1").Scan(&value); err != nil || value != 1 {
					t.Fatalf("admitted sandbox unavailable: %d, %v", value, err)
				}
				if err := sandbox.Release(ctx); err != nil {
					t.Fatal(err)
				}
			}
			// Host admission must not impose Docker's disposable durability settings.
			db, err := connection.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for _, setting := range []string{"fsync", "synchronous_commit", "full_page_writes"} {
				var actual string
				if err := db.QueryRowContext(ctx, "SHOW "+setting).Scan(&actual); err != nil || actual != "on" {
					t.Fatalf("host %s changed: %q, %v", setting, actual, err)
				}
			}
		})
	}
}

func TestManagerCapacityRefusesExistingResourcesNative(t *testing.T) {
	probe := testpostgres.StartCapacityProbe(t, 300)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	connection := capacityConnection(t, probe.DSN)
	manager, err := testpostgres.NewManager(ctx, connection)
	if err != nil {
		t.Fatal(err)
	}
	sandbox, err := manager.Acquire(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := sandbox.Release(ctx); err != nil {
		t.Fatal(err)
	}
	// A crashed allocator leaves an owned sandbox that a new admitted manager
	// would reconcile. Refusal must precede that deletion as well as creation.
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestManagerCapacityOrphanHelper$")
	command.Env = append(os.Environ(), "SWARM_CAPACITY_ORPHAN_DSN="+probe.DSN)
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("seed interrupted allocator: %v\n%s", err, out)
	}
	before := probe.DatabaseInventory(t)
	for _, prefix := range []string{"mas_control_v1_", "mas_template_v1_", "mas_test_v1_"} {
		found := false
		for _, database := range before {
			found = found || strings.HasPrefix(database, prefix)
		}
		if !found {
			t.Fatalf("missing existing %s resource: %q", prefix, before)
		}
	}
	probe.Restart(t, 299)
	offset := len(probe.Log(t))
	manager, err = testpostgres.NewManager(ctx, connection)
	assertCapacityRefusal(t, manager, err, 299)
	assertNoCapacityMutation(t, probe, before, offset)
}

func TestManagerCapacityOrphanHelper(t *testing.T) {
	dsn := os.Getenv("SWARM_CAPACITY_ORPHAN_DSN")
	if dsn == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	manager, err := testpostgres.NewManager(ctx, capacityConnection(t, dsn))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Acquire(ctx, false); err != nil {
		t.Fatal(err)
	}
}
