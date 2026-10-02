package testutil

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

func TestPostgresHelperCapacityAdmissionNative(t *testing.T) {
	probe := testpostgres.StartCapacityProbe(t, 100)
	for _, capacity := range []int{100, 299} {
		if capacity != 100 {
			probe.Restart(t, capacity)
		}
		for _, kind := range []string{"template", "empty"} {
			t.Run(strconv.Itoa(capacity)+"/"+kind, func(t *testing.T) {
				before, offset := probe.DatabaseInventory(t), len(probe.Log(t))
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPostgresCapacityHelperProcess$")
				command.Env = append(os.Environ(), testpostgres.SourceEnv+"="+probe.DSN, "SWARM_CAPACITY_HELPER_KIND="+kind)
				out, err := command.CombinedOutput()
				if err == nil || !strings.Contains(string(out), "max_connections="+strconv.Itoa(capacity)+", need >= 300") {
					t.Fatalf("%s accepted insufficient server: %v\n%s", kind, err, out)
				}
				if strings.Contains(string(out), "capacity-secret-sentinel") {
					t.Fatal("helper refusal exposed credentials")
				}
				if after := probe.DatabaseInventory(t); !reflect.DeepEqual(before, after) {
					t.Fatalf("helper refusal changed inventory: %q -> %q", before, after)
				}
				if regexp.MustCompile(`(?im)(?:statement:|execute [^:]*:)\s*(?:CREATE|ALTER|DROP|INSERT|UPDATE|DELETE|COMMENT|TRUNCATE|DO|CALL)\b`).MatchString(probe.Log(t)[offset:]) {
					t.Fatal("helper refusal executed a resource mutation")
				}
			})
		}
	}
}

func TestPostgresCapacityHelperProcess(t *testing.T) {
	switch os.Getenv("SWARM_CAPACITY_HELPER_KIND") {
	case "template":
		StartPostgres(t)
	case "empty":
		StartEmptyPostgres(t)
	}
}

func TestPostgresManagerCapacityCacheNative(t *testing.T) {
	probe := testpostgres.StartCapacityProbe(t, 100)
	connection, err := testpostgres.ParseConnection(probe.DSN)
	if err != nil {
		t.Fatal(err)
	}
	key, err := connection.String()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		postgresManagers.Lock()
		delete(postgresManagers.bySource, key)
		postgresManagers.Unlock()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	manager, err := postgresManagerForConnection(ctx, connection)
	var capacity *testpostgres.CapacityError
	if manager != nil || !errors.As(err, &capacity) || capacity.Have != 100 {
		t.Fatalf("failed manager admitted: %v, %v", manager, err)
	}
	postgresManagers.Lock()
	cached := postgresManagers.bySource[key]
	postgresManagers.Unlock()
	if cached != nil {
		t.Fatal("failed admission was cached")
	}
	probe.Restart(t, 300)
	offset := len(probe.Log(t))
	manager, err = postgresManagerForConnection(ctx, connection)
	if err != nil {
		t.Fatal(err)
	}
	reused, err := postgresManagerForConnection(ctx, connection)
	if err != nil || reused != manager {
		t.Fatalf("successful manager cache changed: %v", err)
	}
	t.Setenv(testpostgres.SourceEnv, probe.DSN)
	StartPostgres(t)
	StartEmptyPostgres(t)
	if count := strings.Count(probe.Log(t)[offset:], "SHOW max_connections"); count != 1 {
		t.Fatalf("successful manager revalidated %d times, want 1", count)
	}
	if _, err := testpostgres.NewManager(ctx, connection); err != nil {
		t.Fatal(err)
	}
	if count := strings.Count(probe.Log(t)[offset:], "SHOW max_connections"); count != 2 {
		t.Fatalf("independent manager skipped fresh admission: %d checks", count)
	}
}
