package testpostgres

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// CapacityProbe owns only its disposable, Unix-socket-only test server.
type CapacityProbe struct {
	DSN     string
	bin     string
	data    string
	socket  string
	log     string
	running bool
}

// StartCapacityProbe never changes the inherited shared test service.
// TEST_POSTGRES_BIN can explicitly select the native PostgreSQL tools.
func StartCapacityProbe(t *testing.T, capacity int) *CapacityProbe {
	t.Helper()
	bin := os.Getenv("TEST_POSTGRES_BIN")
	if bin == "" {
		pgConfig, err := exec.LookPath("pg_config")
		if err != nil {
			t.Skip("native capacity proof requires pg_config or TEST_POSTGRES_BIN")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		out, err := exec.CommandContext(ctx, pgConfig, "--bindir").Output()
		cancel()
		if err != nil {
			t.Fatalf("resolve native PostgreSQL tools: %v", err)
		}
		bin = strings.TrimSpace(string(out))
	}
	for _, name := range []string{"initdb", "pg_ctl"} {
		if _, err := os.Stat(filepath.Join(bin, name)); err != nil {
			if os.Getenv("TEST_POSTGRES_BIN") != "" {
				t.Fatalf("explicit native PostgreSQL tool %s: %v", name, err)
			}
			t.Skipf("native capacity proof requires %s", filepath.Join(bin, name))
		}
	}
	// Keep the socket below sockaddr_un's limit even with long test names.
	root, err := os.MkdirTemp("", "swarm-capacity-")
	if err != nil {
		t.Fatal(err)
	}
	source := &url.URL{Scheme: "postgres", User: url.UserPassword("swarm_capacity_probe", "capacity-secret-sentinel"), Path: "/postgres"}
	query := url.Values{"host": {root}, "port": {"5432"}, "sslmode": {"disable"}, "connect_timeout": {"2"}}
	source.RawQuery = query.Encode()
	p := &CapacityProbe{
		bin: bin, data: filepath.Join(root, "data"), socket: root,
		log: filepath.Join(root, "postgres.log"),
		DSN: source.String(),
	}
	t.Cleanup(func() {
		p.stop(t)
		if err := os.RemoveAll(root); err != nil {
			t.Errorf("remove private PostgreSQL cluster: %v", err)
		}
	})
	// This disposable admission fixture does not test bootstrap crash durability.
	// Keep server durability defaults; avoid syncing the entire initial cluster.
	p.run(t, "initdb", "-D", p.data, "-U", "swarm_capacity_probe", "--auth-local=trust", "--auth-host=reject", "--no-locale", "--encoding=UTF8", "--no-sync")
	p.start(t, capacity)
	return p
}

func (p *CapacityProbe) run(t *testing.T, name string, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, filepath.Join(p.bin, name), args...).CombinedOutput()
	if err != nil {
		t.Fatalf("private PostgreSQL %s: %v\n%s", name, err, out)
	}
}

func (p *CapacityProbe) start(t *testing.T, capacity int) {
	t.Helper()
	options := fmt.Sprintf("-h '' -k %s -p 5432 -c max_connections=%d -c log_statement=all", p.socket, capacity)
	p.run(t, "pg_ctl", "-D", p.data, "-l", p.log, "-o", options, "-w", "start")
	p.running = true
}

func (p *CapacityProbe) stop(t *testing.T) {
	t.Helper()
	if p.running {
		p.run(t, "pg_ctl", "-D", p.data, "-m", "fast", "-w", "stop")
		p.running = false
	}
}

func (p *CapacityProbe) Restart(t *testing.T, capacity int) {
	t.Helper()
	p.stop(t)
	p.start(t, capacity)
}

func (p *CapacityProbe) Log(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(p.log)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func (p *CapacityProbe) DatabaseInventory(t *testing.T) []string {
	t.Helper()
	connection, err := ParseConnection(p.DSN)
	if err != nil {
		t.Fatal(err)
	}
	db, err := connection.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `SELECT datname, COALESCE(shobj_description(oid, 'pg_database'), '') FROM pg_database ORDER BY datname`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var inventory []string
	for rows.Next() {
		var name, metadata string
		if err := rows.Scan(&name, &metadata); err != nil {
			t.Fatal(err)
		}
		inventory = append(inventory, name+"\x00"+metadata)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return inventory
}
