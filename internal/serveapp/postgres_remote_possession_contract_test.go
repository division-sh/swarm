package serveapp

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
)

// Closing the client leg while retaining the server leg models detected local
// connection loss whose remote backend has not observed disconnection yet.
func contractPartitionProxy(t *testing.T, target string) (address string, cut, release func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var clients, servers []net.Conn
	partitioned := false
	released := make(chan struct{})
	var releaseOnce sync.Once
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			client, err := l.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			closed := partitioned
			clients = append(clients, client)
			mu.Unlock()
			if closed {
				_ = client.Close()
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer client.Close()
				up, err := net.DialTimeout("tcp", target, time.Second)
				if err != nil {
					return
				}
				mu.Lock()
				servers = append(servers, up)
				mu.Unlock()
				frontDone := make(chan struct{})
				go func() {
					defer close(frontDone)
					_, _ = io.Copy(up, client)
					mu.Lock()
					hold := partitioned
					mu.Unlock()
					if hold {
						<-released
					}
					_ = up.Close()
				}()
				_, _ = io.Copy(client, up)
				_ = client.Close()
				<-frontDone
			}()
		}
	}()
	cut = func() {
		mu.Lock()
		partitioned = true
		for _, c := range clients {
			_ = c.Close()
		}
		mu.Unlock()
	}
	release = func() {
		releaseOnce.Do(func() {
			close(released)
			mu.Lock()
			for _, c := range servers {
				_ = c.Close()
			}
			mu.Unlock()
		})
	}
	t.Cleanup(func() { _ = l.Close(); cut(); release(); wg.Wait() })
	return l.Addr().String(), cut, release
}

func TestServePostgresRemotePossessionOutlivesLocalLoss(t *testing.T) {
	server := newContractPostgresServer(t)
	var port string
	if _, err := fmt.Sscanf(server.options, "-h 127.0.0.1 -p %s", &port); err != nil {
		t.Fatal(err)
	}
	address, cut, release := contractPartitionProxy(t, net.JoinHostPort("127.0.0.1", port))
	host, p, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	routed := fmt.Sprintf("host=%s port=%s user=swarm_probe dbname=postgres sslmode=disable connect_timeout=2", host, p)
	observer, err := sql.Open("postgres", server.dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = observer.Close() })
	stubServeRuntimeWorkspaceLifecycle(t)
	dsn := routed
	oldBuild := buildStoresForServe
	buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
		return storeselected.OpenRuntime(ctx, storeselected.RuntimeRequest{Selection: storebackend.Selection{Backend: storebackend.BackendPostgres}, PostgresDSN: dsn, SessionLockTTL: runtimeSessionLockTTL(cfg)})
	}
	t.Cleanup(func() { buildStoresForServe = oldBuild })
	opts := cliapp.ServeOptions{ConfigPath: writeServeRuntimeTestConfig(t), SourceRoot: filepath.Join("tests", "tier8-boot-verification", "test-boot-success"), PlatformSpecPath: defaultPlatformSpecPath, StoreMode: "postgres", StoreModeSet: true, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true, ShutdownGrace: time.Second}
	first := startServeRuntimeTestProcess(t, opts)
	first.waitForReadyLine()
	const ownershipSQL = `SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND classid::bigint=CASE WHEN hashtext($1)<0 THEN 4294967295::bigint ELSE 0::bigint END AND objid::bigint=(hashtext($1)::bigint & 4294967295::bigint) AND objsubid=1 LIMIT 1`
	var predecessor int
	if err := observer.QueryRow(ownershipSQL, "swarm:runtime:shared-store-owner").Scan(&predecessor); err != nil {
		t.Fatal(err)
	}
	cut()
	if code, done := first.waitForExit(15 * time.Second); !done || code == 0 {
		t.Fatalf("local connection loss did not fail closed: done=%t code=%d\n%s", done, code, first.outputString())
	}
	var retained int
	if err := observer.QueryRow(ownershipSQL, "swarm:runtime:shared-store-owner").Scan(&retained); err != nil || retained != predecessor {
		t.Fatalf("test lost remote hold: pid=%d want=%d err=%v", retained, predecessor, err)
	}
	dsn = server.dsn
	refused := startServeRuntimeTestProcess(t, opts)
	if code, done := refused.waitForExit(10 * time.Second); !done || code == 0 || serveOutputIsReady(refused.outputString()) {
		t.Fatalf("successor bypassed remote ownership: done=%t code=%d\n%s", done, code, refused.outputString())
	}
	release()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var count int
		if err := observer.QueryRow(`SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND pid=$1`, predecessor).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server did not release predecessor locks")
		}
		time.Sleep(10 * time.Millisecond)
	}
	next := startServeRuntimeTestProcess(t, opts)
	next.waitForReadyLine()
	var successor int
	if err := observer.QueryRow(ownershipSQL, "swarm:runtime:shared-store-owner").Scan(&successor); err != nil || successor == predecessor {
		t.Fatalf("no exact new backend possession: %d %v", successor, err)
	}
	if code := next.stop(); code != 0 {
		t.Fatalf("healthy new owner exit=%d\n%s", code, next.outputString())
	}
	t.Logf("remote PID %d held authority after local failure; supported successor refused until remote release, then acquired PID %d", predecessor, successor)
}
