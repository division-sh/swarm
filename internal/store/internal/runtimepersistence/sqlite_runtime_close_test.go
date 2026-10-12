package runtimepersistence

import (
	"context"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	storeownership "github.com/division-sh/swarm/internal/store/internal/startupownership"
)

type sqliteRuntimeCloseFaultConnector struct {
	*pipelineCrashConnector
	started, resume, joined chan struct{}
	once                    sync.Once
	fault                   error
}

func (c *sqliteRuntimeCloseFaultConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.pipelineCrashConnector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &sqliteRuntimeCloseFaultConn{pipelineCrashConn: conn.(*pipelineCrashConn), owner: c}, nil
}

type sqliteRuntimeCloseFaultConn struct {
	*pipelineCrashConn
	owner *sqliteRuntimeCloseFaultConnector
}

func (c *sqliteRuntimeCloseFaultConn) Close() error {
	c.owner.once.Do(func() { close(c.owner.started) })
	<-c.owner.resume
	err := c.pipelineCrashConn.Close()
	close(c.owner.joined)
	return errors.Join(err, c.owner.fault)
}

func TestSQLiteRuntimeCloseErrorReleasesConstructionAfterNativeJoin(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.db")
	guard, err := storeownership.AcquireSQLiteConstructionGuard(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := guard.Release(); err != nil {
			t.Error(err)
		}
	})
	fault := errors.New("injected native schema close failure")
	control := &sqliteRuntimeCloseFaultConnector{
		pipelineCrashConnector: &pipelineCrashConnector{Connector: pipelineCrashSQLiteConnector{path: path}},
		started:                make(chan struct{}), resume: make(chan struct{}), joined: make(chan struct{}), fault: fault,
	}
	var resumeOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(control.resume) }) }
	t.Cleanup(resume)
	store, err := NewSQLiteRuntimeStore(path, nativeSQLiteFixtureConstruction{connector: control, possession: guard})
	if err != nil {
		t.Fatal(err)
	}
	var closeDone <-chan struct{}
	t.Cleanup(func() {
		resume()
		if closeDone != nil {
			select {
			case <-closeDone:
			case <-time.After(5 * time.Second):
				t.Error("native close cleanup did not join; possession remains held")
				return
			}
		}
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := store.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	joined := make(chan struct{})
	closeDone = joined
	go func() { defer close(joined); closed <- store.Close() }()
	select {
	case <-control.started:
	case <-time.After(5 * time.Second):
		t.Fatal("native close did not start")
	}
	if contender, err := storeownership.AcquireSQLiteConstructionGuard(path); err == nil {
		_ = contender.Release()
		t.Fatal("construction possession released before native close joined")
	}
	select {
	case err := <-closed:
		t.Fatalf("runtime close returned before native close joined: %v", err)
	default:
	}
	resume()
	select {
	case err := <-closed:
		if !errors.Is(err, fault) {
			t.Fatalf("native close error lost: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runtime close did not join")
	}
	select {
	case <-control.joined:
	default:
		t.Fatal("runtime close returned before actual driver close")
	}
	if err := store.Ping(context.Background()); err == nil || !strings.Contains(err.Error(), "database is closed") {
		t.Fatalf("failed close did not terminally dispose the DB: %v", err)
	}
	successorGuard, err := storeownership.AcquireSQLiteConstructionGuard(path)
	if err != nil {
		t.Fatalf("failed schema close retained construction possession: %v", err)
	}
	t.Cleanup(func() {
		if err := successorGuard.Release(); err != nil {
			t.Error(err)
		}
	})
	successor, err := NewSQLiteRuntimeStore(path, nativeSQLiteFixtureConstruction{
		connector: pipelineCrashSQLiteConnector{path: path}, possession: successorGuard,
	})
	if err != nil {
		t.Fatalf("failed schema close retained construction possession: %v", err)
	}
	t.Cleanup(func() {
		if err := successor.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := successor.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("repeat close changed joined disposition: %v", err)
	}
}
