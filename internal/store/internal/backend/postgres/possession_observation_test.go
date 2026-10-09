package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil"

	"github.com/lib/pq"
)

func TestPossessionObservationDialPreservesEarlierNativeConnectDeadline(t *testing.T) {
	phase, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	dialer := &observationDialer{ctx: phase}
	dialer.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 100*time.Millisecond {
			t.Fatalf("native connect deadline was lost: %v", deadline)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if _, err := dialer.DialTimeout("tcp", "unused", 10*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("earlier native deadline: %v", err)
	}
}

func TestPossessionObservationBoundsSilentNativeHandshakeAndJoinsDisposal(t *testing.T) {
	for _, cancelEarly := range []bool{false, true} {
		t.Run(strconv.FormatBool(cancelEarly), func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			accepted := make(chan net.Conn, 1)
			joined := make(chan error, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					joined <- err
					return
				}
				accepted <- conn
				defer conn.Close()
				var buffer [4096]byte
				for {
					if _, err := conn.Read(buffer[:]); err != nil {
						joined <- nil
						return
					}
				}
			}()
			cfg, err := pq.NewConfig("host=127.0.0.1 port=" + strconv.Itoa(listener.Addr().(*net.TCPAddr).Port) + " dbname=probe user=probe sslmode=disable")
			if err != nil {
				t.Fatal(err)
			}
			connector, err := pq.NewConnectorConfig(cfg)
			if err != nil {
				t.Fatal(err)
			}
			// The pool is only the selected configuration owner; the probe must
			// construct an independent session and cannot wait on this pool's I/O.
			db := sql.OpenDB(connector)
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			backend, err := NewWithInspectionConfig(db, cfg)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			result := make(chan error, 1)
			started := time.Now()
			go func() {
				available, err := backend.ObserveAdvisoryLock(ctx, "exact-test-key")
				if available {
					err = errors.Join(err, errors.New("silent handshake fabricated available lock"))
				}
				result <- err
			}()
			select {
			case <-accepted:
			case <-time.After(time.Second):
				t.Fatal("native connection was not attempted")
			}
			if cancelEarly {
				cancel()
			}
			select {
			case err := <-result:
				want := context.DeadlineExceeded
				if cancelEarly {
					want = context.Canceled
				}
				if !errors.Is(err, want) {
					t.Fatalf("caller outcome was lost: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("native handshake escaped the observation deadline")
			}
			if time.Since(started) >= time.Second {
				t.Fatal("deadline was accounting only, not an I/O bound")
			}
			select {
			case err := <-joined:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("observation returned without disposing its native connection")
			}
		})
	}
}

func TestPossessionObservationNativeQueryUnlockCloseAndCancellationFailures(t *testing.T) {
	for _, boundary := range []string{"acquire_read", "acquire_write", "unlock_read", "unlock_write", "close", "cancel_after_acquire"} {
		t.Run(boundary, func(t *testing.T) {
			dsn, db, _ := testutil.StartEmptyPostgres(t)
			cfg, err := pq.NewConfig(dsn)
			if err != nil {
				t.Fatal(err)
			}
			backend, err := NewWithInspectionConfig(db, cfg)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			witness := errors.New("native possession " + boundary)
			dialer := &observationDialer{dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
				if err != nil {
					return nil, err
				}
				return &possessionFailureSocket{Conn: conn, boundary: boundary, witness: witness, cancel: cancel}, nil
			}}
			available, err := backend.observeAdvisoryLock(ctx, "native-fault-key", dialer)
			if err == nil || available {
				t.Fatalf("hostile native outcome admitted: available=%t err=%v", available, err)
			}
			if boundary == "cancel_after_acquire" {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation lost: %v", err)
				}
			} else if !errors.Is(err, witness) {
				t.Fatalf("native failure omitted: %v", err)
			}
			// Disposal is checked independently on a real session, without using
			// the observation as a future acquisition permit.
			contender, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer contender.Close()
			deadline := time.Now().Add(time.Second)
			for {
				var held bool
				if err := contender.QueryRowContext(context.Background(), tryAdvisoryLockSQL, "native-fault-key").Scan(&held); err != nil {
					t.Fatal(err)
				}
				if held {
					var released bool
					if err := contender.QueryRowContext(context.Background(), unlockAdvisoryLockSQL, "native-fault-key").Scan(&released); err != nil || !released {
						t.Fatalf("contender release: %t %v", released, err)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("ambiguous observation escaped a native lock")
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}

type possessionFailureSocket struct {
	net.Conn
	mu            sync.Mutex
	boundary      string
	witness       error
	cancel        context.CancelFunc
	failRead      bool
	readBytes     []byte
	deadlineCalls int
}

func (s *possessionFailureSocket) Write(data []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	acquire := bytes.Contains(data, []byte("pg_try_advisory_lock"))
	unlock := bytes.Contains(data, []byte("pg_advisory_unlock"))
	query := bytes.Contains(data, []byte("SELECT 1"))
	rollback := bytes.Contains(data, []byte("ROLLBACK"))
	if (acquire && s.boundary == "acquire_write") || (unlock && s.boundary == "unlock_write") || (query && s.boundary == "query_write") || (rollback && s.boundary == "rollback_write") {
		return 0, s.witness
	}
	if (acquire && s.boundary == "acquire_read") || (unlock && s.boundary == "unlock_read") || (query && s.boundary == "query_read") || (rollback && s.boundary == "rollback_read") {
		s.failRead = true
	}
	return s.Conn.Write(data)
}

func (s *possessionFailureSocket) SetDeadline(deadline time.Time) error {
	s.mu.Lock()
	s.deadlineCalls++
	fail := s.boundary == "cleanup_bind" && s.deadlineCalls == 2
	s.mu.Unlock()
	err := s.Conn.SetDeadline(deadline)
	if fail {
		return errors.Join(err, s.witness)
	}
	return err
}

func (s *possessionFailureSocket) Read(data []byte) (int, error) {
	s.mu.Lock()
	fail := s.failRead
	s.mu.Unlock()
	if fail {
		return 0, s.witness
	}
	n, err := s.Conn.Read(data)
	s.mu.Lock()
	s.readBytes = append(s.readBytes, data[:n]...)
	acquired := s.boundary == "cancel_after_acquire" && bytes.Contains(s.readBytes, []byte{'D', 0, 0, 0, 11, 0, 1, 0, 0, 0, 1, 't'})
	s.mu.Unlock()
	if acquired {
		s.cancel()
	}
	return n, err
}

func (s *possessionFailureSocket) Close() error {
	err := s.Conn.Close()
	if s.boundary == "close" {
		return errors.Join(err, s.witness)
	}
	return err
}
