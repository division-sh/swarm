package pq

import (
	"bufio"
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lib/pq/internal/proto"
)

type nativeTestSocket struct {
	read     func([]byte) (int, error)
	write    func([]byte) (int, error)
	closeErr error
	closed   atomic.Int32
}

func (s *nativeTestSocket) Read(p []byte) (int, error) {
	if s.read != nil {
		return s.read(p)
	}
	return 0, io.EOF
}
func (s *nativeTestSocket) Write(p []byte) (int, error) {
	if s.write != nil {
		return s.write(p)
	}
	return len(p), nil
}
func (s *nativeTestSocket) Close() error                   { s.closed.Add(1); return s.closeErr }
func (*nativeTestSocket) LocalAddr() net.Addr              { return nil }
func (*nativeTestSocket) RemoteAddr() net.Addr             { return nil }
func (*nativeTestSocket) SetDeadline(time.Time) error      { return nil }
func (*nativeTestSocket) SetReadDeadline(time.Time) error  { return nil }
func (*nativeTestSocket) SetWriteDeadline(time.Time) error { return nil }

type nativeTestDialer struct {
	socket net.Conn
	err    error
	calls  atomic.Int32
}

type nativeTestWriteObserver struct {
	net.Conn
	entered chan struct{}
}

func (c nativeTestWriteObserver) Write(p []byte) (int, error) {
	close(c.entered)
	return c.Conn.Write(p)
}

func (d *nativeTestDialer) Dial(string, string) (net.Conn, error) {
	d.calls.Add(1)
	return d.socket, d.err
}
func (d *nativeTestDialer) DialTimeout(n, a string, _ time.Duration) (net.Conn, error) {
	return d.Dial(n, a)
}

func nativeGateFixture(t *testing.T, ctx context.Context) (*conn, *OperationScope, *nativeTestDialer, <-chan struct{}, func()) {
	t.Helper()
	sent := make(chan struct{})
	release := make(chan struct{})
	var sentOnce, releaseOnce sync.Once
	cancelSocket := &nativeTestSocket{
		write: func(p []byte) (int, error) {
			if len(p) != 16 || binary.BigEndian.Uint32(p[4:8]) != uint32(proto.CancelRequestCode) {
				return 0, errors.New("not an actual CancelRequest packet")
			}
			sentOnce.Do(func() { close(sent) })
			return len(p), nil
		},
		read: func([]byte) (int, error) { <-release; return 0, io.EOF },
	}
	dialer := &nativeTestDialer{socket: cancelSocket}
	socket := &nativeTestSocket{}
	cn := &conn{c: socket, buf: bufio.NewReader(socket), dialer: dialer,
		cfg: Config{Host: "localhost", Port: 5432, SSLMode: SSLModeDisable}, txnStatus: txnStatusIdle}
	finish := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(finish)
	return cn, NewOperationScope(ctx), dialer, sent, finish
}

func nativeTestWait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("native barrier did not settle")
	}
}

func nativeTestOperation(t *testing.T, cn *conn, scope *OperationScope, kind string) *nativeOperation {
	t.Helper()
	op, err := cn.startNative(context.WithoutCancel(scope.Context(scope.owner)), kind)
	if err != nil {
		t.Fatal(err)
	}
	if err := op.beforeSend(); err != nil {
		t.Fatal(err)
	}
	op.afterSend(nil)
	return op
}

func TestNativeScopeContextAndBeginAdmission(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cn, scope, dialer, _, _ := nativeGateFixture(t, ctx)
	if scope.Context(ctx).Done() != ctx.Done() {
		t.Fatal("caller Done was detached")
	}
	var writes atomic.Int32
	cn.c.(*nativeTestSocket).write = func(p []byte) (int, error) { writes.Add(1); return len(p), nil }
	cancel()
	_, err := cn.BeginTx(context.WithoutCancel(scope.Context(ctx)), driver.TxOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Begin = %v", err)
	}
	if writes.Load() != 0 || dialer.calls.Load() != 0 {
		t.Fatal("canceled Begin sent work")
	}
	if err := scope.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeScopeBeginArmedDuringNativeExecution(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cn, scope, _, sent, release := nativeGateFixture(t, ctx)
	cn.noticeHandler = func(*Error) {
		cancel()
		<-sent
		release()
	}
	frames := nativeTestFrame(byte(proto.NoticeResponse), []byte("SNOTICE\x00Mbegin active\x00\x00"))
	frames = append(frames, nativeTestFrame(byte(proto.ErrorResponse), []byte("SERROR\x00C57014\x00Mquery canceled\x00\x00"))...)
	frames = append(frames, nativeTestFrame(byte(proto.ReadyForQuery), []byte{'I'})...)
	cn.buf = bufio.NewReader(bytes.NewReader(frames))
	tx, err := cn.BeginTx(context.WithoutCancel(scope.Context(ctx)), driver.TxOptions{})
	if tx != nil || !errors.Is(err, context.Canceled) || scope.Wait() != nil {
		t.Fatalf("native Begin cancellation = %v / %v / %v", tx, err, scope.Err())
	}
	if cn.native != nil || cn.txnScope != nil || !cn.IsValid() {
		t.Fatal("settled canceled Begin retained work or poisoned its session")
	}
}

func TestNativeScopeCancellationInterruptsBlockedWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cn, scope, dialer, _, _ := nativeGateFixture(t, ctx)
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	entered := make(chan struct{})
	cn.c = nativeTestWriteObserver{Conn: client, entered: entered}
	op, err := cn.startNative(scope.Context(ctx), "query")
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() {
		message := cn.writeBuf(proto.Query)
		message.string("SELECT 1")
		err := cn.send(message)
		finished <- op.finish(err)
	}()
	nativeTestWait(t, entered)
	cancel()
	select {
	case err := <-finished:
		if err == nil || scope.Wait() == nil || cn.IsValid() {
			t.Fatalf("interrupted write lost uncertainty: %v / %v", err, scope.Err())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation did not interrupt blocked write")
	}
	if dialer.calls.Load() != 0 {
		t.Fatal("cancel packet preceded successful query write")
	}
}

func TestNativeScopeErrorBeforeCancellation(t *testing.T) {
	for _, code := range []ErrorCode{"57014", "23505", "40001"} {
		t.Run(string(code), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cn, scope, dialer, _, _ := nativeGateFixture(t, ctx)
			op := nativeTestOperation(t, cn, scope, "query")
			op.observeResponse(proto.ErrorResponse)
			native := &Error{Code: code}
			op.observeError(native)
			cancel()
			op.observeResponse(proto.ReadyForQuery)
			if err := op.finish(native); !errors.Is(err, native) {
				t.Fatalf("lost native error: %v", err)
			}
			if !errors.Is(scope.Wait(), native) || dialer.calls.Load() != 0 {
				t.Fatal("late cancellation relabeled error")
			}
			if !cn.IsValid() {
				t.Fatal("healthy server failure poisoned connection")
			}
		})
	}
}

func TestNativeScopeCancelActionMustSucceedAndJoin(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "worker_failure"}[fail], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cn, scope, dialer, sent, release := nativeGateFixture(t, ctx)
			workerErr := errors.New("cancel socket close failure")
			if fail {
				dialer.socket.(*nativeTestSocket).closeErr = workerErr
			}
			op := nativeTestOperation(t, cn, scope, "exec")
			cancel()
			nativeTestWait(t, sent)
			op.observeResponse(proto.ErrorResponse)
			native := &Error{Code: "57014"}
			op.observeError(native)
			op.observeResponse(proto.ReadyForQuery)
			result := make(chan error, 1)
			go func() { result <- op.finish(native) }()
			select {
			case err := <-result:
				t.Fatalf("returned before worker joined: %v", err)
			default:
			}
			release()
			err := <-result
			if fail {
				if !errors.Is(err, workerErr) || !errors.Is(err, native) || !errors.Is(scope.Wait(), workerErr) {
					t.Fatalf("lost failure: %v / %v", err, scope.Err())
				}
				if cn.IsValid() || cn.c.(*nativeTestSocket).closed.Load() == 0 {
					t.Fatal("cancel failure did not dispose session")
				}
			} else {
				if !errors.Is(err, context.Canceled) || errors.Is(err, native) || scope.Wait() != nil {
					t.Fatalf("owned result = %v / %v", err, scope.Err())
				}
				if !cn.IsValid() {
					t.Fatal("own cancellation poisoned healthy session")
				}
			}
		})
	}
}

func TestNativeScopeIndependentFailureRacingCancellation(t *testing.T) {
	for _, native := range []error{&Error{Code: "23505"}, io.EOF, errors.New("network failure")} {
		ctx, cancel := context.WithCancel(context.Background())
		cn, scope, _, sent, release := nativeGateFixture(t, ctx)
		op := nativeTestOperation(t, cn, scope, "exec")
		cancel()
		nativeTestWait(t, sent)
		op.observeResponse(proto.ErrorResponse)
		op.observeError(native)
		op.observeResponse(proto.ReadyForQuery)
		release()
		if err := op.finish(native); !errors.Is(err, native) || !errors.Is(scope.Wait(), native) {
			t.Fatalf("lost independent error: %v", err)
		}
	}
}

func nativeTestFrame(code byte, payload []byte) []byte {
	b := make([]byte, 5+len(payload))
	b[0] = code
	binary.BigEndian.PutUint32(b[1:5], uint32(4+len(payload)))
	copy(b[5:], payload)
	return b
}

func TestNativeScopeQueryPanicJoinsCancellationWorker(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		t.Run(map[bool]string{false: "query", true: "prepared_query"}[prepared], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cn, scope, _, sent, release := nativeGateFixture(t, ctx)
			marker := errors.New("notice handler panic")
			cn.noticeHandler = func(*Error) { cancel(); <-sent; panic(marker) }
			frames := nativeTestFrame(byte(proto.NoticeResponse), []byte("SNOTICE\x00Mactive\x00\x00"))
			if prepared {
				frames = append(nativeTestFrame(byte(proto.BindComplete), nil), frames...)
			}
			cn.buf = bufio.NewReader(bytes.NewReader(frames))
			panicked := make(chan any, 1)
			go func() {
				defer func() { panicked <- recover() }()
				if prepared {
					_, _ = (&stmt{cn: cn}).QueryContext(scope.Context(ctx), nil)
				} else {
					_, _ = cn.QueryContext(scope.Context(ctx), "SELECT 1", nil)
				}
			}()
			nativeTestWait(t, sent)
			select {
			case got := <-panicked:
				t.Fatalf("panic escaped before worker join: %v", got)
			default:
			}
			release()
			if got := <-panicked; got != marker {
				t.Fatalf("panic = %v", got)
			}
			if cn.native != nil || cn.IsValid() {
				t.Fatal("panic left active or reusable undrained protocol")
			}
			if scope.Wait() == nil {
				t.Fatal("unsettled panic omitted physical settlement uncertainty")
			}
		})
	}
}

func TestNativeScopeCommitAdmissionAndOutcome(t *testing.T) {
	for _, scenario := range []string{"cancel_before", "cancel_after", "failure_after"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cn, scope, dialer, _, _ := nativeGateFixture(t, ctx)
			cn.operationScope, cn.txnScope, cn.txnStatus = scope, scope, txnStatusIdleInTransaction
			scope.work.Add(1)
			var sent string
			cn.c.(*nativeTestSocket).write = func(p []byte) (int, error) { sent = string(p[5:]); cancel(); return len(p), nil }
			command := "COMMIT\x00"
			if scenario == "cancel_before" {
				command = "ROLLBACK\x00"
				cancel()
			}
			frames := nativeTestFrame(byte(proto.CommandComplete), []byte(command))
			if scenario == "failure_after" {
				frames = nativeTestFrame(byte(proto.ErrorResponse), []byte("SERROR\x00C40001\x00Mserialization failure\x00\x00"))
			}
			frames = append(frames, nativeTestFrame(byte(proto.ReadyForQuery), []byte{'I'})...)
			cn.buf = bufio.NewReader(bytes.NewReader(frames))
			err := cn.Commit()
			if sent != command || dialer.calls.Load() != 0 {
				t.Fatalf("unexpected commit/cancel send: %q / %d", sent, dialer.calls.Load())
			}
			switch scenario {
			case "cancel_before":
				if !errors.Is(err, context.Canceled) || scope.Wait() != nil {
					t.Fatalf("admission = %v / %v", err, scope.Err())
				}
			case "cancel_after":
				if err != nil || scope.Wait() != nil {
					t.Fatalf("proven commit = %v / %v", err, scope.Err())
				}
			case "failure_after":
				var native *Error
				if !errors.As(err, &native) || native.Code != "40001" || scope.Wait() == nil {
					t.Fatalf("lost commit uncertainty: %v", err)
				}
			}
			if cn.txnScope != nil {
				t.Fatal("transaction settlement not joined")
			}
		})
	}
}

func TestNativeScopeImplicitBeginLifetime(t *testing.T) {
	for _, scenario := range []string{"commit", "rollback", "cancel_before", "server_failure", "cancel_during"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cn, _, _, _, _ := nativeGateFixture(t, ctx)
			frames := nativeTestFrame(byte(proto.CommandComplete), []byte("BEGIN\x00"))
			frames = append(frames, nativeTestFrame(byte(proto.ReadyForQuery), []byte{'T'})...)
			switch scenario {
			case "cancel_before":
				cancel()
			case "server_failure":
				frames = nativeTestFrame(byte(proto.ErrorResponse), []byte("SERROR\x00C25006\x00Mbegin refused\x00\x00"))
				frames = append(frames, nativeTestFrame(byte(proto.ReadyForQuery), []byte{'I'})...)
			case "cancel_during":
				cn.c.(*nativeTestSocket).write = func(p []byte) (int, error) {
					cancel()
					return 0, io.ErrClosedPipe
				}
			}
			cn.buf = bufio.NewReader(bytes.NewReader(frames))
			tx, err := cn.BeginTx(ctx, driver.TxOptions{})
			if scenario == "commit" || scenario == "rollback" {
				if err != nil {
					t.Fatal(err)
				}
				scope := cn.txnScope
				if scope == nil || !scope.implicit {
					t.Fatal("implicit transaction has no authority")
				}
				// Statements inside the transaction must still inherit its authority.
				if cn.scopeFor(context.Background()) != scope {
					t.Fatal("transaction statement lost its scope")
				}
				command := "COMMIT\x00"
				if scenario == "rollback" {
					command = "ROLLBACK\x00"
				}
				frames = nativeTestFrame(byte(proto.CommandComplete), []byte(command))
				frames = append(frames, nativeTestFrame(byte(proto.ReadyForQuery), []byte{'I'})...)
				cn.buf = bufio.NewReader(bytes.NewReader(frames))
				if scenario == "commit" {
					err = tx.Commit()
				} else {
					err = tx.Rollback()
				}
				if err != nil || scope.Wait() != nil {
					t.Fatalf("settlement = %v / %v", err, scope.Err())
				}
			} else if err == nil || tx != nil {
				t.Fatalf("failed Begin = %v / %v", tx, err)
			}
			cancel()
			if cn.native != nil || cn.txnScope != nil || cn.operationScope != nil || cn.scopeFor(context.Background()) != nil {
				t.Fatal("implicit Begin authority survived settlement/refusal")
			}
			if scenario != "cancel_during" {
				op, err := cn.startNative(context.Background(), "query")
				if err != nil {
					t.Fatalf("successor inherited failed/settled Begin: %v", err)
				}
				if err := op.finish(nil); err != nil {
					t.Fatal(err)
				}
			} else if cn.IsValid() {
				t.Fatal("failed transport remains reusable")
			}
		})
	}
}

func TestNativeScopeExplicitBindingSurvivesTransaction(t *testing.T) {
	for _, scenario := range []string{"commit", "rollback", "cancel_before", "server_failure"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cn, scope, _, _, _ := nativeGateFixture(t, ctx)
			if err := BindOperationScope(cn, scope); err != nil {
				t.Fatal(err)
			}
			frames := nativeTestFrame(byte(proto.CommandComplete), []byte("BEGIN\x00"))
			frames = append(frames, nativeTestFrame(byte(proto.ReadyForQuery), []byte{'T'})...)
			if scenario == "cancel_before" {
				cancel()
			} else if scenario == "server_failure" {
				frames = nativeTestFrame(byte(proto.ErrorResponse), []byte("SERROR\x00C25006\x00Mbegin refused\x00\x00"))
				frames = append(frames, nativeTestFrame(byte(proto.ReadyForQuery), []byte{'I'})...)
			}
			cn.buf = bufio.NewReader(bytes.NewReader(frames))
			tx, err := cn.BeginTx(context.Background(), driver.TxOptions{})
			if scenario == "commit" || scenario == "rollback" {
				if err != nil {
					t.Fatal(err)
				}
				command := "COMMIT\x00"
				if scenario == "rollback" {
					command = "ROLLBACK\x00"
				}
				frames = nativeTestFrame(byte(proto.CommandComplete), []byte(command))
				frames = append(frames, nativeTestFrame(byte(proto.ReadyForQuery), []byte{'I'})...)
				cn.buf = bufio.NewReader(bytes.NewReader(frames))
				if scenario == "commit" {
					err = tx.Commit()
				} else {
					err = tx.Rollback()
				}
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("Begin unexpectedly succeeded")
			}
			cancel()
			if cn.operationScope != scope || cn.txnScope != nil {
				t.Fatal("explicit cleanup binding lost or transaction unsettled")
			}
			if _, err := cn.startNative(context.Background(), "query"); !errors.Is(err, context.Canceled) {
				t.Fatalf("explicit owner no longer governs admission: %v", err)
			}
			closeErr := errors.New("post-transaction disposal failed")
			cn.c.(*nativeTestSocket).closeErr = closeErr
			if err := cn.Close(); !errors.Is(err, closeErr) || !errors.Is(scope.Wait(), closeErr) {
				t.Fatalf("explicit physical cleanup evidence lost: %v / %v", err, scope.Err())
			}
		})
	}
}

func TestNativeScopePhysicalCloseSettlesFailedRollback(t *testing.T) {
	cn, scope, _, _, _ := nativeGateFixture(t, context.Background())
	cn.operationScope, cn.txnScope = scope, scope
	scope.work.Add(1)
	closeErr := errors.New("physical close failed")
	cn.c.(*nativeTestSocket).closeErr = closeErr
	cn.c.(*nativeTestSocket).write = func([]byte) (int, error) { return 0, io.EOF }
	if err := cn.Close(); !errors.Is(err, closeErr) {
		t.Fatal(err)
	}
	if err := scope.Wait(); !errors.Is(err, closeErr) {
		t.Fatalf("hidden physical failure lost: %v", err)
	}
	if !errors.Is(scope.Err(), io.EOF) {
		t.Fatal("physical termination EOF was collapsed into ErrBadConn")
	}
}

type nativeScopeForwarder struct{ driver.Conn }

func (c nativeScopeForwarder) BindOperationScope(s *OperationScope) error {
	return BindOperationScope(c.Conn, s)
}

func TestNativeScopeBindingAndReset(t *testing.T) {
	cn, scope, _, _, _ := nativeGateFixture(t, context.Background())
	if err := BindOperationScope(nativeScopeForwarder{cn}, scope); err != nil {
		t.Fatal(err)
	}
	op := nativeTestOperation(t, cn, scope, "query")
	if err := BindOperationScope(cn, nil); err == nil {
		t.Fatal("active operation binding replaced")
	}
	op.observeResponse(proto.ReadyForQuery)
	if err := op.finish(nil); err != nil {
		t.Fatal(err)
	}
	if err := BindOperationScope(cn, nil); err != nil {
		t.Fatal(err)
	}
	if cn.operationScope != nil {
		t.Fatal("healthy retained binding not cleared")
	}
	if err := BindOperationScope(cn, scope); err != nil {
		t.Fatal(err)
	}
	if err := cn.ResetSession(context.Background()); err != nil {
		t.Fatal(err)
	}
	if cn.operationScope != nil {
		t.Fatal("pool reuse inherited prior operation owner")
	}
	if err := BindOperationScope(cn, scope); err != nil {
		t.Fatal(err)
	}
	cn.err.set(driver.ErrBadConn)
	if err := cn.ResetSession(context.Background()); err != driver.ErrBadConn {
		t.Fatal(err)
	}
	closeErr := errors.New("discard close failed")
	cn.c.(*nativeTestSocket).closeErr = closeErr
	_ = cn.Close()
	if !errors.Is(scope.Err(), closeErr) {
		t.Fatal("bad reset erased discard evidence")
	}
}

func TestNativeScopeRowsCloseRetainsHiddenFailure(t *testing.T) {
	cn, scope, _, _, _ := nativeGateFixture(t, context.Background())
	op := nativeTestOperation(t, cn, scope, "query")
	frames := nativeTestFrame(byte(proto.ErrorResponse), []byte("SERROR\x00C57014\x00Mserver cancellation\x00\x00"))
	frames = append(frames, nativeTestFrame(byte(proto.ReadyForQuery), []byte{'I'})...)
	cn.buf = bufio.NewReader(bytes.NewReader(frames))
	rs := &rows{cn: cn, native: op}
	_ = rs.Close() // Model a consumer/database/sql discarding Close's error.
	var native *Error
	if !errors.As(scope.Wait(), &native) || native.Code != "57014" {
		t.Fatalf("hidden row-close error lost: %v", scope.Err())
	}
	if !cn.IsValid() {
		t.Fatal("drained server error poisoned healthy connection")
	}
}

func TestNativeScopeRowsNextPanicSettlesGate(t *testing.T) {
	cn, scope, _, _, _ := nativeGateFixture(t, context.Background())
	op := nativeTestOperation(t, cn, scope, "query")
	marker := errors.New("stream notice panic")
	cn.noticeHandler = func(*Error) { panic(marker) }
	cn.buf = bufio.NewReader(bytes.NewReader(nativeTestFrame(byte(proto.NoticeResponse), []byte("SNOTICE\x00Mstream active\x00\x00"))))
	rs := &rows{cn: cn, native: op}
	func() {
		defer func() {
			if got := recover(); got != marker {
				t.Fatalf("panic = %v", got)
			}
		}()
		_ = rs.Next(nil)
	}()
	if cn.native != nil || cn.IsValid() || scope.Wait() == nil {
		t.Fatal("stream panic left live or reusable native protocol")
	}
}
