//go:build linux || darwin

package cliapp

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

type confirmationPollContext struct {
	context.Context
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (c *confirmationPollContext) Err() error {
	err := c.Context.Err()
	if c.calls.Add(1) == 2 {
		close(c.entered)
		<-c.release
	}
	return err
}

func confirmationInputFlags(t *testing.T, input *os.File) int {
	t.Helper()
	raw, err := input.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var flags int
	if err := raw.Control(func(fd uintptr) { flags, err = unix.FcntlInt(fd, unix.F_GETFL, 0) }); err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	return flags
}

func TestChannelConfirmationInputCancellationJoinsWithoutClosingStdin(t *testing.T) {
	input, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	defer write.Close()
	before := confirmationInputFlags(t, input)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reader, closeInput, err := channelConfirmationReader(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	owned := reader.(*channelConfirmationInput)
	witness := &confirmationPollContext{Context: ctx, entered: make(chan struct{}), release: make(chan struct{})}
	owned.ctx = witness
	done := make(chan error, 1)
	go func() { _, err := reader.Read(make([]byte, 1)); done <- err }()
	<-witness.entered
	// The last pre-Poll check has already observed nil. Cancellation must wake
	// the descriptor wait itself; no input byte or caller-file close releases it.
	cancel()
	close(witness.release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("read cancellation=%v", err)
		}
	case <-time.After(time.Second):
		_, _ = owned.wakeSend.Write([]byte{1})
		<-done
		_ = closeInput()
		t.Fatal("owned input wait did not join after cancellation")
	}
	if err := closeInput(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-owned.joined:
	default:
		t.Fatal("wakeup callback did not join")
	}
	if after := confirmationInputFlags(t, input); after != before {
		t.Fatalf("borrowed fd flags changed: before=%d after=%d", before, after)
	}
	if _, err := write.WriteString("x"); err != nil {
		t.Fatal(err)
	}
	var value [1]byte
	if _, err := io.ReadFull(input, value[:]); err != nil || value[0] != 'x' {
		t.Fatalf("caller stdin no longer usable: %v", err)
	}
}

func TestChannelConfirmationInputPreservesAnswersAndEOF(t *testing.T) {
	for _, cell := range []struct {
		name, input string
		approved    bool
	}{
		{"yes", "y\n", true}, {"full_yes", "YES\n", true}, {"no", "n\n", false},
		{"blank", "\n", false}, {"EOF", "", false}, {"answer_then_EOF", "yes", true},
	} {
		t.Run(cell.name, func(t *testing.T) {
			input, write, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()
			before := confirmationInputFlags(t, input)
			if _, err := write.WriteString(cell.input); err != nil {
				t.Fatal(err)
			}
			if err := write.Close(); err != nil {
				t.Fatal(err)
			}
			reader, closeInput, err := channelConfirmationReader(t.Context(), input)
			if err != nil {
				t.Fatal(err)
			}
			approved, err := confirmChannelAccount(t.Context(), reader, io.Discard, "[y/N] ")
			if err != nil || approved != cell.approved {
				t.Fatalf("approved=%t err=%v", approved, err)
			}
			if err := closeInput(); err != nil {
				t.Fatal(err)
			}
			if after := confirmationInputFlags(t, input); after != before {
				t.Fatalf("borrowed fd flags changed: before=%d after=%d", before, after)
			}
			if _, err := input.Stat(); err != nil {
				t.Fatalf("normal completion closed stdin: %v", err)
			}
		})
	}
}

type confirmationCancellingReader struct {
	cancel context.CancelFunc
	input  io.Reader
}

func (r confirmationCancellingReader) Read(p []byte) (int, error) {
	n, err := r.input.Read(p)
	r.cancel()
	return n, err
}

func TestChannelConfirmationCancellationWinsOverAnswerOrEOF(t *testing.T) {
	for _, input := range []string{"yes\n", "n\n", ""} {
		t.Run(input, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			approved, err := confirmChannelAccount(ctx, confirmationCancellingReader{cancel: cancel, input: strings.NewReader(input)}, io.Discard, "[y/N] ")
			if approved || !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel became an answer: approved=%t err=%v", approved, err)
			}
		})
	}
}
