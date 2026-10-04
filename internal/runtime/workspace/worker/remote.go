package worker

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
)

// Ready precedes execution, so the parent can bind observation to an existing
// remote process without confusing a late launch with a completed one.
type Ready struct {
	Invocation string   `json:"invocation"`
	Identity   Identity `json:"identity"`
}

type Acknowledgement struct {
	Invocation string `json:"invocation"`
	Execute    bool   `json:"execute"`
}

func InvocationArgument(invocation string) (string, error) {
	decoded, err := hex.DecodeString(invocation)
	if err != nil || len(decoded) != 16 || strings.ToLower(invocation) != invocation {
		return "", fmt.Errorf("invalid workspace worker invocation")
	}
	return Argument + "=" + invocation, nil
}

func RunArgument(ctx context.Context, argument string, input io.ReadCloser, output io.Writer) (int, bool) {
	if argument == Argument {
		return Run(ctx, input, output), true
	}
	invocation, remote := strings.CutPrefix(argument, Argument+"=")
	if !remote {
		return 0, false
	}
	if _, err := InvocationArgument(invocation); err != nil {
		return 2, true
	}
	return runRemote(ctx, invocation, input, output), true
}

// ReadFrame bounds allocation before decoding; untrusted frames cannot make the
// native worker or the attachment owner buffer an unbounded line.
func ReadFrame(reader *bufio.Reader) ([]byte, error) {
	var frame []byte
	for {
		fragment, err := reader.ReadSlice('\n')
		payloadBytes := len(frame) + len(fragment)
		if err == nil {
			payloadBytes-- // The framing delimiter is not part of the JSON budget.
		}
		if payloadBytes > MaxBytes {
			return nil, fmt.Errorf("workspace worker frame over budget")
		}
		frame = append(frame, fragment...)
		if err == nil {
			return frame, nil
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return nil, err
		}
	}
}

func DecodeReady(raw []byte) (Ready, error) {
	var ready Ready
	err := decodeMessage(raw, &ready)
	return ready, err
}

func runRemote(ctx context.Context, invocation string, input io.ReadCloser, output io.Writer) int {
	defer input.Close()
	var err error
	input, err = InterruptibleInput(input)
	if err != nil {
		return 2
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer input.Close()
	stop := context.AfterFunc(ctx, func() { _ = input.Close() })
	defer stop()
	reader := bufio.NewReader(input)
	raw, err := ReadFrame(reader)
	var request Request
	if err != nil || decodeMessage(raw, &request) != nil {
		return 2
	}
	if !request.Deadline.IsZero() {
		var stopDeadline context.CancelFunc
		ctx, stopDeadline = context.WithDeadline(ctx, request.Deadline)
		defer stopDeadline()
		stop := context.AfterFunc(ctx, func() { _ = input.Close() })
		defer stop()
	}
	identity, err := ExecutableIdentity()
	if err != nil || json.NewEncoder(output).Encode(Ready{Invocation: invocation, Identity: identity}) != nil {
		return 2
	}
	raw, err = ReadFrame(reader)
	var acknowledgement Acknowledgement
	if err != nil || decodeMessage(raw, &acknowledgement) != nil || !acknowledgement.Execute || acknowledgement.Invocation != invocation {
		return 2
	}
	var finished atomic.Bool
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		// No more input is valid after acknowledgement. EOF (including loss of
		// the Docker attachment) or unsolicited input withdraws execution.
		_, _ = reader.ReadByte()
		if !finished.Load() {
			cancel()
		}
	}()
	result := runRequest(ctx, request, identity)
	finished.Store(true)
	_ = input.Close()
	<-joined
	if json.NewEncoder(output).Encode(result) != nil {
		return 2
	}
	return 0
}
