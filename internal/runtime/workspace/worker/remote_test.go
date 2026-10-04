package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/toolgateway"
)

func TestRemoteWorkerDisconnectCancelsAndJoinsRequest(t *testing.T) {
	entered, retired := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
		close(retired)
	}))
	defer server.Close()
	identity, err := ExecutableIdentity()
	if err != nil {
		t.Fatal(err)
	}
	const invocation = "0123456789abcdef0123456789abcdef"
	argument, err := InvocationArgument(invocation)
	if err != nil {
		t.Fatal(err)
	}
	input, parent := io.Pipe()
	child, output := io.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	completed := make(chan int, 1)
	go func() {
		code, _ := RunArgument(ctx, argument, input, output)
		_ = output.Close()
		completed <- code
	}()
	defer parent.Close()
	defer child.Close()
	request := Request{Mode: "gateway", Expected: identity, Gateway: toolgateway.HTTPObservation{URL: server.URL, Headers: map[string]string{"Authorization": "Bearer offline-disconnect"}}}
	if err := json.NewEncoder(parent).Encode(request); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(child)
	frame, err := ReadFrame(reader)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := DecodeReady(frame)
	if err != nil || ready.Invocation != invocation || ready.Identity != identity {
		t.Fatalf("ready: %+v %v", ready, err)
	}
	select {
	case <-entered:
		t.Fatal("worker performed HTTP before launch acknowledgement")
	default:
	}
	if err := json.NewEncoder(parent).Encode(Acknowledgement{Invocation: invocation, Execute: true}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("HTTP checkpoint was not reached")
	}
	_ = parent.Close()
	frame, err = ReadFrame(reader)
	if err != nil {
		t.Fatal(err)
	}
	result, err := DecodeResult(frame)
	if err != nil || result.Err() == nil || result.ModelStarted {
		t.Fatalf("disconnect fabricated success: %+v %v", result, err)
	}
	if code := <-completed; code != 0 {
		t.Fatalf("worker exit: %d", code)
	}
	select {
	case <-retired:
	case <-ctx.Done():
		t.Fatal("worker failed to cancel its actual HTTP request")
	}
}

func TestRemoteWorkerRequiresExactLaunchAcknowledgement(t *testing.T) {
	identity, err := ExecutableIdentity()
	if err != nil {
		t.Fatal(err)
	}
	const invocation = "0123456789abcdef0123456789abcdef"
	argument, _ := InvocationArgument(invocation)
	for _, acknowledgement := range []string{
		"",
		`{"invocation":"ffffffffffffffffffffffffffffffff","execute":true}` + "\n",
		`{"invocation":"0123456789abcdef0123456789abcdef","execute":false}` + "\n",
		`{"invocation":"0123456789abcdef0123456789abcdef","execute":true,"extra":1}` + "\n",
	} {
		t.Run(acknowledgement, func(t *testing.T) {
			request, err := json.Marshal(Request{Mode: "identity", Expected: identity})
			if err != nil {
				t.Fatal(err)
			}
			var output strings.Builder
			code, handled := RunArgument(context.Background(), argument, io.NopCloser(strings.NewReader(string(request)+"\n"+acknowledgement)), &output)
			if !handled || code != 2 || strings.Count(output.String(), "\n") != 1 {
				t.Fatalf("invalid launch was executed: %d %t %q", code, handled, output.String())
			}
		})
	}
}

func TestRemoteWorkerFrameAndArgumentAdmission(t *testing.T) {
	for _, invocation := range []string{"", "short", strings.Repeat("a", 31), strings.Repeat("a", 34), strings.Repeat("A", 32), strings.Repeat("g", 32)} {
		if _, err := InvocationArgument(invocation); err == nil {
			t.Fatalf("accepted malformed coordinate %q", invocation)
		}
	}
	for _, input := range []string{`{}`, strings.Repeat(" ", MaxBytes+1) + "\n"} {
		if _, err := ReadFrame(bufio.NewReader(strings.NewReader(input))); err == nil {
			t.Fatal("accepted incomplete or over-budget frame")
		}
	}
	if _, err := ReadFrame(bufio.NewReader(strings.NewReader(strings.Repeat(" ", MaxBytes) + "\n"))); err != nil {
		t.Fatalf("exact payload budget was reduced by framing: %v", err)
	}
}

func TestWorkerCancellationResultPreservesExactContextOutcome(t *testing.T) {
	for _, test := range []struct {
		value string
		want  error
	}{
		{"canceled", context.Canceled},
		{"deadline_exceeded", context.DeadlineExceeded},
	} {
		raw, err := json.Marshal(Result{Identity: Identity{ABI: ABI}, Cancellation: test.value})
		if err != nil {
			t.Fatal(err)
		}
		result, err := DecodeResult(raw)
		if err != nil || result.Err() != test.want {
			t.Fatalf("cancellation causality: %+v %v", result, err)
		}
	}
	for _, raw := range []string{
		`{"identity":{"abi":"` + ABI + `"},"cancellation":"invented"}`,
		`{"identity":{"abi":"` + ABI + `"},"cancellation":"canceled","failure":{}}`,
	} {
		if _, err := DecodeResult([]byte(raw)); err == nil {
			t.Fatal("accepted contradictory cancellation/result envelope")
		}
	}
}
