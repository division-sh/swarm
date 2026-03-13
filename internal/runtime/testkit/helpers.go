package testkit

import (
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
)

func TempScript(t testing.TB, name, body string) string {
	t.Helper()
	path := t.TempDir() + "/" + name
	if err := osWriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write script %s: %v", name, err)
	}
	return path
}

func WaitForEventTypes(t testing.TB, ch <-chan events.Event, expected []string, timeout time.Duration) map[string]events.Event {
	t.Helper()
	if timeout <= 0 {
		timeout = 1500 * time.Millisecond
	}
	need := make(map[string]struct{}, len(expected))
	for _, typ := range expected {
		need[strings.TrimSpace(typ)] = struct{}{}
	}
	got := make(map[string]events.Event, len(expected))
	deadline := time.After(timeout)
	for len(got) < len(expected) {
		select {
		case evt := <-ch:
			typ := strings.TrimSpace(string(evt.Type))
			if _, ok := need[typ]; ok {
				if _, seen := got[typ]; !seen {
					got[typ] = evt
				}
			}
		case <-deadline:
			t.Fatalf("timed out waiting for events; got=%v expected=%v", KeysFromEventMap(got), expected)
		}
	}
	return got
}

func KeysFromEventMap(m map[string]events.Event) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func PtrTime(v time.Time) *time.Time { return &v }

func HTTPResponse(code int, body string) *http.Response {
	return &http.Response{
		StatusCode: code,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

// Wrapped to keep helper package small and avoid os import churn in test files.
var osWriteFile = func(name string, data []byte, perm uint32) error {
	return os.WriteFile(name, data, os.FileMode(perm))
}
