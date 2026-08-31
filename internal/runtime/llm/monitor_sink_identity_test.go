package llm

import (
	"context"
	"os"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
)

func TestFileMonitorSinkIsolatesConcreteSameSlugSiblings(t *testing.T) {
	root := t.TempDir()
	sink := NewFileMonitorSink(root)
	identityA := testMemoryIdentity("worker", "review/inst-a")
	identityB := testMemoryIdentity("worker", "review/inst-b")

	pathA, err := MonitorLogPath(root, identityA)
	if err != nil {
		t.Fatal(err)
	}
	replayPathA, err := MonitorLogPath(root, identityA)
	if err != nil {
		t.Fatal(err)
	}
	pathB, err := MonitorLogPath(root, identityB)
	if err != nil {
		t.Fatal(err)
	}
	if pathA != replayPathA {
		t.Fatalf("stable identity paths differ: %q != %q", pathA, replayPathA)
	}
	if pathA == pathB {
		t.Fatalf("same-slug sibling paths collapsed to %q", pathA)
	}

	for _, identity := range []agentmemory.Identity{identityA, identityB} {
		writer, err := sink.OpenTurn(context.Background(), MonitorTurnMeta{
			AgentID:        "worker",
			MemoryIdentity: identity,
		})
		if err != nil {
			t.Fatalf("OpenTurn(%s): %v", identity.FlowInstance(), err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("Close(%s): %v", identity.FlowInstance(), err)
		}
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("monitor files = %d, want two concrete owners", len(entries))
	}
}

func TestFileMonitorSinkRejectsMissingConcreteIdentity(t *testing.T) {
	sink := NewFileMonitorSink(t.TempDir())
	if _, err := sink.OpenTurn(context.Background(), MonitorTurnMeta{AgentID: "worker"}); err == nil {
		t.Fatal("slug-only monitor ownership was accepted")
	}
}
