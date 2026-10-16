package startupownership

import (
	"context"
	"maps"
	"reflect"
	"strings"
	"testing"
)

// Unit-only byte visibility complements the native held-cut diagnostic. The
// changed reader result models a later commit; it is not native write evidence.
func TestAgentRecoverySeedRetainsEarlierCountsWithoutFreshnessJoin(t *testing.T) {
	for _, mutation := range []string{"claim", "settlement"} {
		for _, cut := range []string{"read", "publication", "installed"} {
			t.Run(mutation+"/"+cut, func(t *testing.T) {
				p, session, read := newAgentRecoverySeedCapability(t)
				before, after := 1, 2
				if mutation == "settlement" {
					before, after = 2, 1
				}
				read.InProgressByProfile = map[string]int{"anthropic": before}
				session.read = func(ctx context.Context) (AgentRecoverySeedRead, error) {
					value := read
					value.InProgressByProfile = maps.Clone(read.InProgressByProfile)
					ObserveAgentRecoverySeedRead(ctx)
					return value, nil
				}
				if cut == "installed" {
					if err := SeedAgentRecovery(context.Background(), p); err != nil {
						t.Fatal(err)
					}
					read.InProgressByProfile = map[string]int{"anthropic": after}
				} else {
					entered, release := make(chan struct{}), make(chan struct{})
					hold := func() { close(entered); <-release }
					probe := &AgentRecoverySeedFaultProbeForTest{}
					if cut == "read" {
						probe.InsideRead = hold
					} else {
						probe.BeforePublication = hold
					}
					remove, err := InstallAgentRecoverySeedFaultProbeForTest(p, probe)
					if err != nil {
						t.Fatal(err)
					}
					defer remove()
					result := make(chan error, 1)
					go func() { result <- SeedAgentRecovery(context.Background(), p) }()
					<-entered
					read.InProgressByProfile = map[string]int{"anthropic": after}
					close(release)
					if err := <-result; err != nil {
						t.Fatal(err)
					}
				}
				seed := p.agentRecoverySeed
				if seed == nil || !reflect.DeepEqual(seed.inProgressByProfile, map[string]int{"anthropic": before}) {
					t.Fatalf("unit counterexample lost earlier private bytes: %#v", seed)
				}
				if err := SeedAgentRecovery(context.Background(), p); err == nil || !strings.Contains(err.Error(), "already installed") || p.agentRecoverySeed != seed {
					t.Fatalf("changed reader data refreshed or revoked the earlier seed: %v", err)
				}
				if err := p.ProveCurrent(context.Background()); err != nil {
					t.Fatalf("unchanged native-owner probe was not live: %v", err)
				}
				t.Logf("UNIT ONLY: %s at %s leaves private count %d, reader now %d; no capacity consumer or native-commit evidence", mutation, cut, before, after)
			})
		}
	}
}
