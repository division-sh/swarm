package serveapp

import (
	"os"
	"strings"
	"testing"
)

func TestServeRestoresNativeOwnershipBeforeRetainedProofExecution(t *testing.T) {
	raw, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	markers := []string{
		"sessionBootstrap, err := newServeSessionBootstrap(",
		"operatorChannels, err := operatorchannel.NewService(",
		"operatorPrincipal, err := operatorChannels.PreparePrincipal(",
		"releaseRuntimeContexts, err = prepareServeRuntimeContexts(",
		"channelOnboarding.RestoreSessions(ctx)",
		"operatorChannels.Bootstrap(ctx, bootStartedAt)",
		"if err := channelActivationRefresher.publishChannelActivations(ctx)",
		"if err := releaseRuntimeContexts()",
		"if err := channelOnboarding.Recover(ctx)",
	}
	previous := -1
	for _, marker := range markers {
		position := strings.Index(source, marker)
		if position <= previous {
			t.Fatalf("startup marker %q at %d must follow %d", marker, position, previous)
		}
		previous = position
	}
}
