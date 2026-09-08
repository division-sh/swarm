package selection

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
)

func TestCommandOwnedAgentSelection(t *testing.T) {
	for _, backend := range []string{BackendClaudeCLI, BackendAnthropic, BackendOpenAICompatible, BackendOpenAIResponses} {
		profile, err := ResolveLiveBackend(backend)
		if err != nil {
			t.Fatal(err)
		}
		for _, posture := range []executionposture.Posture{executionposture.Live, executionposture.MockOnly} {
			for _, pin := range []string{"", backend} {
				for _, double := range []bool{false, true} {
					input := AgentExecutionSelectionInput{Posture: posture, ConfiguredDefault: profile, AuthoredBackend: pin, MockConfigured: double}
					got, err := ResolveAgentExecutionSelection(input)
					if posture == executionposture.MockOnly && !double {
						if err == nil || !strings.Contains(err.Error(), "requires an exact mock performance") {
							t.Fatalf("%+v: %v", input, err)
						}
						continue
					}
					if err != nil {
						t.Fatalf("%+v: %v", input, err)
					}
					if got.ModelProfile.ID != backend {
						t.Fatalf("lost live model metadata: %+v", got)
					}
					if posture == executionposture.Live {
						if got.Mode != executionmode.Live || got.Profile.ID != backend || got.ArtifactRequirement != ArtifactForbidden {
							t.Fatalf("live selection = %+v", got)
						}
					} else if got.Mode != executionmode.Mock || got.Profile.ID != BackendMock || got.ArtifactRequirement != ArtifactRequired {
						t.Fatalf("test selection = %+v", got)
					}
				}
			}
		}
	}
}

func TestCommandOwnedSelectionRefusesMissingPurposeAndRetiredSelectors(t *testing.T) {
	live, _ := ResolveLiveBackend(BackendClaudeCLI)
	mock, _ := ResolveActiveBackend(BackendMock)
	for _, input := range []AgentExecutionSelectionInput{
		{ConfiguredDefault: live, MockConfigured: true},
		{Posture: "invalid", ConfiguredDefault: live, MockConfigured: true},
		{Posture: executionposture.Live, ConfiguredDefault: mock, MockConfigured: true},
		{Posture: executionposture.MockOnly, ConfiguredDefault: mock, MockConfigured: true},
		{Posture: executionposture.Live, ConfiguredDefault: live, AuthoredBackend: BackendMock, MockConfigured: true},
		{Posture: executionposture.MockOnly, ConfiguredDefault: live, AuthoredBackend: BackendMock, MockConfigured: true},
		{Posture: executionposture.Live, ConfiguredDefault: live, AuthoredBackend: BackendAnthropic},
	} {
		if _, err := ResolveAgentExecutionSelection(input); err == nil {
			t.Fatalf("accepted %+v", input)
		}
	}
}
