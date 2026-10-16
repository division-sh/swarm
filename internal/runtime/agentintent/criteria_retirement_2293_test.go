package agentintent

import (
	"strings"
	"testing"
)

func Test2293Q2IntentPromptRetirementKeepsExactIdentity(t *testing.T) {
	intent, err := Resolve(SourceInline, "inline", "agents.yaml#agents.worker.intent", "  Work on the admitted request.\n")
	if err != nil {
		t.Fatal(err)
	}
	prompt, err := IntentOnlyPrompt(intent)
	if err != nil {
		t.Fatal(err)
	}
	text, err := prompt.Text(intent)
	if err != nil || text != intent.Content {
		t.Fatalf("exact intent bytes changed: %q error=%v", text, err)
	}
	other, err := Resolve(SourceInline, "inline", "agents.yaml#agents.other.intent", intent.Content)
	if err != nil {
		t.Fatal(err)
	}
	if err := prompt.Validate(other); err == nil {
		t.Fatal("prompt accepted a different declaration with identical content")
	}
	if _, err := (DerivedPrompt{}).Text(intent); err == nil {
		t.Fatal("empty derived prompt became an admitted carrier")
	}
	changed := prompt
	changed.text += "\nUnowned suffix."
	changed.digest = derivedPromptDigest(intent.Identity, changed.text)
	if err := changed.Validate(intent); err == nil {
		t.Fatal("intent-only prompt accepted appended text after criteria retirement")
	}
}

func Test2293Q2ProviderPromptRetirementKeepsEnvironmentAuthority(t *testing.T) {
	intent, err := Resolve(SourceInline, "inline", "agents.yaml#agents.worker.intent", "Perform the admitted work.")
	if err != nil {
		t.Fatal(err)
	}
	derived, err := IntentOnlyPrompt(intent)
	if err != nil {
		t.Fatal(err)
	}
	environment := RuntimeEnvironmentContext()
	provider, err := AssembleProviderPrompt(intent, derived, environment)
	if err != nil {
		t.Fatal(err)
	}
	text, err := provider.Text()
	if err != nil || text != intent.Content+"\n\n"+environment.text || strings.Contains(text, "Contract Criteria") {
		t.Fatalf("provider rendering changed or retained retired criteria: %q error=%v", text, err)
	}
	for _, changed := range []EnvironmentContext{{}, {owner: environment.owner, text: "different environment"}, {owner: "different owner", text: environment.text}} {
		if _, err := AssembleProviderPrompt(intent, derived, changed); err == nil {
			t.Fatal("provider assembly accepted an unowned environment")
		}
	}
	changed := provider
	changed.text = intent.Content + "\n\nDifferent environment."
	changed.digest = derivedPromptDigest(intent.Identity, changed.text)
	if err := changed.Validate(intent); err == nil {
		t.Fatal("provider validation accepted a different environment with a recomputed digest")
	}
}
