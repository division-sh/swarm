package providertriggers

import (
	"slices"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestManifestCurrentVocabularyDiagnosticAndMergeAdmission(t *testing.T) {
	for _, body := range []string{
		"provider: github\nredact_keyz: []\n",
		"provider: github\n<<: {redact_keyz: []}\n",
		"provider: github\n<<: &options {redact_keyz: []}\n",
	} {
		_, err := parseManifestStrict([]byte(body))
		diagnostic, ok := runtimecontracts.AsLoaderDiagnostic(err)
		if !ok || diagnostic.Location.File != "trigger.yaml" || diagnostic.Location.Line < 1 || diagnostic.Location.Column < 1 || !strings.Contains(diagnostic.Location.YAMLPath, "redact_keyz") {
			t.Fatalf("manifest source coordinates lost: %+v; error=%v", diagnostic, err)
		}
		if !slices.Contains(diagnostic.ValidOptions, "redact_keys") || !strings.Contains(diagnostic.Remediation, `Did you mean "redact_keys"?`) {
			t.Fatalf("current vocabulary/suggestion missing: %+v", diagnostic)
		}
		for _, want := range []string{"redact_keyz", "Valid fields:", "trigger.yaml", "line ", "column "} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("human diagnostic omits %q: %v", want, err)
			}
		}
		if strings.Contains(err.Error(), "RETIRED") || strings.Contains(err.Error(), "migration") {
			t.Fatalf("migration layer reintroduced: %v", err)
		}
	}

	for _, value := range []string{"null", "''", "{}", "[]", "false", "7"} {
		_, err := parseManifestStrict([]byte("provider: github\n<<: {author_summary_field: " + value + "}\n"))
		if err == nil || !strings.Contains(err.Error(), `field "author_summary_field" is not supported`) {
			t.Fatalf("unsupported merged manifest field admitted: %v", err)
		}
	}

	manifest, err := parseManifestStrict([]byte("provider: github\nevent_name: {literal: inbound.github}\n<<: &options {redact_keys: [secret], metadata: {author_summary_field: user_agent}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(manifest.value.definition.RedactKeys, []string{"secret"}) || manifest.Metadata()["author_summary_field"] != "user_agent" {
		t.Fatalf("supported aliases/open business metadata changed: %+v, %v", manifest, err)
	}
}
