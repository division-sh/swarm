package attemptgeneration

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestGenerationKeySuffixStrict(t *testing.T) {
	g := Generation{FlowID: "", LoopID: "loop", ActivationID: "activation", RevisionField: "revision", RevisionID: "revision-1", Attempt: 1}
	key := g.KeySuffix()
	parsed, ok := ParseKeySuffix(key)
	if !ok || parsed.RevisionField != "" || parsed.Valid() {
		t.Fatal("partial key became a full generation")
	}
	parsed.RevisionField = g.RevisionField
	if parsed != g {
		t.Fatal("lost key coordinates")
	}
	invalid := []string{" " + key, key + " ", key + ".extra", strings.Replace(key, ".", "=.", 1)}
	for _, text := range []string{"1junk", "+1", "01", "1 ", "0", "-1", "9999999999999999999999999"} {
		parts := strings.Split(key, ".")
		parts[4] = base64.RawURLEncoding.EncodeToString([]byte(text))
		invalid = append(invalid, strings.Join(parts, "."))
	}
	for _, raw := range invalid {
		t.Run(raw, func(t *testing.T) {
			if _, ok := ParseKeySuffix(raw); ok {
				t.Fatalf("accepted malformed suffix %q", raw)
			}
		})
	}
}
