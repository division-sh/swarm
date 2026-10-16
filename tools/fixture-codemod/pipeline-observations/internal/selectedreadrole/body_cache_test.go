package selectedreadrole

import (
	"strings"
	"testing"
)

func TestReadRoleBodyCacheKeepsExactSourceMultiplicityAndParseRefusals(t *testing.T) {
	const original = "package proof\nfunc target() { originalCut() }\n"
	want, err := Body([]byte(original), "target")
	if err != nil || !strings.Contains(want, "originalCut()") {
		t.Fatal("original source did not warm its exact content entry:", err)
	}
	for _, tc := range []struct {
		name, source string
		refuse       bool
	}{
		{"changed_same_size", strings.Replace(original, "originalCut", "distinctCut", 1), false},
		{"missing", "package proof\nfunc absent() {}\n", true},
		{"duplicate", original + "func target() { foreignCut() }\n", true},
		{"method_only", "package proof\ntype fixture struct{}\nfunc (fixture) target() {}\n", true},
		{"malformed", original + "func broken(", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := Body([]byte(tc.source), "target")
			if tc.refuse {
				if err == nil {
					t.Fatal("warm content cache hid missing, ambiguous or invalid source")
				}
			} else if err != nil || actual == want || !strings.Contains(actual, "distinctCut()") {
				t.Fatal("warm content cache reused a changed source:", err)
			}
			if again, err := Body([]byte(original), "target"); err != nil || again != want {
				t.Fatal("a hostile source poisoned the immutable original entry:", err)
			}
		})
	}
}
