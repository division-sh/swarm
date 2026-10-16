package selectedreadrole

import (
	"reflect"
	"strings"
	"testing"
)

func TestStandingIngressDeclarationCacheIsOnlyAnImmutableParse(t *testing.T) {
	const original = "package proof\nfunc target() { originalCut() }\n"
	want, err := standingIngressDeclarations([]byte(original))
	if err != nil || !strings.Contains(want["target"], "originalCut()") {
		t.Fatal("original declaration parse failed:", err)
	}
	for _, source := range []string{
		strings.Replace(original, "originalCut", "distinctCut", 1),
		"package proof\nfunc absent() {}\n",
		original + "func target() { foreignCut() }\n",
		original + "func broken(",
	} {
		got, err := standingIngressDeclarations([]byte(source))
		switch {
		case strings.Contains(source, "foreignCut"), strings.Contains(source, "broken"):
			if err == nil {
				t.Fatal("warm parse cache hid duplicate or malformed declarations")
			}
		case strings.Contains(source, "absent"):
			if err != nil || got["target"] != "" {
				t.Fatal("warm parse cache supplied a missing declaration:", err)
			}
		default:
			if err != nil || got["target"] == want["target"] {
				t.Fatal("warm parse cache reused different source bytes:", err)
			}
		}
		again, err := standingIngressDeclarations([]byte(original))
		if err != nil || !reflect.DeepEqual(again, want) {
			t.Fatal("different source poisoned the immutable original parse:", err)
		}
		again["target"] = "foreign caller mutation"
		fresh, err := standingIngressDeclarations([]byte(original))
		if err != nil || !reflect.DeepEqual(fresh, want) {
			t.Fatal("caller changed the cached declaration map:", err)
		}
	}
}

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
