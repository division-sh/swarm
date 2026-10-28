package formatproof

import (
	"bytes"

	"testing"
)

func TestFiniteFormattingRejectsNonFormattingAndProtectedComments(t *testing.T) {
	before := []byte("//go:build linux\n\n// owner: preserve\npackage fixture\n\nvar value = `a b`\n\nfunc take() int {\n\treturn 1\n}\n")
	for name, after := range map[string][]byte{
		"literal":   bytes.Replace(before, []byte("`a b`"), []byte("`a  b`"), 1),
		"comment":   bytes.Replace(before, []byte("owner: preserve"), []byte("owner:  preserve"), 1),
		"anchor":    bytes.Replace(before, []byte("// owner:"), []byte(" // owner:"), 1),
		"build":     bytes.Replace(before, []byte("build linux"), []byte("build darwin"), 1),
		"token":     bytes.Replace(before, []byte("return 1"), []byte("return 2"), 1),
		"semicolon": bytes.Replace(before, []byte("return 1"), []byte("return\n1"), 1),
		"unicode":   bytes.Replace(before, []byte("var value"), []byte("var\u00a0value"), 1),
		"newline":   append(append([]byte{}, before...), '\n'),
		"noop":      before,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := FormattingEdits(before, after); err == nil {
				t.Fatal("protected source change accepted as formatting")
			}
		})
	}
	if _, err := FormattingEdits(before, bytes.Replace(before, []byte("var value ="), []byte("var value  ="), 1)); err != nil {
		t.Fatalf("unchanged comments and syntax must permit horizontal formatting: %v", err)
	}
}
