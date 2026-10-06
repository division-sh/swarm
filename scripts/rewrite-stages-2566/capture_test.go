package main

import (
	"bytes"
	"testing"
)

func TestRewrite2566CapturePreservesInsertDeleteAndFinalLine(t *testing.T) {
	for _, tc := range []struct{ before, after, diff string }{
		{"a\nb\n", "z\na\nb\n", "@@ -0,0 +1 @@\n"},
		{"a\nb\n", "a\n", "@@ -2 +1,0 @@\n"},
		{"a\nb", "a\nc", "@@ -2 +2 @@\n"},
		{"a\nb\nc\n", "a\nx\nc\ny\n", "@@ -2 +2 @@\n@@ -3,0 +4 @@\n"},
	} {
		c := change{File: "fixture", BeforeHash: digest([]byte(tc.before)), AfterHash: digest([]byte(tc.after))}
		var err error
		c.Edits, err = captureLineEdits([]byte(tc.before), []byte(tc.after), tc.diff)
		if err != nil {
			t.Fatal(err)
		}
		output, err := rewrite(c, []byte(tc.before))
		if err != nil || !bytes.Equal(output, []byte(tc.after)) {
			t.Fatalf("line capture changed bytes: %q, %v", output, err)
		}
	}
}
