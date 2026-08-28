package stringsutil

import (
	"reflect"
	"testing"
)

func TestFirstNonEmpty(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		want   string
	}{
		{"empty", nil, ""},
		{"all blank", []string{"", "  ", "\t\n"}, ""},
		{"first wins", []string{"  a  ", "b"}, "a"},
		{"skips blank then trims", []string{"", "  b  "}, "b"},
		{"already trimmed", []string{"x"}, "x"},
		{"trimmed returned", []string{"  x  "}, "x"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FirstNonEmpty(tt.values...); got != tt.want {
				t.Fatalf("FirstNonEmpty(%q) = %q, want %q", tt.values, got, tt.want)
			}
		})
	}
}

func TestContains(t *testing.T) {
	if !Contains([]string{"a", "b"}, "a") {
		t.Fatal("expected exact match")
	}
	if Contains([]string{"a", "b"}, "c") {
		t.Fatal("unexpected match")
	}
	if Contains([]string{" a "}, "a") {
		t.Fatal("Contains must not trim")
	}
	if Contains(nil, "a") {
		t.Fatal("nil slice must not match")
	}
}

func TestContainsTrimmed(t *testing.T) {
	if !ContainsTrimmed([]string{" a ", "b"}, "a") {
		t.Fatal("expected trimmed match")
	}
	if !ContainsTrimmed([]string{"a"}, " a ") {
		t.Fatal("expected target-side trim")
	}
	if ContainsTrimmed([]string{"a", "b"}, "c") {
		t.Fatal("unexpected match")
	}
	if ContainsTrimmed(nil, "a") {
		t.Fatal("nil slice must not match")
	}
}

func TestUnique(t *testing.T) {
	got := Unique([]string{" b ", "a", "  b", "a", "", " "})
	want := []string{"b", "a"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Unique = %q, want %q", got, want)
	}
	if got := Unique(nil); got == nil || len(got) != 0 {
		t.Fatalf("Unique(nil) = %#v, want empty non-nil slice", got)
	}
	if got := Unique([]string{}); got == nil || len(got) != 0 {
		t.Fatalf("Unique([]) = %#v, want empty non-nil slice", got)
	}
	if got := Unique([]string{"x"}); !reflect.DeepEqual(got, []string{"x"}) {
		t.Fatalf("Unique single = %q, want [x]", got)
	}
	// Order preservation.
	if got := Unique([]string{"c", "a", "b", "a"}); !reflect.DeepEqual(got, []string{"c", "a", "b"}) {
		t.Fatalf("Unique order = %q, want [c a b]", got)
	}
}
