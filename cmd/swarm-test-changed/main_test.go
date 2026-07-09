package main

import (
	"reflect"
	"testing"
)

func TestParseNameStatusIncludesDeletesAndRenameSourceAndTarget(t *testing.T) {
	files := parseNameStatus("M\tinternal/a/a.go\nD\tinternal/b/b.go\nR100\told/path.go\tinternal/c/c.go\n")
	want := []string{"internal/a/a.go", "internal/b/b.go", "old/path.go", "internal/c/c.go"}
	var got []string
	for _, file := range files {
		got = append(got, file.Path)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("paths = %#v, want %#v", got, want)
	}
}

func TestParseNameStatusIncludesOnlyCopyTarget(t *testing.T) {
	files := parseNameStatus("C100\told/path.go\tinternal/c/c.go\n")
	want := []string{"internal/c/c.go"}
	var got []string
	for _, file := range files {
		got = append(got, file.Path)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("paths = %#v, want %#v", got, want)
	}
}

func TestShellCommandQuotesGoTestCommand(t *testing.T) {
	got := shellCommand([]string{"go", "test", "-run", "Test Name", "./internal/a"})
	want := "go test -run 'Test Name' ./internal/a"
	if got != want {
		t.Fatalf("shell command = %q, want %q", got, want)
	}
}
