package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

func TestSignedSourcesRefusesMissingTreeParseErrorsAndNewHelperReferences(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "internal/serveapp")
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	write := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(path, name), []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}
	git := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatal(err, string(out))
		}
	}
	git("init", "-q")
	write("signed.go", "package serveapp\nfunc original(){}\n")
	git("add", ".")
	git("-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid", "-c", "commit.gpgsign=false", "commit", "-qm", "signed source")
	if _, err := collectSignedSources(root, "HEAD"); err != nil {
		t.Fatal("signed source inventory refused:", err)
	}
	if _, err := collectSignedSources(root, strings.Repeat("a", 40)); err == nil {
		t.Fatal("missing signed tree was swallowed as new source")
	}
	for _, source := range []string{
		"package serveapp\nfunc bridge(\n",
		"//go:build foreign_fixture_platform\n\npackage serveapp\nvar escape=func(){servedEventPublishNodeDeliveryCount(t,db,backend,run,event,node)}\n",
		"package serveapp\nvar saved=servedEventPublishNodeDeliveryCount\n",
	} {
		write("new.go", source)
		if _, err := collectSignedSources(root, "HEAD"); err == nil {
			t.Fatal("new/excluded malformed or unrecorded helper source was omitted:", source)
		}
	}
	write("new.go", "package serveapp\nfunc nativeBridge(){}\n")
	if _, err := collectSignedSources(root, "HEAD"); err != nil {
		t.Fatal("unrelated native bridge should not become a source donor:", err)
	}
	if err := os.Remove(filepath.Join(path, "signed.go")); err != nil {
		t.Fatal(err)
	}
	if _, err := collectSignedSources(root, "HEAD"); err == nil {
		t.Fatal("incomplete owned signed source enumeration was accepted")
	}
}

func TestDestinationCandidateKeepsUnrelatedSourceAndRefusesMissingDuplicateOrChangedBody(t *testing.T) {
	path := filepath.Join(t.TempDir(), "current.go")
	pin := runphysical.Pin{File: path, Function: "selected", Before: "func selected() {\n\told()\n}", After: "func selected() {\n\tnative()\n}"}
	unrelated := "// retained current-only source\nfunc unrelated(){ currentOnly() }\n"
	for _, body := range []string{pin.Before, pin.After} {
		if err := os.WriteFile(path, []byte("package proof\n"+body+"\n"+unrelated), 0644); err != nil {
			t.Fatal(err)
		}
		out, err := destinationCandidate(path, []runphysical.Pin{pin})
		if err != nil || !strings.Contains(string(out), pin.After) || !strings.Contains(string(out), unrelated) {
			t.Fatal("finite candidate changed unrelated source or lost the exact endpoint:", err)
		}
	}
	for _, body := range []string{"", pin.After + "\n" + pin.After, "func selected(){ foreign() }"} {
		if err := os.WriteFile(path, []byte("package proof\n"+body), 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := destinationCandidate(path, []runphysical.Pin{pin}); err == nil {
			t.Fatal("finite destination accepted missing, duplicate or changed declaration:", body)
		}
	}
}
