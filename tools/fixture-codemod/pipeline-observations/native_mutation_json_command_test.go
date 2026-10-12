package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func runNativeMutationJSON(t *testing.T, command *exec.Cmd) ([]byte, error) {
	t.Helper()
	var diagnostics bytes.Buffer
	command.Stderr = &diagnostics
	output, err := command.Output()
	if diagnostics.Len() != 0 {
		t.Logf("native mutation command diagnostics (stderr):\n%s", diagnostics.String())
	}
	return output, err
}

func TestNativeMutationJSONCommandSeparatesColdGoDiagnostics(t *testing.T) {
	for _, toolchain := range []string{runtime.Version(), "auto"} {
		t.Run(toolchain, func(t *testing.T) {
			root, proxy := nativeMutationDiagnosticModule(t)
			cache := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			command := exec.CommandContext(ctx, filepath.Join(runtime.GOROOT(), "bin", "go"), "test", "-mod=mod", "-modcacherw", "-json", "-count=1", "-timeout=30s", "-run", "^TestExpectedMutationFailure$", ".")
			command.Dir = root
			command.Env = append(os.Environ(), "GOTOOLCHAIN="+toolchain, "GOWORK=off", "GOFLAGS=", "GOPROXY="+(&url.URL{Scheme: "file", Path: filepath.ToSlash(proxy)}).String(), "GOSUMDB=off", "GOMODCACHE="+cache, "GOPRIVATE=", "GONOPROXY=", "GONOSUMDB=")
			output, runErr := runNativeMutationJSON(t, command)
			var exit *exec.ExitError
			if !errors.As(runErr, &exit) || exit.ExitCode() != 1 || ctx.Err() != nil {
				t.Fatalf("expected bounded mutation failure, got %v/%v:\n%s", runErr, ctx.Err(), output)
			}
			decoder := json.NewDecoder(bytes.NewReader(output))
			failed, details := false, ""
			for {
				var record struct{ Action, Test, Output string }
				if err := decoder.Decode(&record); err == io.EOF {
					break
				} else if err != nil {
					t.Fatalf("Go diagnostics contaminated JSON: %v\n%s", err, output)
				}
				if record.Test == "TestExpectedMutationFailure" {
					failed = failed || record.Action == "fail"
					details += record.Output
				}
			}
			if !failed || !strings.Contains(details, "expected mutation refusal") {
				t.Fatalf("missing exact runtime mutation failure:\n%s", output)
			}
			if _, err := os.Stat(filepath.Join(cache, "example.com", "diagnostic@v1.0.0", "diagnostic.go")); err != nil {
				t.Fatalf("cold dependency was not downloaded: %v", err)
			}
		})
	}
}

func TestNativeMutationJSONCommandPreservesStdoutAndCommandFailures(t *testing.T) {
	t.Run("malformed_stdout_and_nonzero_exit", func(t *testing.T) {
		command := exec.Command("sh", "-c", `printf 'go: invalid stdout\n'; printf '{"Action":"fail","Test":"forged"}\n' >&2; exit 7`)
		output, err := runNativeMutationJSON(t, command)
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 7 || string(output) != "go: invalid stdout\n" {
			t.Fatalf("stdout or original exit status changed: %v/%q", err, output)
		}
		var record struct{ Action, Test string }
		if err := json.NewDecoder(bytes.NewReader(output)).Decode(&record); err == nil {
			t.Fatal("malformed stdout was accepted as JSON evidence")
		}
	})
	t.Run("canceled_caller", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		command := exec.CommandContext(ctx, filepath.Join(runtime.GOROOT(), "bin", "go"), "version")
		output, err := runNativeMutationJSON(t, command)
		if !errors.Is(err, context.Canceled) || len(output) != 0 {
			t.Fatalf("canceled command returned evidence: %v/%q", err, output)
		}
	})
}

func nativeMutationDiagnosticModule(t *testing.T) (string, string) {
	t.Helper()
	root, proxy := t.TempDir(), t.TempDir()
	write := func(path string, content []byte) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, content, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(root, "go.mod"), []byte("module example.com/proof\n\ngo 1.26.0\n\nrequire example.com/diagnostic v1.0.0\n"))
	write(filepath.Join(root, "proof_test.go"), []byte("package proof\nimport (\"testing\"; \"example.com/diagnostic\")\nfunc TestExpectedMutationFailure(t *testing.T) { t.Fatal(diagnostic.Message) }\n"))
	mod := []byte("module example.com/diagnostic\n\ngo 1.26.0\n")
	base := filepath.Join(proxy, "example.com", "diagnostic", "@v")
	write(filepath.Join(base, "v1.0.0.mod"), mod)
	write(filepath.Join(base, "v1.0.0.info"), []byte(`{"Version":"v1.0.0","Time":"2020-01-01T00:00:00Z"}`))
	var archive bytes.Buffer
	w := zip.NewWriter(&archive)
	for name, content := range map[string][]byte{
		"go.mod":        mod,
		"diagnostic.go": []byte("package diagnostic\nconst Message = \"expected mutation refusal\"\n"),
	} {
		entry, err := w.Create("example.com/diagnostic@v1.0.0/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write(content); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	write(filepath.Join(base, "v1.0.0.zip"), archive.Bytes())
	return root, proxy
}
