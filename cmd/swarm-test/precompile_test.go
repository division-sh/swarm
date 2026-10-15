package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
)

func precompileFixtureGoHeader() string {
	return fmt.Sprintf("#!/bin/sh\nset -eu\nif [ \"$1\" = env ]; then exec %q \"$@\"; fi\nif [ \"${2-}\" = -exec=/bin/true ]; then exit 0; fi\n", filepath.Join(runtime.GOROOT(), "bin", "go"))
}

func TestPrecompileArgsConservativelySelectSafeForms(t *testing.T) {
	for _, args := range [][]string{
		{"./p"}, {"file.go", "file_test.go"}, {"./p/...", "./q"},
		{"-json", "-race", "-tags", "issue2413", "-count=1", "-p=1", "-timeout", "10s", "./p"},
		{"./p", "--test.run=^TestOne$", "-test.skip", "Other", "--test.count=3", "-test.v=true"},
		{"-cover", "-covermode=atomic", "-coverpkg=./p", "-vet=off", "-trimpath", "./p"},
		{"./p", "-bench=Benchmark", "-benchtime=1x", "-benchmem", "-list=Test", "-short=false", "-cpu=1,2", "-parallel=2", "-shuffle=on", "-failfast", "-fullpath"},
	} {
		if !precompileArgsSupported(args) {
			t.Errorf("ordinary form bypassed: %q", args)
		}
	}
	for _, flag := range []string{"exec", "c", "o", "args", "toolexec", "compiler", "work", "n", "outputdir", "artifacts", "coverprofile", "cpuprofile", "memprofile", "blockprofile", "mutexprofile", "trace", "fuzz", "fuzztime", "unknown", "test.exec", "test.c", "test.o", "test.outputdir", "test.artifacts", "test.coverprofile", "test.cpuprofile", "test.fuzz", "test.race", "test.json"} {
		for _, spelling := range []string{"-" + flag, "--" + flag, "-" + flag + "=value", "--" + flag + "=value"} {
			args := []string{"./p", spelling, "value"}
			if precompileArgsSupported(args) {
				t.Errorf("unsupported form warmed: %q", args)
			}
		}
	}
	for _, args := range [][]string{{"./p", "--"}, {"./p", "-run"}, {"./p", "-run="}, {"./p", "-run", "-exec=custom"}, {"./p", "-v="}, {"./p", "-v=not-bool"}, {"./p", "-v", "false"}, {"./p", "-race", "true"}, {"./p", "---run=Test"}, {""}, {"./p", "-args", "-exec=custom"}} {
		if precompileArgsSupported(args) {
			t.Errorf("ambiguous form warmed: %q", args)
		}
	}
}

func TestPrecompileUsesOnlyOwnedBuildAndDiscardsSuccess(t *testing.T) {
	if preflightDescendantJoinMode != "native" {
		t.Skip("precompile intentionally bypasses unsupported native ownership")
	}
	t.Setenv("GOFLAGS", "")
	args := []string{"./p", "-json", "-race", "-count=1", "-run=^TestOne$"}
	original := append([]string(nil), args...)
	var calls [][]string
	var diagnostics bytes.Buffer
	metadata := testPrecompileEnvironment()
	err := precompileTestArgs(context.Background(), args, &diagnostics, func(ctx context.Context, command []string, stdout, stderr io.Writer) (preflightProcessTiming, error) {
		calls = append(calls, append([]string(nil), command...))
		if command[1] == "env" {
			return preflightProcessTiming{}, json.NewEncoder(stdout).Encode(metadata)
		}
		fmt.Fprintln(stdout, `{"Action":"pass","Package":"warm-only"}`)
		fmt.Fprintln(stderr, "successful warm-up output")
		return preflightProcessTiming{}, nil
	})
	if err != nil || len(calls) != 2 || !reflect.DeepEqual(calls[1], append([]string{"go", "test", "-exec=/bin/true"}, original...)) || !reflect.DeepEqual(args, original) {
		t.Fatalf("precompile changed the admitted input: calls=%q args=%q err=%v", calls, args, err)
	}
	if strings.Contains(diagnostics.String(), "warm-only") || strings.Contains(diagnostics.String(), "successful warm-up output") || !strings.Contains(diagnostics.String(), "outside test slot") {
		t.Fatalf("successful warm-up output escaped: %s", diagnostics.String())
	}
}

func TestPrecompileEnvironmentAndFailureRefusals(t *testing.T) {
	if preflightDescendantJoinMode != "native" {
		t.Skip("precompile intentionally bypasses unsupported native ownership")
	}
	t.Setenv("GOFLAGS", "")
	for _, mode := range []string{"shell_flags", "persisted_flags", "foreign_os", "foreign_arch", "invalid_environment", "metadata_failure", "build_failure", "cancel_before_spawn", "cancel_after_metadata", "cancel_after_build", "join_failure"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("GOFLAGS", "")
			if mode == "shell_flags" {
				t.Setenv("GOFLAGS", "-exec=custom")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "cancel_before_spawn" {
				cancel()
			}
			cause, cleanup := errors.New("compiler failed"), errors.New("descendant join failed")
			calls := 0
			var diagnostics bytes.Buffer
			err := precompileTestArgs(ctx, []string{"./p", "-json"}, &diagnostics, func(_ context.Context, command []string, stdout, stderr io.Writer) (preflightProcessTiming, error) {
				calls++
				if command[1] == "env" {
					metadata := testPrecompileEnvironment()
					switch mode {
					case "persisted_flags":
						metadata.GOFLAGS = "-exec=custom"
					case "foreign_os":
						metadata.GOOS = "another-os"
					case "foreign_arch":
						metadata.GOARCH = "another-arch"
					case "invalid_environment":
						metadata.GOVERSION = ""
					case "metadata_failure":
						return preflightProcessTiming{}, cause
					case "cancel_after_metadata":
						cancel()
					}
					return preflightProcessTiming{}, json.NewEncoder(stdout).Encode(metadata)
				}
				fmt.Fprintln(stdout, `{"Action":"pass","Package":"partially-warm"}`)
				fmt.Fprintln(stderr, "compiler diagnostic")
				switch mode {
				case "build_failure":
					return preflightProcessTiming{}, cause
				case "cancel_after_build":
					cancel()
				case "join_failure":
					return preflightProcessTiming{}, errors.Join(cause, cleanup)
				}
				return preflightProcessTiming{}, nil
			})
			bypass := mode == "shell_flags" || mode == "persisted_flags" || mode == "foreign_os" || mode == "foreign_arch"
			if (err == nil) != bypass || (bypass && calls > 1) {
				t.Fatalf("environment/failed build fell through: calls=%d err=%v", calls, err)
			}
			if (mode == "shell_flags" || mode == "cancel_before_spawn") && calls != 0 {
				t.Fatalf("early refusal/bypass spawned work: calls=%d", calls)
			}
			if strings.HasPrefix(mode, "cancel_") && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation was lost: %v", err)
			}
			if mode == "join_failure" && (!errors.Is(err, cause) || !errors.Is(err, cleanup)) {
				t.Fatalf("independent process/cleanup failures lost: %v", err)
			}
			if mode == "build_failure" && (!strings.Contains(diagnostics.String(), "partially-warm") || !strings.Contains(diagnostics.String(), "not test evidence")) {
				t.Fatalf("compiler failure diagnostics lost: %s", diagnostics.String())
			}
		})
	}
}

func testPrecompileEnvironment() struct {
	GOFLAGS string
	testplanning.BuildContext
} {
	return struct {
		GOFLAGS string
		testplanning.BuildContext
	}{BuildContext: testplanning.BuildContext{GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, CGOEnabled: "1", GOVERSION: runtime.Version()}}
}
