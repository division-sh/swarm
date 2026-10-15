package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
)

func precompileTestArgs(ctx context.Context, args []string, diagnostics io.Writer, runOwned func(context.Context, []string, io.Writer, io.Writer) (preflightProcessTiming, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if preflightDescendantJoinMode != "native" || os.Getenv("GOFLAGS") != "" || !precompileArgsSupported(args) {
		return nil
	}
	started := time.Now()
	var stdout, stderr bytes.Buffer
	metadata, err := runOwned(ctx, []string{"go", "env", "-json", "GOFLAGS", "GOOS", "GOARCH", "CGO_ENABLED", "GOWORK", "GOVERSION"}, &stdout, &stderr)
	if err != nil {
		return precompileFailure(ctx, "read effective build environment", err, diagnostics, &stdout, &stderr)
	}
	var env struct {
		GOFLAGS string
		testplanning.BuildContext
	}
	if err := json.Unmarshal(stdout.Bytes(), &env); err != nil {
		return precompileFailure(ctx, "decode effective build environment", err, diagnostics, &stdout, &stderr)
	}
	if err := env.BuildContext.Validate(); err != nil {
		return precompileFailure(ctx, "validate effective build environment", err, diagnostics, &stdout, &stderr)
	}
	if env.GOFLAGS != "" || env.GOOS != runtime.GOOS || env.GOARCH != runtime.GOARCH {
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	stdout.Reset()
	stderr.Reset()
	command := append([]string{"go", "test", "-exec=/bin/true"}, args...)
	build, err := runOwned(ctx, command, &stdout, &stderr)
	fmt.Fprintf(diagnostics, "Precompile outside test slot: environment=%.3fs build=%.3fs cleanup=%.3fs total=%.3fs\n",
		metadata.ExecutionSeconds, build.ExecutionSeconds, metadata.CleanupSeconds+build.CleanupSeconds, time.Since(started).Seconds())
	if err != nil {
		return precompileFailure(ctx, "compile tests", err, diagnostics, &stdout, &stderr)
	}
	return ctx.Err()
}

func precompileFailure(ctx context.Context, phase string, err error, diagnostics io.Writer, stdout, stderr *bytes.Buffer) error {
	// Even a failed multi-package warm-up may contain successful package records.
	// They are diagnostics only, never part of the admitted JSON proof stream.
	fmt.Fprintf(diagnostics, "Precompile %s diagnostics (not test evidence):\n%s%s", phase, stdout.Bytes(), stderr.Bytes())
	return fmt.Errorf("%s: %w", phase, errors.Join(err, ctx.Err()))
}

func precompileArgsSupported(args []string) bool {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "" {
			return false
		}
		if !strings.HasPrefix(arg, "-") {
			continue
		}
		name, value, assigned := strings.Cut(strings.TrimPrefix(strings.TrimPrefix(arg, "-"), "-"), "=")
		takesValue, supported := precompileFlag(name)
		if !supported {
			return false
		}
		if !takesValue {
			if assigned {
				if _, err := strconv.ParseBool(value); err != nil {
					return false
				}
			} else if i+1 < len(args) {
				if _, err := strconv.ParseBool(args[i+1]); err == nil {
					return false
				}
			}
			continue
		}
		if !assigned {
			i++
			if i == len(args) {
				return false
			}
			value = args[i]
		}
		if value == "" || strings.HasPrefix(value, "-") {
			return false
		}
	}
	return true
}

func precompileFlag(name string) (takesValue, supported bool) {
	if !strings.HasPrefix(name, "test.") {
		switch name {
		case "race", "cover", "json", "x", "trimpath":
			return false, true
		case "tags", "p", "vet", "covermode", "coverpkg":
			return true, true
		}
	}
	switch strings.TrimPrefix(name, "test.") {
	case "v", "short", "failfast", "fullpath", "benchmem":
		return false, true
	case "run", "skip", "count", "timeout", "parallel", "cpu", "shuffle", "bench", "benchtime", "list":
		return true, true
	default:
		return false, false
	}
}
