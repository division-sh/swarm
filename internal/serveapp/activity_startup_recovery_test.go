package serveapp

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestServeActivityRecoveryTargetsRequireExactRuntimeSources(t *testing.T) {
	process := worklifetime.NewProcess()
	makeContext := func(hash string) serveRuntimeBundleContext {
		fact := mustServeTestPersistedSourceArtifactFact(runtimeContextTestHash(hash))
		return serveRuntimeBundleContext{sourceArtifactFact: fact, runtime: &runtime.Runtime{
			Options:  runtime.RuntimeOptions{SourceArtifactFact: fact, RuntimeInstanceID: "recovery-instance", ProcessWorkOwner: process},
			Pipeline: &pipeline.PipelineCoordinator{},
		}}
	}
	first, sibling := makeContext("a"), makeContext("b")
	targets, err := serveActivityRecoveryTargets([]serveRuntimeBundleContext{first, sibling})
	if err != nil || len(targets) != 2 || targets[first.sourceArtifactFact.BundleHash()] != first.runtime || targets[sibling.sourceArtifactFact.BundleHash()] != sibling.runtime {
		t.Fatalf("recovery lost exact sibling source routing: %v", err)
	}
	for _, scenario := range []string{"missing_runtime", "missing_pipeline", "foreign_source", "missing_process", "foreign_process", "foreign_instance", "missing_instance", "duplicate"} {
		t.Run(scenario, func(t *testing.T) {
			changed := makeContext("b")
			switch scenario {
			case "missing_runtime":
				changed.runtime = nil
			case "missing_pipeline":
				changed.runtime.Pipeline = nil
			case "foreign_source":
				changed.runtime.Options.SourceArtifactFact = first.sourceArtifactFact
			case "missing_process":
				changed.runtime.Options.ProcessWorkOwner = nil
			case "foreign_process":
				changed.runtime.Options.ProcessWorkOwner = worklifetime.NewProcess()
			case "foreign_instance":
				changed.runtime.Options.RuntimeInstanceID = "another-process"
			case "missing_instance":
				changed.runtime.Options.RuntimeInstanceID = ""
			case "duplicate":
				changed = first
			}
			if targets, err := serveActivityRecoveryTargets([]serveRuntimeBundleContext{first, changed}); err == nil || targets != nil {
				t.Fatal("inexact or ambiguous source routing was admitted")
			}
		})
	}
}

func TestServeActivityRecoveryKeepsMissingOrCanceledProcessClosed(t *testing.T) {
	if err := recoverServeDisplacedHTTPActivities(nil, nil, nil); err == nil {
		t.Fatal("missing original process context admitted recovery")
	}
	if err := recoverServeDisplacedHTTPActivities(context.Background(), nil, nil); err == nil {
		t.Fatal("missing native recovery capability admitted startup")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := recoverServeDisplacedHTTPActivities(ctx, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled startup did not retain its original error")
	}
}
