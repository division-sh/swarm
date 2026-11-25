package cliapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/tests/support/testutil/packfixture"
	"github.com/division-sh/swarm/tests/support/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type verifyPackGenerationArtifactReader struct {
	availability runbundle.Availability
	record       sourceartifact.Persisted
	rooted       sourceartifact.RootedSource
}

type verifyRetainedReportReader struct {
	availabilities []runbundle.Availability
	records        map[string]sourceartifact.Persisted
	roots          map[string]sourceartifact.RootedSource
	afterRead      func()
	reads          int
}

func (r *verifyRetainedReportReader) ActiveNonStandingRunBundleAvailabilities(context.Context) ([]runbundle.Availability, error) {
	return append([]runbundle.Availability(nil), r.availabilities...), nil
}

func (r *verifyRetainedReportReader) GetSourceArtifact(_ context.Context, hash string) (sourceartifact.Persisted, error) {
	row, ok := r.records[hash]
	if !ok {
		return sourceartifact.Persisted{}, sourceartifact.ErrNotFound
	}
	return row, nil
}

func (r *verifyRetainedReportReader) LoadRunRootedSource(ctx context.Context, runID string) (sourceartifact.RootedSource, error) {
	r.reads++
	if r.afterRead != nil {
		r.afterRead()
	}
	for _, availability := range r.availabilities {
		if availability.RunID != runID {
			continue
		}
		row, err := r.GetSourceArtifact(ctx, availability.BundleHash)
		if err != nil {
			return sourceartifact.RootedSource{}, err
		}
		if err := row.Validate(); err != nil {
			return sourceartifact.RootedSource{}, err
		}
		return r.roots[runID], r.roots[runID].Validate()
	}
	return sourceartifact.RootedSource{}, sourceartifact.ErrNotFound
}

func (r *verifyRetainedReportReader) add(t *testing.T, status string, rooted sourceartifact.RootedSource) string {
	t.Helper()
	if r.records == nil {
		r.records = make(map[string]sourceartifact.Persisted)
		r.roots = make(map[string]sourceartifact.RootedSource)
	}
	record, err := sourceartifact.PersistedFromArtifact(rooted.Artifact(), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewString()
	r.availabilities = append(r.availabilities, runbundle.Availability{
		RunID: runID, Status: status, BundleHash: record.BundleHash, RootReference: rooted.Admission().Reference(),
		RootAdmissionPresent: true, SourceArtifactPresent: true,
	})
	r.records[record.BundleHash], r.roots[runID] = record, rooted
	return runID
}

type verifyCountedPackResolver struct {
	packartifact.PlatformPackBaseResolver
	calls int
}

func (r *verifyCountedPackResolver) CurrentPlatformPackBase() (*packartifact.PlatformPackInventory, error) {
	r.calls++
	return r.PlatformPackBaseResolver.CurrentPlatformPackBase()
}

type verifyReportPackResolver func() (*packartifact.PlatformPackInventory, error)

func (resolve verifyReportPackResolver) CurrentPlatformPackBase() (*packartifact.PlatformPackInventory, error) {
	return resolve()
}

func (r verifyPackGenerationArtifactReader) LoadRunBundleAvailability(context.Context, string) (runbundle.Availability, error) {
	return r.availability, nil
}

func (r verifyPackGenerationArtifactReader) GetSourceArtifact(context.Context, string) (sourceartifact.Persisted, error) {
	return r.record, nil
}

func (r verifyPackGenerationArtifactReader) LoadRunRootedSource(context.Context, string) (sourceartifact.RootedSource, error) {
	return r.rooted, r.rooted.Validate()
}

func (r verifyPackGenerationArtifactReader) LoadSelectedForkRootedSource(context.Context, string) (sourceartifact.RootedSource, error) {
	return sourceartifact.RootedSource{}, sourceartifact.ErrRootAdmissionMissing
}

func (r verifyPackGenerationArtifactReader) GetRootedSource(_ context.Context, ref sourceartifact.RootAdmissionReference) (sourceartifact.RootedSource, error) {
	if r.rooted.Admission().Reference() != ref {
		return sourceartifact.RootedSource{}, sourceartifact.ErrRootAdmissionMissing
	}
	return r.rooted, r.rooted.Validate()
}

func (r verifyPackGenerationArtifactReader) ResolveUnboundRootedSource(_ context.Context, hash string) (sourceartifact.RootedSource, error) {
	if r.rooted.Admission().BundleHash() != hash {
		return sourceartifact.RootedSource{}, sourceartifact.ErrRootAdmissionMissing
	}
	return r.rooted, r.rooted.Validate()
}

func TestVerifyRetainedSourceConsumerSharesFrozenConfiguredPackGeneration(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	embedded := packfixture.EmbeddedBase(t)
	entry, ok := embedded.Lookup("telegram")
	if !ok {
		t.Fatal("missing Telegram pack")
	}
	body, ok := entry.Provider().Body(packmodel.BodyTrigger)
	if !ok {
		t.Fatal("missing Telegram trigger BODY")
	}
	replacement := strings.Replace(string(body.Bytes()), "telegram update object is required", "frozen configured telegram object is required", 1)
	if replacement == string(body.Bytes()) {
		t.Fatal("configured trigger revision must change its captured BODY")
	}
	configured, dirs := packfixture.DevelopmentBase(t, map[string][]byte{
		"telegram/trigger.yaml": []byte(replacement),
	})
	root := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
	configPath := filepath.Join(t.TempDir(), "runtime.yaml")
	writeRuntimeConfigText(t, configPath, "platform:\n  packs:\n    platform_dirs: ["+strings.Join(dirs, ", ")+"]\n")
	cfg, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: configPath})
	if err != nil {
		t.Fatal(err)
	}
	base, err := LoadConfiguredPlatformPackBase(root, cfg)
	if err != nil || base.Digest() != configured.Digest() || base.Digest() == embedded.Digest() {
		t.Fatalf("configured generation was not selected: %v", err)
	}
	bases, err := packartifact.NewPlatformPackBaseGenerationOwner(base)
	if err != nil {
		t.Fatal(err)
	}
	_, bundle, err := NewSwarmWorkflowModuleWithPackBase(root, root, base)
	if err != nil {
		t.Fatal(err)
	}
	record, err := sourceartifact.PersistedFromArtifact(bundle.SourceArtifact, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(configPath); err != nil {
		t.Fatal(err)
	}
	verify, err := verifyRetainedSourceLoader(root, bases)
	if err != nil {
		t.Fatal(err)
	}
	sourceID := uuid.NewString()
	reader := verifyPackGenerationArtifactReader{
		record: record, availability: runbundle.Availability{RunID: sourceID, Status: "running", BundleHash: record.BundleHash, SourceArtifactPresent: true},
		rooted: sourceartifactfixture.CapturedRoot(t, bundle.SourceArtifact),
	}
	verify.Store = reader
	boot := runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: root, PlatformPackBases: bases, Store: reader}
	for _, request := range []runforkexecution.SelectedContractSourceLoadRequest{
		{Responsibility: runforkexecution.FreshSelectedSource(record.BundleHash), BundleHash: record.BundleHash, Selection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: record.BundleHash}},
		{Responsibility: runforkexecution.OriginalRunSource(sourceID), Selection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts}},
	} {
		for _, loader := range []runforkexecution.SourceArtifactSelectedContractSourceLoader{verify, boot} {
			loaded, err := loader.InspectRunForkSelectedContractSourceForRequest(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			actual, ok := semanticview.Bundle(loaded.Source)
			if !ok || actual.PackInventory.BaseDigest() != configured.Digest() || loaded.RuntimeProjection != nil || loaded.Cleanup != nil {
				t.Fatalf("selected/original retained consumer lost its frozen generation or prepared a runtime: %+v", loaded)
			}
		}
	}
	if _, err := verifyRetainedSourceLoader(root, nil); err == nil {
		t.Fatal("missing generation fell back to embedded inventory")
	}
	retained := &verifyRetainedReportReader{}
	retained.add(t, "paused", reader.rooted)
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	counted := &verifyCountedPackResolver{PlatformPackBaseResolver: bases}
	cache, temporary := t.TempDir(), t.TempDir()
	t.Setenv("XDG_CACHE_HOME", cache)
	t.Setenv("TMPDIR", temporary)
	assertUnmaterialized := func() {
		t.Helper()
		for _, path := range []string{filepath.Join(cache, "swarm", "embedded-assets"), filepath.Join(temporary, "swarm-embedded-assets")} {
			if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("retained inspection materialized a specification cache: %s: %v", path, err)
			}
		}
	}
	assertUnmaterialized()
	var report runtime.WorkflowContractValidationResult
	inspectVerifyRetainedSources(context.Background(), retained, RepoRoot(), "source:"+record.BundleHash, counted, &report)
	if counted.calls == 0 || len(report.BootReport.Observations) != 2 || report.BootReport.Observations[1].Status != bootverify.AdmissionPassed {
		t.Fatalf("retained report did not compile frozen source/configured generation: calls=%d report=%+v", counted.calls, report.BootReport)
	}
	verifyRetainedReportCells(t, bases)
	assertUnmaterialized()
}

func verifyRetainedReportCells(t *testing.T, bases packartifact.PlatformPackBaseResolver) {
	t.Helper()
	rooted := func(state, directory string, agents ...string) sourceartifact.RootedSource {
		t.Helper()
		members := map[string][]byte{
			"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
			"schema.yaml":   []byte("stages: []\n"),
			"types.yaml":    []byte("Profile:\n  name: text\n"),
			"state.yaml":    []byte(state),
		}
		if len(agents) != 0 {
			members["agents.yaml"] = []byte(agents[0])
		}
		artifact, err := sourceartifact.AdmitMembers(members)
		if err != nil {
			t.Fatal(err)
		}
		source, path := sourceartifactfixture.Rooted(t, directory, artifact)
		if err := os.RemoveAll(path); err != nil {
			t.Fatal(err)
		}
		return source
	}
	valid := rooted("x: text\n", "kept")
	invalid := rooted("x: \"text\"\n", "quoted")
	invalidRecord := rooted("x: 'Profile'\n", "record")
	otherRoot, _ := sourceartifactfixture.Rooted(t, "other", valid.Artifact())
	postPackRefusal := rooted("x: text\n", "missing-data", "worker:\n  role: worker\n  intent: {inline: Read static data.}\n  flow_data_access: [missing.md]\n")
	t.Run("exact_compile_inputs", func(t *testing.T) {
		verdicts := make(verifyRetainedCompileVerdicts)
		sameBase, err := bases.CurrentPlatformPackBase()
		if err != nil {
			t.Fatal(err)
		}
		anotherBase := packfixture.EmbeddedBase(t)
		resolveAnother := verifyReportPackResolver(func() (*packartifact.PlatformPackInventory, error) { return anotherBase, nil })
		for _, source := range []sourceartifact.RootedSource{valid, otherRoot, rooted("y: text\n", "kept")} {
			if err := verdicts.compile(context.Background(), RepoRoot(), source, bases); err != nil {
				t.Fatal(err)
			}
		}
		if err := verdicts.compile(context.Background(), RepoRoot(), valid, resolveAnother); err != nil {
			t.Fatal(err)
		}
		key := verifyRetainedCompileInput{valid.Admission().Reference(), string(valid.Artifact().LogicalBlob())}
		_, first := verdicts[key][sameBase]
		_, second := verdicts[key][anotherBase]
		if len(verdicts) != 3 || len(verdicts[key]) != 2 || !first || !second {
			t.Fatal("different root, bytes or immutable pack selection reused a verdict")
		}
	})
	for _, name := range []string{"mixed_active", "same_source_runs", "same_source_refusal", "early_refusal", "cancel_hit", "resolver_error", "unreadable_hit", "different_roots", "missing_source", "corrupt_source", "crossed_root", "cancel_before", "cancel_after", "missing_generation"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reader := &verifyRetainedReportReader{}
			want := map[string]bootverify.AdmissionObservationStatus{}
			resolver := bases
			unavailable := errors.New("pack generation unavailable during retained inspection")
			switch name {
			case "mixed_active":
				want[reader.add(t, "running", invalid)] = bootverify.AdmissionFailed
				want[reader.add(t, "paused", invalidRecord)] = bootverify.AdmissionFailed
				want[reader.add(t, "live", valid)] = bootverify.AdmissionPassed
			case "same_source_runs":
				want[reader.add(t, "running", valid)] = bootverify.AdmissionPassed
				want[reader.add(t, "paused", valid)] = bootverify.AdmissionPassed
			case "same_source_refusal", "cancel_hit", "resolver_error":
				want[reader.add(t, "running", postPackRefusal)] = bootverify.AdmissionFailed
				second := reader.add(t, "paused", postPackRefusal)
				want[second] = bootverify.AdmissionFailed
				if name != "same_source_refusal" {
					if name == "cancel_hit" {
						want[second] = bootverify.AdmissionUnavailable
					}
					resolver = verifyReportPackResolver(func() (*packartifact.PlatformPackInventory, error) {
						if reader.reads == 2 {
							if name == "resolver_error" {
								return nil, unavailable
							}
							cancel()
						}
						return bases.CurrentPlatformPackBase()
					})
				}
			case "early_refusal":
				want[reader.add(t, "running", invalid)] = bootverify.AdmissionFailed
				want[reader.add(t, "paused", invalid)] = bootverify.AdmissionFailed
			case "unreadable_hit":
				want[reader.add(t, "running", valid)] = bootverify.AdmissionPassed
				second := reader.add(t, "paused", valid)
				want[second] = bootverify.AdmissionNotRun
				delete(reader.roots, second)
			case "different_roots":
				first := reader.add(t, "running", valid)
				second := reader.add(t, "paused", otherRoot)
				reader.roots[first], reader.roots[second] = otherRoot, valid
				want[first], want[second] = bootverify.AdmissionNotRun, bootverify.AdmissionNotRun
			case "cancel_before":
				reader.add(t, "paused", valid)
				cancel()
			default:
				id := reader.add(t, "paused", valid)
				want[id] = bootverify.AdmissionNotRun
				switch name {
				case "missing_source":
					delete(reader.records, valid.Admission().BundleHash())
					want[reader.add(t, "running", invalidRecord)] = bootverify.AdmissionFailed
				case "corrupt_source":
					row := reader.records[valid.Admission().BundleHash()]
					row.SourceBlob = []byte("corrupt retained source")
					reader.records[row.BundleHash] = row
					want[reader.add(t, "running", invalidRecord)] = bootverify.AdmissionFailed
				case "crossed_root":
					reader.roots[id] = invalid
					want[reader.add(t, "running", invalidRecord)] = bootverify.AdmissionFailed
				case "cancel_after":
					want[reader.add(t, "live", valid)] = bootverify.AdmissionNotRun
					reader.afterRead = cancel
				case "missing_generation":
					resolver = nil
				}
			}
			var report runtime.WorkflowContractValidationResult
			report.BootReport.Purpose = bootverify.ExecutionValidation
			inspectVerifyRetainedSources(ctx, reader, RepoRoot(), "source:candidate", resolver, &report)
			if len(report.BootReport.Observations) != len(want)+1 {
				t.Fatalf("report lost population after refusal: %+v", report.BootReport)
			}
			for _, observation := range report.BootReport.Observations[1:] {
				var id string
				for runID := range want {
					if strings.HasPrefix(observation.Subject, "run:"+runID+"/source:") {
						id = runID
						break
					}
				}
				if id == "" || observation.Status != want[id] {
					t.Fatalf("wrong per-run result: %+v wanted=%+v", observation, want)
				}
				delete(want, id)
				if observation.NotRunCause != nil {
					t.Fatal("unchecked retained source acquired absent-store authority")
				}
				if observation.Status == bootverify.AdmissionNotRun && observation.FailureClass != failures.ClassDependencyUnavailable {
					t.Fatalf("unchecked source was nonblocking: %+v", observation)
				}
				if observation.Status == bootverify.AdmissionFailed && name != "same_source_refusal" && name != "cancel_hit" && name != "resolver_error" &&
					(!strings.Contains(observation.Reason, "remove the quotes") || !strings.Contains(observation.Reason, "state.yaml")) {
					t.Fatalf("compiler refusal lost original location/teaching: %+v", observation)
				}
			}
			switch name {
			case "same_source_refusal", "early_refusal", "cancel_hit", "resolver_error":
				first, second := report.BootReport.Observations[1], report.BootReport.Observations[2]
				if name != "early_refusal" && !strings.Contains(first.Reason, "missing.md") {
					t.Fatalf("expected real post-pack static-data refusal: %+v", first)
				}
				switch name {
				case "same_source_refusal":
					if first.Cause != second.Cause || first.Reason != second.Reason || reader.reads != 2 {
						t.Fatal("equal inputs did not reuse the real refusal for both observed runs")
					}
				case "early_refusal":
					if first.Cause == second.Cause {
						t.Fatal("early refusal reused without having resolved its pack input")
					}
				case "cancel_hit":
					if !errors.Is(second.Cause, context.Canceled) || errors.Is(second.Cause, first.Cause) || !strings.Contains(second.Reason, "missing.md") {
						t.Fatalf("cancellation reused a verdict or lost the original compiler refusal: %+v", second)
					}
				case "resolver_error":
					if !errors.Is(second.Cause, unavailable) || errors.Is(second.Cause, first.Cause) {
						t.Fatalf("resolver refusal reused the earlier compiler verdict: %+v", second)
					}
				}
			}
			decision := report.BootReport.AdmissionDecision(bootverify.AdmissionFindingPolicy{})
			if name != "same_source_runs" && (decision.Complete || decision.FailureClass == "") {
				t.Fatalf("incomplete/refused retained population passed: %+v", decision)
			}
			if strings.HasPrefix(name, "cancel_") && !decision.Interrupted {
				t.Fatalf("cancellation disappeared: %+v", report.BootReport)
			}
			if name == "same_source_runs" && (!decision.Complete || reader.reads != 2) {
				t.Fatalf("equal source suppressed a run check: decision=%+v reads=%d", decision, reader.reads)
			}
			if name == "missing_generation" && (reader.reads != 0 || !strings.Contains(report.BootReport.Observations[1].Reason, "generation is required")) {
				t.Fatalf("missing frozen generation reached source/compiler: %+v", report.BootReport)
			}
			if name == "cancel_before" && (reader.reads != 0 || !errors.Is(ctx.Err(), context.Canceled)) {
				t.Fatal("canceled report admitted retained reads")
			}
		})
	}
}
