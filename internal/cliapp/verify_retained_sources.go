package cliapp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/startuprecovery"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

type verifyRetainedSourceReader interface {
	runbundle.ActiveAvailabilityReader
	startuprecovery.ArtifactReader
	LoadRunRootedSource(context.Context, string) (sourceartifact.RootedSource, error)
}

type verifyRetainedCompileInput struct {
	root    sourceartifact.RootAdmissionReference
	logical string
}

type verifyRetainedCompileVerdicts map[verifyRetainedCompileInput]map[*packartifact.PlatformPackInventory]error

// A candidate hit may resolve early, but the compiler still receives that exact
// result only at its original resolution seat if the candidate misses.
type verifyRetainedPackSelection struct {
	resolver packartifact.PlatformPackBaseResolver
	resolved bool
	base     *packartifact.PlatformPackInventory
	err      error
}

func (s *verifyRetainedPackSelection) CurrentPlatformPackBase() (*packartifact.PlatformPackInventory, error) {
	if !s.resolved {
		s.base, s.err = s.resolver.CurrentPlatformPackBase()
		s.resolved = true
	}
	return s.base, s.err
}

func (verdicts verifyRetainedCompileVerdicts) compile(ctx context.Context, repo string, rooted sourceartifact.RootedSource, packBases packartifact.PlatformPackBaseResolver) error {
	input := verifyRetainedCompileInput{rooted.Admission().Reference(), string(rooted.Artifact().LogicalBlob())}
	prior := verdicts[input]
	selection := &verifyRetainedPackSelection{resolver: packBases}
	if len(prior) != 0 {
		base, err := selection.CurrentPlatformPackBase()
		if verdict, ok := prior[base]; ok && err == nil && ctx.Err() == nil {
			return verdict
		}
	}
	_, err := contracts.LoadWorkflowContractBundleFromRootedSource(repo, rooted, platform.PlatformSpecDisplayPath, contracts.WorkflowContractLoadOptions{
		PlatformPackBases: selection, AdmitPackInventory: packadmission.AdmitInventory,
	})
	if selection.base != nil && selection.err == nil && ctx.Err() == nil &&
		!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		if prior == nil {
			prior = make(map[*packartifact.PlatformPackInventory]error)
			verdicts[input] = prior
		}
		prior[selection.base] = err
	}
	return err
}

func inspectVerifyRetainedSources(ctx context.Context, reader verifyRetainedSourceReader, repo, subject string, packBases packartifact.PlatformPackBaseResolver, result *runtime.WorkflowContractValidationResult) {
	const check = "retained_source_integrity"
	_, preparationErr := verifyRetainedSourceLoader(repo, packBases)
	var inventory startuprecovery.Result
	verifyDeploymentObservation(ctx, result, check, "internal/runtime/startuprecovery.Inspect", subject, failures.ClassLifecycleConflict, func(ctx context.Context) error {
		var err error
		inventory, err = startuprecovery.Inspect(ctx, startuprecovery.Request{AvailabilityReader: reader, ArtifactReader: reader})
		return errors.Join(err, preparationErr)
	})
	// The repo, embedded spec and compiler options are fixed within this check.
	// Every run still reads its own rooted source, even after an earlier refusal.
	verdicts := make(verifyRetainedCompileVerdicts)
	for _, availability := range inventory.CheckedAvailabilities {
		runSubject := "run:" + availability.RunID
		if availability.BundleHash != "" {
			runSubject += "/source:" + availability.BundleHash
		}
		started := time.Now().UTC()
		rooted, err := readVerifyRetainedSource(ctx, reader, availability, preparationErr)
		if err != nil {
			result.BootReport.Observations = append(result.BootReport.Observations, bootverify.AdmissionObservation{
				CheckID: check, Owner: "internal/store/selected.AdmissionSnapshot.LoadRunRootedSource", Subject: runSubject,
				Class: bootverify.AdmissionDeploymentObservation, Status: bootverify.AdmissionNotRun,
				Reason: FormatCLIAPIError(err), FailureClass: failures.ClassDependencyUnavailable,
				StartedAt: started, FinishedAt: time.Now().UTC(),
			})
			result.BootReport.Interrupted = result.BootReport.Interrupted || errors.Is(err, context.Canceled)
			continue
		}
		attempted := false
		verifyDeploymentObservation(ctx, result, check, "internal/runtime/contracts.LoadWorkflowContractBundleFromRootedSource", runSubject, failures.ClassSchemaInvalid, func(ctx context.Context) error {
			attempted = true
			return verdicts.compile(ctx, repo, rooted, packBases)
		})
		if !attempted {
			result.BootReport.Observations[len(result.BootReport.Observations)-1].Status = bootverify.AdmissionNotRun
		}
	}
}

func readVerifyRetainedSource(ctx context.Context, reader verifyRetainedSourceReader, availability runbundle.Availability, preparationErr error) (sourceartifact.RootedSource, error) {
	if err := errors.Join(ctx.Err(), preparationErr); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	if !availability.Available() {
		return sourceartifact.RootedSource{}, fmt.Errorf("%s", availability.DetailString())
	}
	rooted, err := reader.LoadRunRootedSource(ctx, availability.RunID)
	if err := errors.Join(err, ctx.Err()); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	if err := rooted.Validate(); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	if rooted.Admission().Reference() != availability.RootReference {
		return sourceartifact.RootedSource{}, fmt.Errorf("%s: %s", runbundle.CodeBundleDataIntegrityError, availability.DetailString())
	}
	return rooted, nil
}
