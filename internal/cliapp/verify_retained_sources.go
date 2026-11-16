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

func inspectVerifyRetainedSources(ctx context.Context, reader verifyRetainedSourceReader, repo, subject string, packBases packartifact.PlatformPackBaseResolver, result *runtime.WorkflowContractValidationResult) {
	const check = "retained_source_integrity"
	_, preparationErr := verifyRetainedSourceLoader(repo, packBases)
	var inventory startuprecovery.Result
	verifyDeploymentObservation(ctx, result, check, "internal/runtime/startuprecovery.Inspect", subject, failures.ClassLifecycleConflict, func(ctx context.Context) error {
		var err error
		inventory, err = startuprecovery.Inspect(ctx, startuprecovery.Request{AvailabilityReader: reader, ArtifactReader: reader})
		return errors.Join(err, preparationErr)
	})
	// Inspect retains its complete population even when one artifact fails.
	// Compile each exact run independently; an earlier refusal is not a cut.
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
			_, err := contracts.LoadWorkflowContractBundleFromRootedSource(repo, rooted, platform.PlatformSpecDisplayPath, contracts.WorkflowContractLoadOptions{
				PlatformPackBases: packBases, AdmitPackInventory: packadmission.AdmitInventory,
			})
			return err
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
