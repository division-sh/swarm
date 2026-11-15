package cliapp

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/store"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
)

func inspectServeSourceAdmission(ctx context.Context, in ServeCheckStageRequest, out *ServeCheckStageResult, preference WorkspaceBackendSelection) (WorkspaceBackendSelection, workspace.Resolver, bool) {
	if out.Source != nil {
		return inspectVerifySourceAdmission(ctx, in.Config, out.Source, out.Options, &out.Validation, preference)
	}
	// No source is materialized by a clear-to-empty reset, so no workspace or
	// provider target is selected. Host/config/listener/store checks still run.
	out.Validation.BootReport.Observations = append(out.Validation.BootReport.Observations, bootverify.AdmissionObservation{
		CheckID: "workspace_source_admission", Owner: "internal/runtime/destructivereset.Operation.RecoverySourceSet", Subject: "reset:" + out.reset.Request.OperationID,
		Class: bootverify.AdmissionDeploymentObservation, Status: bootverify.AdmissionNotApplicable, Reason: "the admitted reset has no source requiring a workspace",
	})
	return WorkspaceBackendSelection{Backend: WorkspaceBackendNone}, nil, true
}

func serveCheckPlatformSchema() (store.SchemaBootstrapRequest, error) {
	spec, err := contracts.ParsePlatformSpecDocument(platform.PlatformSpecYAML(), platform.PlatformSpecDisplayPath)
	if err != nil {
		return store.SchemaBootstrapRequest{}, err
	}
	plans, err := store.GeneratePlatformTableDDLs(spec)
	if err != nil {
		return store.SchemaBootstrapRequest{}, err
	}
	return SchemaBootstrapRequest(spec, plans)
}

// Pending reset evidence, not parse failure, determines the compilation source.
// This snapshot closes before provider checks; it confers no recovery authority.
func inspectServeCheckSource(ctx context.Context, in ServeCheckStageRequest, root string) (bundle *contracts.WorkflowContractBundle, reset *destructivereset.Operation, retErr error) {
	if !(in.Options.Dev && !in.Options.LocalRun) {
		request, err := selectedAuthorityRequest(ctx, in.Selection, in.Config.Config)
		if err != nil {
			return nil, nil, err
		}
		schema, err := serveCheckPlatformSchema()
		if err != nil {
			return nil, nil, err
		}
		bounded, cancel := context.WithTimeout(ctx, verifyRemoteDeadline)
		defer cancel()
		reader, err := storeselected.OpenAdmissionInspection(bounded, request)
		var absent *storeselected.AbsentSQLiteStore
		if err != nil && !errors.As(err, &absent) {
			return nil, nil, err
		}
		if reader != nil {
			defer func() { retErr = errors.Join(retErr, reader.Close()) }()
			_, err = reader.Inspect(bounded, schema, func(snapshot *storeselected.AdmissionSnapshot) error {
				var err error
				bundle, reset, err = inspectServeResetSource(bounded, in, snapshot)
				return err
			})
			if err != nil || reset != nil {
				return bundle, reset, err
			}
		}
	}
	base, err := in.PackBases.CurrentPlatformPackBase()
	if err != nil {
		return nil, nil, err
	}
	_, bundle, err = NewSwarmWorkflowModuleWithPackBase(in.Repo, root, base)
	return bundle, nil, err
}

func inspectServeResetSource(ctx context.Context, in ServeCheckStageRequest, snapshot *storeselected.AdmissionSnapshot) (*contracts.WorkflowContractBundle, *destructivereset.Operation, error) {
	fresh, err := snapshot.RequiresSchemaPreparation()
	if err != nil || fresh {
		return nil, nil, err
	}
	pending, err := destructivereset.InspectPendingOperations(ctx, snapshot)
	if err != nil || len(pending) == 0 {
		return nil, nil, err
	}
	reset := &pending[0]
	plan, err := reset.RecoverySourceSet()
	if err != nil || len(plan.Sources) == 0 {
		return nil, reset, err
	}
	if len(plan.Sources) != 1 {
		return nil, reset, fmt.Errorf("reset recovery requires one captured loaded bundle")
	}
	reference := plan.Sources[0].RootReference()
	rooted, err := snapshot.GetRootedSource(ctx, reference)
	if err != nil {
		return nil, reset, err
	}
	if rooted.Admission().Reference() != reference {
		return nil, reset, errors.New("reset recovery artifact disagrees with admitted root")
	}
	bundle, err := contracts.LoadWorkflowContractBundleFromRootedSource(in.Repo, rooted, platform.PlatformSpecDisplayPath, contracts.WorkflowContractLoadOptions{
		PlatformPackBases: in.PackBases, AdmitPackInventory: packadmission.AdmitInventory,
	})
	return bundle, reset, err
}

func matchServeCheckReset(ctx context.Context, reader destructivereset.PendingOperationReader, expected *destructivereset.Operation) error {
	pending, err := destructivereset.InspectPendingOperations(ctx, reader)
	if err != nil {
		return err
	}
	var current *destructivereset.Operation
	if len(pending) != 0 {
		current = &pending[0]
	}
	if !reflect.DeepEqual(current, expected) {
		return destructivereset.ErrOperationChanged
	}
	return nil
}
