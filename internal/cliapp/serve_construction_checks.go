package cliapp

import (
	"context"
	"errors"
	"reflect"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
)

// ServeConstructionChecks owns the single fixed stage invocation. Its result
// contains observations, never the native owner's retained exclusion.
type ServeConstructionChecks struct {
	Input   ServeCheckStageRequest
	Result  ServeCheckStageResult
	Failure error
	called  bool
}

func (c *ServeConstructionChecks) InspectConstruction(ctx context.Context, target bootverify.ConstructionTarget) (report bootverify.Report, retErr error) {
	if c == nil || c.called {
		return bootverify.Report{}, errors.New("serve construction checks require one invocation")
	}
	defer func() { c.Failure = retErr }()
	c.called = true
	request, err := selectedAuthorityRequest(ctx, c.Input.Selection, c.Input.Config.Config)
	if err != nil {
		return bootverify.Report{}, err
	}
	var expected bootverify.ConstructionTarget
	if request.Selection.Backend == storebackend.BackendSQLite {
		expected, err = bootverify.SQLiteConstructionTarget(request.Selection.SQLitePath)
	} else {
		expected, err = storeselected.PostgresCheckTarget(request.PostgresDSN)
	}
	if err != nil || expected != target {
		return bootverify.Report{}, errors.Join(errors.New("serve check target differs from native construction"), err)
	}
	c.Input.construction = &target
	c.Result, err = InspectServeCheckStage(ctx, c.Input)
	err = errors.Join(err, c.Result.Failure())
	if err == nil {
		err = c.Result.Revalidate(ctx, c.Input)
	}
	return c.Result.Validation.BootReport, err
}

// StrictConsumerRefusal returns the original validation refusal only when
// native construction returned that stage failure without a cleanup sibling.
func (c *ServeConstructionChecks) StrictConsumerRefusal(ctx context.Context, failure error) error {
	if ctx.Err() != nil || !isUnaccompaniedConstructionFailure(failure, c.Failure) {
		return nil
	}
	return c.Result.strictConsumerRefusal()
}

func isUnaccompaniedConstructionFailure(failure, stage error) bool {
	if stage == nil || !reflect.TypeOf(stage).Comparable() {
		return false
	}
	if failure == stage {
		return true
	}
	// Both native constructors join the stage failure with their own cleanup.
	if joined, ok := failure.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		return len(causes) == 1 && causes[0] == stage
	}
	return false
}
