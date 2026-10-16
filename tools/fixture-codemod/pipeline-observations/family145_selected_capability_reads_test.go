package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145SelectedCapabilityCuts = map[string][]string{
	"assertSelectedForkProviderCapabilityEvidence": {
		"selected any",
		"storetest.CountSelectedProviderOperationsStorage(ctx, selected, proof.RuntimeExecutionID, proof.RuntimeGeneration)",
		"storetest.ReadSelectedCompletionCapabilitiesStorage(ctx, selected, proof.RuntimeExecutionID, adapter, proof.RuntimeGeneration, proof.AuthorityExecutionOwner, result.Materialization.ForkRunID)",
		"for _, row := range rows",
		"attemptSurfaceID, turnSurfaceID, rawSurface, planFingerprint, turnID := row.AttemptSurfaceID, row.TurnSurfaceID, row.Surface, row.PlanFingerprint, row.TurnID",
		"operationFrameBytes, turnFrameBytes := row.OperationFrame, row.TurnFrame",
		"assertSelectedForkCompletionModelAlias(t, ctx, selected, proof.RuntimeExecutionID, adapter, wantTurns)",
	},
	"assertSelectedForkClaudeCapabilityEvidence": {
		"selected any",
		"storetest.ReadSelectedPreparationStorage(ctx, selected, proof.RuntimeExecutionID)",
		"preparationRaw, preparationFingerprint := stored.Binding, stored.Fingerprint",
		"storetest.ReadSelectedClaudeStartupCountsStorage(ctx, selected, preparation.PreparationID, preparation.Coordinates.ProcessAuthorityID)",
		"startupSurfaces, startupAttempts := counts.Surfaces, counts.Attempts",
		"storetest.ReadSelectedClaudeStartupSurfaceStorage(ctx, selected, preparation.PreparationID, preparation.Coordinates.ProcessAuthorityID)",
		"storetest.ReadSelectedClaudeTurnCapabilityStorage(ctx, selected, proof.RuntimeExecutionID, result.Materialization.ForkRunID)",
		"storetest.ReadSelectedClaudeAttemptDiagnosticsStorage(ctx, selected, proof.RuntimeExecutionID)",
		"attemptSurfaceID, turnSurfaceID, rawSurface := turn.AttemptSurfaceID, turn.TurnSurfaceID, turn.Surface",
		"assertSelectedForkCompletionModelAlias(t, ctx, selected, proof.RuntimeExecutionID, \"claude_cli\", 1)",
	},
	"assertSelectedForkCompletionModelAlias": {
		"selected any",
		"storetest.ReadSelectedCompletionModelAliasStorage(ctx, selected, executionID, adapter)",
		"total, canonical := counts.Total, counts.Canonical",
	},
}

func family145SelectedCapabilityProofError(function, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("capability read changed exact frames, identity, cardinality, tool names, fingerprints or model assertions: %s %v", function, err)
	}
	cursor := -1
	for _, cut := range family145SelectedCapabilityCuts[function] {
		at := strings.Index(after, cut)
		if at <= cursor || strings.Count(after, cut) != 1 {
			return fmt.Errorf("capability read changed an original independent cut, argument or destination: %s", cut)
		}
		cursor = at
	}
	if strings.Contains(after, "QueryContext") || strings.Contains(after, "QueryRowContext") || strings.Contains(after, "*sql.DB") {
		return fmt.Errorf("capability helper still transports raw query authority")
	}
	return nil
}

func family145SelectedCapabilityQueryOwners(function string) [][2]string {
	effect := "internal/backend/effectpersistence/selected_capability_observation.go"
	switch function {
	case "assertSelectedForkProviderCapabilityEvidence":
		return [][2]string{{effect, "CountSelectedProviderOperationsStorageTx"}, {effect, "ReadSelectedCompletionCapabilitiesStorageTx"}}
	case "assertSelectedForkClaudeCapabilityEvidence":
		return [][2]string{
			{"internal/backend/runforkpersistence/preparation_observation.go", "ReadSelectedPreparationStorageTx"},
			{effect, "ReadSelectedClaudeStartupCountsStorageTx"},
			{effect, "ReadSelectedClaudeStartupSurfaceStorageTx"},
			{effect, "ReadSelectedClaudeTurnCapabilityStorageTx"},
			{effect, "ReadSelectedClaudeAttemptDiagnosticsStorageTx"},
		}
	case "assertSelectedForkCompletionModelAlias":
		return [][2]string{{"internal/budgetspend/selected_model_observation.go", "ReadSelectedCompletionModelAliasStorageTx"}}
	default:
		panic("unreviewed selected capability helper")
	}
}
