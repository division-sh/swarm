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

func TestFamily145SelectedCapabilityEntireHelperChainKeepsAllAssertionsAndQueryScopes(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		cuts := family145SelectedCapabilityCuts[row.Function]
		if cuts == nil {
			continue
		}
		if seen[row.Function] {
			t.Fatal("duplicate capability helper mapping")
		}
		seen[row.Function] = true
		if err := family145SelectedCapabilityProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range cuts {
			if family145SelectedCapabilityProofError(row.Function, row.Before, strings.Replace(row.After, cut, "foreignCapabilityCut()", 1)) == nil {
				t.Fatalf("lost field/context/authority/identity/temporal cut passed: %s", cut)
			}
		}
		owners := family145SelectedCapabilityQueryOwners(row.Function)
		queries := family145A2ClaimLiteralQueries(t, row.Before)
		if len(queries) != len(owners) {
			t.Fatalf("original capability query inventory: %s %d/%d", row.Function, len(queries), len(owners))
		}
		for i, owner := range owners {
			source := family145ConversationForkOwnerSource(t, owner[0], owner[1])
			if got := family145A2ClaimLiteralQueries(t, source); len(got) != 1 || got[0] != queries[i] {
				t.Fatalf("capability owner changed exact SQL columns, NULL policy, scopes, joins or order: %s %v", owner[1], got)
			}
		}
	}
	if len(seen) != 3 {
		t.Fatalf("capability whole-helper inventory=%d/3", len(seen))
	}
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

func TestFamily145SelectedCapabilityCallersKeepOriginalOwnerAndErrorOnlyDiagnostics(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	callers := map[string][2]string{
		"TestSelectedContractForkProviderTurnsUseCanonicalExecutionFrames": {
			"assertSelectedForkProviderCapabilityEvidence(t, ctx, pg, result, tc.adapter, 2)",
			"assertSelectedForkProviderCapabilityEvidence(t, ctx, db, result, tc.adapter, 2)",
		},
		"proveSelectedForkClaudeOAuth": {
			"assertSelectedForkClaudeCapabilityEvidence(t, ctx, selected, result)",
			"assertSelectedForkClaudeCapabilityEvidence(t, ctx, db, result)",
		},
	}
	seen := 0
	for _, row := range rows {
		binding, ok := callers[row.Function]
		if !ok {
			continue
		}
		seen++
		if strings.Count(row.After, binding[0]) != 1 || strings.Count(row.Before, binding[1]) != 1 {
			t.Fatalf("capability caller changed owner/result/adapter/turn count: %s", row.Function)
		}
		want, err := family145DescriptorBusinessAssertions(row.Before)
		if err != nil {
			t.Fatal(err)
		}
		got, err := family145DescriptorBusinessAssertions(row.After)
		if err != nil || !reflect.DeepEqual(want, got) {
			t.Fatalf("capability caller lost original dispatch/emission/process or refusal assertions: %s %v", row.Function, err)
		}
	}
	if seen != 2 {
		t.Fatalf("capability caller inventory=%d/2", seen)
	}
	owner := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_selected_capability_storage.go", "ReadSelectedCompletionModelAliasStorageForTest")
	if strings.Count(owner, "llmselection.ModelAliasRegular") != 1 {
		t.Fatal("selected model read no longer requires the canonical regular alias")
	}
}
