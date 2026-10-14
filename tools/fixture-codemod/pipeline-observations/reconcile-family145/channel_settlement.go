package main

import (
	"encoding/json"
	"reflect"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/budgetdiagnostic"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channelsettlement"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

func composeChannelSettlementHistory(rows []recipe, changes []transition) ([]recipe, []transition) {
	pins, err := channelsettlement.ReadPins(".")
	must(err)
	must(channelsettlement.ValidateCurrent(".", pins))
	for _, input := range []struct {
		value, out any
		recipes    bool
	}{{rows, &rows, true}, {changes, &changes, false}} {
		prior, err := channelsettlement.PriorHistory(".", input.recipes)
		must(err)
		candidate, err := json.Marshal(input.value)
		must(err)
		var old, current []map[string]any
		must(json.Unmarshal(prior, &old))
		must(json.Unmarshal(candidate, &current))
		if !reflect.DeepEqual(old, current) {
			panic("channel settlement earlier reconciliation changed its approved predecessor")
		}
		raw, err := channelsettlement.ComposeHistory(prior, pins, input.recipes)
		must(err)
		must(json.Unmarshal(raw, input.out))
	}
	return rows, changes
}

func composeBudgetDiagnosticHistory(rows []recipe, changes []transition) ([]recipe, []transition) {
	pins, err := budgetdiagnostic.ReadPins(".")
	must(err)
	must(budgetdiagnostic.ValidateCurrent(".", pins))
	for _, input := range []struct {
		value, out any
		recipes    bool
	}{{rows, &rows, true}, {changes, &changes, false}} {
		prior, err := budgetdiagnostic.PriorHistory(".", input.recipes)
		must(err)
		candidate, err := json.Marshal(input.value)
		must(err)
		var old, current []map[string]any
		must(json.Unmarshal(prior, &old))
		must(json.Unmarshal(candidate, &current))
		if !reflect.DeepEqual(old, current) {
			panic("budget earlier reconciliation changed its exact signed predecessor")
		}
		raw, err := budgetdiagnostic.ComposeHistory(".", prior, input.recipes)
		must(err)
		must(json.Unmarshal(raw, input.out))
	}
	return rows, changes
}

func budgetEarlierDeclaration(path, function, current string) (string, error) {
	return budgetdiagnostic.EarlierSource(".", path, function, current)
}

func composeSelectedReadRoleHistory(rows []recipe, changes []transition) ([]recipe, []transition) {
	pins, err := selectedreadrole.ReadPins(".")
	must(err)
	must(selectedreadrole.ValidateCurrent(".", pins))
	for _, input := range []struct {
		value, out any
		recipes    bool
	}{{rows, &rows, true}, {changes, &changes, false}} {
		prior, err := selectedreadrole.PriorHistory(".", input.recipes)
		must(err)
		candidate, err := json.Marshal(input.value)
		must(err)
		var old, current []map[string]any
		must(json.Unmarshal(prior, &old))
		must(json.Unmarshal(candidate, &current))
		if !reflect.DeepEqual(old, current) {
			panic("selected read-role earlier reconciliation changed its complete signed predecessor")
		}
		raw, err := selectedreadrole.ComposeHistory(".", prior, input.recipes)
		must(err)
		must(json.Unmarshal(raw, input.out))
	}
	return rows, changes
}

func composeProcessReadHistory(rows []recipe, changes []transition) ([]recipe, []transition) {
	pins, err := selectedreadrole.ReadProcessPins(".")
	must(err)
	must(selectedreadrole.ValidateProcessCurrent(".", pins))
	for _, input := range []struct {
		value, out any
		recipes    bool
	}{{rows, &rows, true}, {changes, &changes, false}} {
		prior, err := selectedreadrole.PriorProcessHistory(".", input.recipes)
		must(err)
		candidate, err := json.Marshal(input.value)
		must(err)
		var old, current []map[string]any
		must(json.Unmarshal(prior, &old))
		must(json.Unmarshal(candidate, &current))
		if !reflect.DeepEqual(old, current) {
			panic("process-read reconciliation changed its complete signed predecessor")
		}
		raw, err := selectedreadrole.ComposeProcessHistory(".", prior, input.recipes)
		must(err)
		must(json.Unmarshal(raw, input.out))
	}
	return rows, changes
}

func composeReadbackHistory(rows []recipe, changes []transition) ([]recipe, []transition) {
	pins, err := selectedreadrole.ReadReadbackPins(".")
	must(err)
	must(selectedreadrole.ValidateReadbackCurrent(".", pins))
	for _, input := range []struct {
		value, out any
		recipes    bool
	}{{rows, &rows, true}, {changes, &changes, false}} {
		prior, err := selectedreadrole.PriorReadbackHistory(".", input.recipes)
		must(err)
		candidate, err := json.Marshal(input.value)
		must(err)
		var old, current []map[string]any
		must(json.Unmarshal(prior, &old))
		must(json.Unmarshal(candidate, &current))
		if !reflect.DeepEqual(old, current) {
			panic("readback reconciliation changed its complete signed predecessor")
		}
		raw, err := selectedreadrole.ComposeReadbackHistory(".", prior, input.recipes)
		must(err)
		must(json.Unmarshal(raw, input.out))
	}
	return rows, changes
}

func composeEntityToolHistory(rows []recipe, changes []transition) ([]recipe, []transition) {
	for _, input := range []struct {
		value, out any
		recipes    bool
	}{{rows, &rows, true}, {changes, &changes, false}} {
		candidate, err := json.Marshal(input.value)
		must(err)
		raw, err := selectedreadrole.ComposeEntityToolHistory(".", candidate, input.recipes)
		must(err)
		must(json.Unmarshal(raw, input.out))
	}
	return rows, changes
}
