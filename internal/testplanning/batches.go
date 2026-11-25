package testplanning

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"
)

// ProofBatch changes runner placement, never logical command ownership.
type ProofBatch struct {
	ID             string   `json:"id"`
	Units          []string `json:"units"`
	BudgetClass    string   `json:"budget_class"`
	WeightSeconds  float64  `json:"weight_seconds"`
	TimeoutMinutes int      `json:"timeout_minutes"`
}

func buildBatches(units []ProofUnit, policy Policy, model WeightModel, profile string) []ProofBatch {
	var batches []ProofBatch
	var short []ProofUnit
	channel := slices.IndexFunc(units, func(u ProofUnit) bool { return u.ID == channelUnit })
	journeys := slices.IndexFunc(units, func(u ProofUnit) bool { return u.ID == channelJourneys })
	if channel >= 0 && journeys >= 0 {
		batch := singleBatch(units[channel])
		// This exact serial pair keeps the pre-split runner limit and shares setup.
		batch.Units = append(batch.Units, channelJourneys)
		batch.WeightSeconds += units[journeys].WeightSeconds
		batches = append(batches, batch)
	}
	for _, unit := range units {
		if channel >= 0 && journeys >= 0 && (unit.ID == channelUnit || unit.ID == channelJourneys) {
			continue
		}
		_, measured := model.UnitSeconds(profile, unit)
		if policy.Units[unit.ID].Packable && measured && unit.BudgetClass != "soak" && unit.WeightSeconds <= 120 {
			short = append(short, unit)
		} else {
			batches = append(batches, singleBatch(unit))
		}
	}
	sort.Slice(short, func(i, j int) bool { return short[i].ID < short[j].ID })
	for len(short) > 0 {
		batch := singleBatch(short[0])
		short = short[1:]
		if len(short) > 0 && batch.BudgetClass == short[0].BudgetClass && batch.WeightSeconds+short[0].WeightSeconds <= 240 {
			second := singleBatch(short[0])
			batch.Units = append(batch.Units, second.Units...)
			batch.ID = "batch-" + batch.ID + "--" + second.ID
			batch.WeightSeconds += second.WeightSeconds
			// Preserve each command's deadline plus the original setup/upload allowance.
			batch.TimeoutMinutes += second.TimeoutMinutes
			short = short[1:]
		}
		batches = append(batches, batch)
	}
	sort.Slice(batches, func(i, j int) bool {
		if batches[i].WeightSeconds == batches[j].WeightSeconds {
			return batches[i].ID < batches[j].ID
		}
		return batches[i].WeightSeconds > batches[j].WeightSeconds
	})
	return batches
}

func singleBatch(unit ProofUnit) ProofBatch {
	minutes := 18
	if unit.GoTimeout != "" {
		deadline, _ := time.ParseDuration(unit.GoTimeout)
		minutes = max(minutes, int(math.Ceil(deadline.Minutes()))+3)
	}
	if unit.BudgetClass == "soak" {
		minutes = 30
	}
	return ProofBatch{ID: unit.ID, Units: []string{unit.ID}, BudgetClass: unit.BudgetClass,
		WeightSeconds: unit.WeightSeconds, TimeoutMinutes: minutes}
}

func (p RunPlan) validateBatches() error {
	seenIDs, owners := map[string]bool{}, map[string]bool{}
	for _, batch := range p.Batches {
		if batch.ID == "" || seenIDs[batch.ID] || len(batch.Units) == 0 || len(batch.Units) > 2 {
			return fmt.Errorf("empty/duplicate/oversized proof batch %q", batch.ID)
		}
		seenIDs[batch.ID] = true
		channel := batch.ID == channelUnit || slices.Contains(batch.Units, channelUnit) || slices.Contains(batch.Units, channelJourneys)
		if channel && (batch.ID != channelUnit || !slices.Equal(batch.Units, []string{channelUnit, channelJourneys})) {
			return fmt.Errorf("channel batch must run both complete members in order")
		}
		var weight float64
		deadline := 0
		for _, id := range batch.Units {
			unit, err := p.Unit(id)
			if err != nil || owners[id] || unit.BudgetClass != batch.BudgetClass || len(batch.Units) > 1 && unit.BudgetClass == "soak" {
				return fmt.Errorf("invalid/duplicate proof batch ownership: %s/%s", batch.ID, id)
			}
			if channel && !validChannelInvocation(unit, p.Venue) {
				return fmt.Errorf("channel batch member %s changed its complete-root execution envelope", id)
			}
			owners[id] = true
			weight += unit.WeightSeconds
			if !channel || id == channelUnit {
				deadline += singleBatch(unit).TimeoutMinutes
			}
		}
		if batch.WeightSeconds != weight || batch.TimeoutMinutes != deadline {
			return fmt.Errorf("proof batch %s has wrong weight/deadline", batch.ID)
		}
	}
	if len(owners) != len(p.Units) {
		return fmt.Errorf("proof batches own %d units, want %d", len(owners), len(p.Units))
	}
	return nil
}

func validChannelInvocation(unit ProofUnit, venue string) bool {
	wantEnv := "ci-postgres-gateway-empty-v1"
	if venue == VenueLocal {
		wantEnv = "local-postgres-gateway-empty-v1"
	}
	return slices.Equal(unit.Packages, []string{ServedReporterPackage}) &&
		unit.Run != "" && !strings.Contains(unit.Run, "/") && unit.Skip == "" &&
		unit.GoTimeout == "" && unit.CountMode == "count-1" &&
		unit.EnvironmentID == wantEnv && unit.BudgetClass == "full"
}
