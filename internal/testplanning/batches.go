package testplanning

import (
	"fmt"
	"math"
	"sort"
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
	for _, unit := range units {
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
		var weight float64
		deadline := 0
		for _, id := range batch.Units {
			unit, err := p.Unit(id)
			if err != nil || owners[id] || unit.BudgetClass != batch.BudgetClass || len(batch.Units) > 1 && unit.BudgetClass == "soak" {
				return fmt.Errorf("invalid/duplicate proof batch ownership: %s/%s", batch.ID, id)
			}
			owners[id] = true
			weight += unit.WeightSeconds
			deadline += singleBatch(unit).TimeoutMinutes
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
