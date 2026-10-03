package testplanning

import (
	"encoding/json"
	"testing"
)

func TestProofBatchesPreserveLogicalOwnershipAndDeadlines(t *testing.T) {
	p := testPolicy()
	p.Units["second"] = p.Units["catalog-full"]
	for _, id := range []string{"catalog-full", "second"} {
		u := p.Units[id]
		u.Packable = true
		u.Run = "^Test" + id + "$"
		p.Units[id] = u
	}
	profile := p.Profiles[ProfileFull]
	profile.Units = []string{"catalog-full", "second"}
	p.Profiles[ProfileFull] = profile
	model := WeightModel{Version: WeightModelVersion, SourceRunID: "measured", Units: map[string]map[string]UnitWeight{ProfileFull: {}}}
	unknown, err := BuildPlan(p, model, []string{"module/catalog"}, ProfileFull, "batch", "head")
	if err != nil {
		t.Fatal(err)
	}
	if len(unknown.Batches) != 2 {
		t.Fatal("unknown weights were packed")
	}
	for _, u := range unknown.Units {
		model.Units[ProfileFull][u.ID] = MeasureUnit(u, 60)
	}
	plan, err := BuildPlan(p, model, []string{"module/catalog"}, ProfileFull, "batch", "head")
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Batches) != 1 || len(plan.Batches[0].Units) != 2 || plan.Batches[0].TimeoutMinutes != 36 {
		t.Fatalf("wrong isolated batch: %+v", plan.Batches)
	}
	for _, mutation := range []string{"missing", "duplicate", "foreign", "deadline", "cost"} {
		t.Run(mutation, func(t *testing.T) {
			raw, _ := json.Marshal(plan)
			var bad RunPlan
			_ = json.Unmarshal(raw, &bad)
			switch mutation {
			case "missing":
				bad.Batches[0].Units = bad.Batches[0].Units[:1]
			case "duplicate":
				bad.Batches = append(bad.Batches, bad.Batches[0])
			case "foreign":
				bad.Batches[0].Units[0] = "other"
			case "deadline":
				bad.Batches[0].TimeoutMinutes--
			case "cost":
				bad.Batches[0].WeightSeconds++
			}
			if bad.Validate() == nil {
				t.Fatal("invalid physical map admitted")
			}
		})
	}
	u := p.Units["second"]
	u.Packable = false
	p.Units["second"] = u
	plan, err = BuildPlan(p, model, []string{"module/catalog"}, ProfileFull, "batch", "head")
	if err != nil || len(plan.Batches) != 2 {
		t.Fatalf("noneligible unit packed: %+v %v", plan.Batches, err)
	}
	u.Packable = true
	u.GoTimeout = "22m"
	p.Units["second"] = u
	if p.Validate() == nil {
		t.Fatal("long command declared packable")
	}
}
