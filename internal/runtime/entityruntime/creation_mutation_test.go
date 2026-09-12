package entityruntime

import (
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestEntityCreationMutationValidatesCombinedCandidate(t *testing.T) {
	contract := Contract{Entity: c.EntityContract{Fields: map[string]c.EntityFieldDecl{
		"left":  {Type: "text", Initial: "same", Refinements: c.SchemaRefinements{EqualTo: "right"}},
		"right": {Type: "text"},
	}}}
	initial, err := InitialValues(contract)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewMutationPlan(contract, initial); err == nil {
		t.Fatal("incomplete equality accepted as stored snapshot")
	}
	for _, value := range []string{"same", "different", ""} {
		t.Run(value, func(t *testing.T) {
			plan, err := NewCreationMutationPlan(contract, initial)
			if err != nil {
				t.Fatal(err)
			}
			if value != "" {
				if err := plan.Append(Mutation{Target: "entity.right", Value: value}); err != nil {
					t.Fatal(err)
				}
			}
			_, err = plan.Validate()
			if (err == nil) != (value == "same") {
				t.Fatalf("combined candidate %q: %v", value, err)
			}
		})
	}
}
