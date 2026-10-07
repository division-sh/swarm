package runtimepersistence

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestSelectedSourceOutcomeConsumesCanonicalIdentityOwnership(t *testing.T) {
	base := SelectedSourceOutcomeFixture{
		RunID: uuid.NewString(), EventID: uuid.NewString(), EntityID: uuid.NewString(),
	}
	for _, coordinate := range []string{"run", "event", "entity"} {
		for _, value := range []string{"", uuid.Nil.String(), "not-a-uuid", "urn:uuid:" + base.RunID} {
			t.Run(coordinate+"/"+value, func(t *testing.T) {
				fixture := base
				switch coordinate {
				case "run":
					fixture.RunID = value
				case "event":
					fixture.EventID = value
				case "entity":
					fixture.EntityID = value
				}
				err := SeedSelectedSourceOutcomeForTest(context.Background(), nil, fixture)
				if err == nil || !strings.Contains(err.Error(), "exact canonical identity") {
					t.Fatalf("invalid %s coordinate reached source/store admission: %v", coordinate, err)
				}
			})
		}
	}
	err := SeedSelectedSourceOutcomeForTest(context.Background(), nil, base)
	if err == nil || !strings.Contains(err.Error(), "exact source and time") {
		t.Fatalf("canonical coordinates did not reach the unchanged source/time gate: %v", err)
	}
}
