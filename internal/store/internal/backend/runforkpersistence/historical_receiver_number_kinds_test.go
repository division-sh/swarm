package runforkpersistence

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/mutationlog"
)

func TestRunForkHistoricalEntityProjectionPreservesNumberKinds(t *testing.T) {
	const entity = "11111111-1111-4111-8111-111111111111"
	snapshot := &runForkRevisionSnapshot{}
	for name, raw := range map[string]string{
		"integer": "7", "double": "7.0", "nested": `{"values":[7,7.0,null],"empty":[]}`,
	} {
		snapshot.EntityMutations = append(snapshot.EntityMutations, runForkRevisionEntityMutation{
			EntityID: entity, Domain: string(mutationlog.DomainAuthoredField), Path: name,
			NewValue: json.RawMessage(raw), CreatedAt: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC),
		})
	}
	states, err := loadRunForkEntityStates(snapshot)
	if err != nil || len(states) != 1 {
		t.Fatalf("historical projection: states=%+v err=%v", states, err)
	}
	want := map[string]any{
		"integer": int64(7), "double": float64(7),
		"nested": map[string]any{"values": []any{int64(7), float64(7), nil}, "empty": []any{}},
	}
	if !reflect.DeepEqual(states[0].Fields, want) {
		t.Fatalf("fixed historical projection lost native number kinds: got=%#v (integer %T, double %T) want=%#v", states[0].Fields, states[0].Fields["integer"], states[0].Fields["double"], want)
	}
}
