package timeridentity

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestA2StageEntryOriginIsExactInheritedHistory(t *testing.T) {
	local := StageEntryRef{RunID: "child", FlowScope: "child", InstanceID: "child", InstancePath: "child", EntityID: "child", Stage: "draft", Cause: "construction"}
	raw, err := json.Marshal(local)
	if err != nil || local.Validate() != nil || strings.Contains(string(raw), "origin_run_id") {
		t.Fatalf("fresh entry carries origin: %s %v", raw, err)
	}
	inherited := local
	inherited.OriginRunID = "source"
	decoded, err := StageEntryRefFromValue(inherited)
	if err != nil || decoded != inherited || local.Key() == inherited.Key() {
		t.Fatalf("inherited comparable identity did not roundtrip: %#v %v", decoded, err)
	}
	for _, origin := range []string{"child", " source", "source ", " "} {
		bad := inherited
		bad.OriginRunID = origin
		if bad.Validate() == nil || bad.Key() != "" {
			t.Fatalf("accepted nonexact/self origin %q", origin)
		}
		if _, err := StageEntryRefFromValue(bad); err == nil {
			t.Fatalf("decoded nonexact/self origin %q", origin)
		}
	}
}
