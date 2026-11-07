package authoractivity

import "testing"

func TestPersistedDraftPreservesCanonicalEffectAuthority(t *testing.T) {
	occurrence := Occurrence{EffectAuthorityEvidence: []byte(`{"transport_parent":{"kind":"deployment_registration","revision":2},"authority_id":"exact"}`)}
	draft := DraftFromOccurrence(occurrence)
	if len(draft.EffectAuthorityEvidence) == 0 {
		t.Fatal("persisted draft dropped original effect authority")
	}
	draft.EffectAuthorityEvidence[0] = ' '
	if occurrence.EffectAuthorityEvidence[0] != '{' {
		t.Fatal("persisted draft aliases immutable occurrence evidence")
	}
	draft = DraftFromOccurrence(occurrence)
	replay := draft
	replay.EffectAuthorityEvidence = []byte(`{ "authority_id": "exact", "transport_parent": {"revision":2, "kind":"deployment_registration"}}`)
	if !DraftsEqual(draft, replay) {
		t.Fatal("JSONB formatting or member order caused an exact replay conflict")
	}
	replay.EffectAuthorityEvidence = []byte(`{"authority_id":"different","transport_parent":{"revision":2,"kind":"deployment_registration"}}`)
	if DraftsEqual(draft, replay) {
		t.Fatal("different effect authority was ignored during replay")
	}
	if !DraftsEqual(Draft{}, DraftFromOccurrence(Occurrence{})) {
		t.Fatal("ordinary authored drafts acquired auxiliary evidence")
	}
}
