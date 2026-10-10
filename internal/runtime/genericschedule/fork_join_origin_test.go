package genericschedule

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/google/uuid"
)

func TestForkJoinOriginDecoderRefusesPartialEvidence(t *testing.T) {
	if absent, err := DecodeForkJoinOrigin(ForkJoinOrigin{}); err != nil || absent != nil {
		t.Fatal("ordinary schedule acquired inherited provenance")
	}
	valid := ForkJoinOrigin{
		SourceActivationID: uuid.NewString(), SourceRunID: uuid.NewString(),
		PointKind: forkpoint.RunStart, PointRevision: 1,
		SourceAdmittedAt: time.Now().UTC().Truncate(time.Microsecond), Owner: ForkJoinReconstructionOwner,
	}
	if origin, err := DecodeForkJoinOrigin(valid); err != nil || origin == nil || *origin != valid {
		t.Fatalf("exact origin rejected: %v %v", origin, err)
	}
	for _, edit := range []struct {
		name  string
		apply func(*ForkJoinOrigin)
	}{
		{"source_activation", func(o *ForkJoinOrigin) { o.SourceActivationID = "" }},
		{"source_run", func(o *ForkJoinOrigin) { o.SourceRunID = "" }},
		{"point_kind", func(o *ForkJoinOrigin) { o.PointKind = "" }},
		{"point_revision", func(o *ForkJoinOrigin) { o.PointRevision = 0 }},
		{"point_event", func(o *ForkJoinOrigin) { o.PointKind = forkpoint.Event }},
		{"start_event", func(o *ForkJoinOrigin) { o.PointEventID = uuid.NewString() }},
		{"source_admission", func(o *ForkJoinOrigin) { o.SourceAdmittedAt = time.Time{} }},
		{"owner", func(o *ForkJoinOrigin) { o.Owner = "another-owner" }},
	} {
		t.Run(edit.name, func(t *testing.T) {
			bad := valid
			edit.apply(&bad)
			if _, err := DecodeForkJoinOrigin(bad); err == nil {
				t.Fatal("partial inherited evidence was treated as ordinary admission")
			}
		})
	}
}

func TestForkJoinOriginBindsDecodedActivationAndDigest(t *testing.T) {
	command := testJoinScheduleCommand(t, "orders", "orders/order-1", attemptgeneration.Generation{})
	_, ref, ok := timeridentity.ParseJoinHandle(command.Payload.Interface().(map[string]any))
	if !ok {
		t.Fatal("fixture lacks exact entry")
	}
	entry := ref.StageEntry()
	entry.OriginRunID = uuid.NewString()
	ref, err := ref.Declaration().BindStageEntry(entry, ref.Generation())
	if err != nil {
		t.Fatal(err)
	}
	handle, err := timeridentity.JoinTimeoutHandle(ref)
	if err != nil {
		t.Fatal(err)
	}
	command.Payload, err = canonicaljson.FromGo(handle.PayloadMetadata())
	if err != nil {
		t.Fatal(err)
	}
	command.ScheduleKey, command.TaskID = handle.TaskID(), handle.TaskID()
	activation := exactWorkflowJoinSchedule(t, command)
	activation.ForkJoinOrigin = &ForkJoinOrigin{
		SourceActivationID: uuid.NewString(), SourceRunID: entry.OriginRunID,
		PointKind: forkpoint.RunStart, PointRevision: 1,
		SourceAdmittedAt: activation.AdmittedAt.Add(-time.Hour), Owner: ForkJoinReconstructionOwner,
	}
	before := *activation.ForkJoinOrigin
	want, err := activation.EvidenceDigest()
	if err != nil {
		t.Fatal(err)
	}
	copy := activation.Canonical()
	copy.ForkJoinOrigin.PointRevision++
	got, err := copy.EvidenceDigest()
	if err != nil || got == want || *activation.ForkJoinOrigin != before {
		t.Fatal("canonical provenance aliases the source or escapes evidence digest")
	}
	copy.ForkJoinOrigin.SourceRunID = command.RunID
	if err := copy.Validate(); err == nil {
		t.Fatal("same-run source lineage was admitted")
	}
}
