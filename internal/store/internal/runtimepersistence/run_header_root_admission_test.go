package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

type rootedRunHeaderStore interface {
	rootedRunStore
	LoadRunHeader(context.Context, string) (operatorread.RunHeader, error)
	ListRunHeaders(context.Context, operatorread.RunHeaderListOptions) ([]operatorread.RunHeader, string, error)
	LoadRunOrigin(context.Context, string) (runlifecycle.RunOrigin, error)
	LoadRunClockSchedules(context.Context, string) ([]genericschedule.ClockReadback, error)
	LoadRunDebugReport(context.Context, string, operatorread.RunDebugQueryOptions) (operatorread.RunDebugReport, error)
}

func TestS1RunHeaderHistoricalRootReadbackBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := selectedRootAdmissionStore(t, backend).(rootedRunHeaderStore)
			source, path := admittedRootForStorage(t, "shop-a")
			ctx := testAuthorActivityContextForBundle(source.Artifact().BundleHash())
			if _, err := selected.EnsureRootedSource(ctx, source); err != nil {
				t.Fatal(err)
			}
			request := rootedRunCreationRequest(source)
			if _, err := selected.CreateRun(ctx, request); err != nil {
				t.Fatal(err)
			}
			before, err := selected.LoadRunHeader(ctx, request.RunID)
			if err != nil {
				t.Fatalf("read native writer header: %v", err)
			}
			if err := os.RemoveAll(path); err != nil {
				t.Fatal(err)
			}
			if removed, err := RemoveRuntimeLogFixtureSourceArtifactForTest(ctx, selected, request.Source.BundleHash()); err != nil || removed != 1 {
				t.Fatalf("delete source content: %d, %v", removed, err)
			}
			if executable, err := selected.LoadRunRootedSource(ctx, request.RunID); err == nil || executable.Validate() == nil {
				t.Fatalf("deleted source remained executable: %v", err)
			}
			snapshot, err := ReadRuntimeLogRunSnapshotForTest(ctx, selected, request.RunID)
			if err != nil || snapshot.RootReference != source.Admission().Reference() {
				t.Fatalf("lost exact historical root: %+v, %v", snapshot.RootReference, err)
			}
			after, err := selected.LoadRunHeader(ctx, request.RunID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("historical detail depends on live source: %+v, %v", after, err)
			}
			listed, cursor, err := selected.ListRunHeaders(ctx, operatorread.RunHeaderListOptions{Limit: 1})
			if err != nil || cursor != "" || len(listed) != 1 || !reflect.DeepEqual(listed[0], before) {
				t.Fatalf("historical list depends on live source: %+v, %q, %v", listed, cursor, err)
			}
			origin, err := selected.LoadRunOrigin(ctx, request.RunID)
			if err != nil || !origin.Equal(request.Origin) {
				t.Fatalf("historical origin depends on live source: %+v, %v", origin, err)
			}
			report, err := selected.LoadRunDebugReport(ctx, request.RunID, operatorread.RunDebugQueryOptions{})
			if err != nil || report.RunID != request.RunID || report.RunTableStatus != string(runlifecycle.StateRunning) {
				t.Fatalf("historical diagnostic depends on live source: %+v, %v", report, err)
			}
			clocks, err := selected.LoadRunClockSchedules(ctx, request.RunID)
			if err != nil || len(clocks) != 0 {
				t.Fatalf("historical clock readback depends on live source: %+v, %v", clocks, err)
			}
			raw, err := json.Marshal(after)
			if err != nil || strings.Contains(string(raw), "root_directory") {
				t.Fatalf("private root leaked into public header: %s, %v", raw, err)
			}
		})
	}
}

func TestS1RunHeadersRefuseMalformedPersistedRootBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := selectedRootAdmissionStore(t, backend).(rootedRunHeaderStore)
			source, _ := admittedRootForStorage(t, "shop-a")
			ctx := testAuthorActivityContextForBundle(source.Artifact().BundleHash())
			if _, err := selected.EnsureRootedSource(ctx, source); err != nil {
				t.Fatal(err)
			}
			at := time.Now().UTC()
			request := rootedRunCreationRequest(source)
			request.StartedAt = at.Add(time.Minute)
			if _, err := selected.CreateRun(ctx, request); err != nil {
				t.Fatal(err)
			}
			for i, root := range []string{"", " ", " shop-a", "shop-a ", "a/b", ".", "../shop-a", "My Shop"} {
				t.Run(strings.ReplaceAll(root, "/", "_"), func(t *testing.T) {
					bad := runlifecyclefixture.CorruptSnapshot{
						RunID: uuid.NewString(), State: string(runlifecycle.StateRunning), BundleHash: request.Source.BundleHash(),
						RootDirectory: root, OriginKind: string(request.Origin.Kind()), StartedAt: at.Add(time.Duration(i) * time.Second),
					}
					// Empty roots are forbidden by both native schemas. Exercise
					// missing carriers separately at the scanner boundary as well.
					err := AttemptCorruptRunSnapshotForTest(ctx, selected, bad)
					if root == "" {
						if err == nil || !strings.Contains(err.Error(), "source_root_directory") || !strings.Contains(strings.ToLower(err.Error()), "check") {
							t.Fatalf("missing root escaped its native constraint: %v", err)
						}
						if _, err := selected.LoadRunHeader(ctx, bad.RunID); !errors.Is(err, operatorread.ErrRunNotFound) {
							t.Fatalf("rejected missing-root insert left a header: %v", err)
						}
						return
					}
					if err != nil {
						t.Fatalf("seed malformed persisted root: %v", err)
					}
					if header, err := selected.LoadRunHeader(ctx, bad.RunID); err == nil || !reflect.DeepEqual(header, operatorread.RunHeader{}) || !strings.Contains(err.Error(), "snapshot root:") {
						t.Fatalf("invalid persisted root gained detail: %+v, %v", header, err)
					}
					if origin, err := selected.LoadRunOrigin(ctx, bad.RunID); err == nil || !reflect.DeepEqual(origin, runlifecycle.RunOrigin{}) {
						t.Fatalf("invalid persisted root gained origin: %+v, %v", origin, err)
					}
					if report, err := selected.LoadRunDebugReport(ctx, bad.RunID, operatorread.RunDebugQueryOptions{}); err == nil || !reflect.DeepEqual(report, operatorread.RunDebugReport{}) {
						t.Fatalf("invalid persisted root gained diagnostic: %+v, %v", report, err)
					}
					if clocks, err := selected.LoadRunClockSchedules(ctx, bad.RunID); err == nil || clocks != nil {
						t.Fatalf("invalid persisted root gained clock readback: %+v, %v", clocks, err)
					}
					// The good header is first; the malformed row is the lookahead.
					// An error must not return that successful prefix or its cursor.
					listed, cursor, err := selected.ListRunHeaders(ctx, operatorread.RunHeaderListOptions{Limit: 1, Since: &bad.StartedAt})
					if err == nil || listed != nil || cursor != "" || !strings.Contains(err.Error(), "snapshot root:") {
						t.Fatalf("invalid root yielded partial list/cursor: %+v, %q, %v", listed, cursor, err)
					}
				})
			}
		})
	}
}
