package processbinding

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestLifecycleMutationUsesExactProcessAndGrantBinding(t *testing.T) {
	previous := Binding{
		ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "owner", ProcessBootID: uuid.NewString(),
		GenerationGrantID: uuid.NewString(), BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64),
		RuntimeInstanceID: uuid.NewString(), RuntimeGeneration: 1,
	}
	for _, field := range []string{"exact", "grant", "generation", "bundle", "runtime", "lease", "owner", "boot"} {
		for _, terminal := range []bool{false, true} {
			t.Run(field+map[bool]string{true: "/terminal", false: "/live"}[terminal], func(t *testing.T) {
				target := previous
				want := "restart"
				switch field {
				case "grant":
					target.GenerationGrantID = uuid.NewString()
				case "generation":
					target.RuntimeGeneration++
				case "bundle":
					target.BundleHash = "bundle-v2:sha256:" + strings.Repeat("b", 64)
				case "runtime":
					target.RuntimeInstanceID = uuid.NewString()
				case "lease":
					target.ProcessAuthorityID = uuid.NewString()
				case "owner":
					target.ProcessOwnerID = "successor"
				case "boot":
					target.ProcessBootID = uuid.NewString()
				}
				if field != "exact" {
					want = "source_set_rebind"
					if terminal {
						want = "source_set_retire"
					}
					if field == "lease" || field == "owner" || field == "boot" {
						want = "process_takeover"
					}
				}
				got, err := previous.LifecycleMutationKind(target, " restart ", terminal)
				if err != nil || got != want {
					t.Fatalf("kind=%q want=%q err=%v", got, want, err)
				}
				if previous.SameProcessLease(target) != (want != "process_takeover") {
					t.Fatal("lease comparison disagrees with mutation class")
				}
			})
		}
	}
	for _, invalid := range []Binding{{}, {ProcessAuthorityID: "not-a-uuid"}} {
		if _, err := invalid.LifecycleMutationKind(previous, "restart", false); err == nil {
			t.Fatal("invalid predecessor classified")
		}
		if _, err := previous.LifecycleMutationKind(invalid, "restart", true); err == nil {
			t.Fatal("invalid target classified")
		}
	}
}
