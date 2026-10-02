package channelonboarding

import (
	"errors"
	"strings"
	"testing"
)

func Test2376ChannelPrefixAndNameCollisionsRemainExact(t *testing.T) {
	left := testCandidate("abcdef0"+strings.Repeat("1", 57), "support")
	right := testCandidate("abcdef0"+strings.Repeat("2", 57), "support")
	for _, labels := range [][2]string{{"Reception@1.0.0", "Reception@1.0.0"}, {"Z-first@1.0.0", "A-second@1.0.0"}, {"abcdef0", "abcdef0"}} {
		left.SourceLabel, right.SourceLabel = labels[0], labels[1]
		for _, order := range [][]Candidate{{left, right}, {right, left}} {
			catalog, err := NewCandidateCatalog(order)
			if err != nil {
				t.Fatal(err)
			}
			rows := catalog.Candidates()
			if len(rows) != 2 || rows[0].Coordinate.BundleHash != left.Coordinate.BundleHash || rows[1].Coordinate.BundleHash != right.Coordinate.BundleHash {
				t.Fatalf("presentation changed exact ordering or cardinality: %#v", rows)
			}
			_, err = catalog.Resolve(CandidateSelection{Provider: "telegram"})
			if !errors.Is(err, ErrConflict) || strings.Contains(err.Error(), "bundle-v2:") || strings.Contains(err.Error(), "--bundle ") || !strings.Contains(err.Error(), "--source") {
				t.Fatalf("ambiguity lost or obsolete teaching survived: %v", err)
			}
			for _, candidate := range rows {
				selected, err := catalog.Resolve(CandidateSelection{Provider: "telegram", BundleHash: candidate.Coordinate.BundleHash})
				if err != nil || !selected.Coordinate.Matches(candidate.Coordinate) {
					t.Fatalf("exact identity failed: %#v %v", selected, err)
				}
			}
			for _, abbreviated := range []string{"abcdef0", left.SourceLabel, right.SourceLabel} {
				if _, err := catalog.Resolve(CandidateSelection{Provider: "telegram", BundleHash: abbreviated}); !errors.Is(err, ErrNotFound) {
					t.Fatalf("human label became selection: %q %v", abbreviated, err)
				}
			}
		}
	}
}
