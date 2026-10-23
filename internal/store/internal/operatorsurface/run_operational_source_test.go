package operatorsurface

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

type operationalSourceReader struct {
	stored sourceartifact.Persisted
	rooted sourceartifact.RootedSource
	err    error
	reads  atomic.Int64
	onRead func()
}

func (s *operationalSourceReader) GetSourceArtifact(ctx context.Context, _ string) (sourceartifact.Persisted, error) {
	if err := ctx.Err(); err != nil {
		return sourceartifact.Persisted{}, err
	}
	s.reads.Add(1)
	if s.onRead != nil {
		s.onRead()
	}
	return s.stored, s.err
}

func (s *operationalSourceReader) GetRootedSource(ctx context.Context, reference sourceartifact.RootAdmissionReference) (sourceartifact.RootedSource, error) {
	if err := ctx.Err(); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	if _, err := s.stored.Decode(); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	if reference != s.rooted.Admission().Reference() {
		return sourceartifact.RootedSource{}, errors.New("operational fixture root mismatch")
	}
	return s.rooted, s.err
}

// Invariant: polling reuses only the exact validated immutable source, not live facts.
func TestRunOperationalSourceMemoRetainsOnlyValidatedImmutableSemantics(t *testing.T) {
	rooted, _ := sourceartifactfixture.Rooted(t, "operational", sourceartifactfixture.Artifact())
	root := rooted.Admission().Reference()
	stored, err := sourceartifact.PersistedFromArtifact(rooted.Artifact(), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	reader := &operationalSourceReader{stored: stored, rooted: rooted}
	header := operatorread.RunHeader{BundleHash: stored.BundleHash}
	memo := newRunOperationalSourceMemo()
	first, err := memo.load(context.Background(), reader, header, root)
	if err != nil {
		t.Fatal(err)
	}
	second, err := memo.load(context.Background(), reader, header, root)
	if err != nil {
		t.Fatal(err)
	}
	firstBundle, _ := semanticview.Bundle(first)
	secondBundle, _ := semanticview.Bundle(second)
	if firstBundle != secondBundle {
		t.Fatal("repeated exact-source polling recompiled immutable semantics")
	}
	if reader.reads.Load() != 2 {
		t.Fatal("warm semantics bypassed the retained source read")
	}
	t.Run("warm_refusals", func(t *testing.T) {
		for _, tc := range []struct {
			name, want string
			change     func(*sourceartifact.Persisted)
		}{
			{"foreign_row", "contradicts exact run bundle", func(p *sourceartifact.Persisted) { p.BundleHash = strings.Repeat("a", 64) }},
			{"corrupt_blob", "decode source artifact", func(p *sourceartifact.Persisted) { p.SourceBlob = []byte("corrupt") }},
			{"member_count", "member_count", func(p *sourceartifact.Persisted) { p.MemberCount++ }},
			{"total_bytes", "total_bytes", func(p *sourceartifact.Persisted) { p.TotalBytes++ }},
			{"missing_creation", "created_at is required", func(p *sourceartifact.Persisted) { p.CreatedAt = time.Time{} }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				reader.stored = stored
				tc.change(&reader.stored)
				if got, err := memo.load(context.Background(), reader, header, root); got != nil || err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("warm invalid row returned semantics: %v / %v, want %s", got, err, tc.want)
				}
			})
		}
		reader.stored, reader.err = stored, sourceartifact.ErrNotFound
		if got, err := memo.load(context.Background(), reader, header, root); got != nil || !errors.Is(err, sourceartifact.ErrNotFound) {
			t.Fatalf("warm missing row returned semantics: %v / %v", got, err)
		}
		reader.err = nil
		// The caller's row bytes must not alias the successfully validated memo.
		reader.stored.SourceBlob[0] ^= 1
		got, err := memo.load(context.Background(), reader, header, root)
		reader.stored.SourceBlob[0] ^= 1
		if got != nil || err == nil {
			t.Fatal("caller mutation changed cached identity and concealed corruption")
		}
	})
	t.Run("changed_source_validation_and_restart", func(t *testing.T) {
		changedArtifact, err := sourceartifact.AdmitMembers(map[string][]byte{
			"manifest.yaml":        []byte("version: 1.0.0\nplatform_version: '*'\n"),
			"global/manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
			"global/schema.yaml":   []byte("stages:\n  ready: {}\n"),
		})
		if err != nil {
			t.Fatal(err)
		}
		changedRoot, _ := sourceartifactfixture.Rooted(t, "changed", changedArtifact)
		changed, err := sourceartifact.PersistedFromArtifact(changedRoot.Artifact(), stored.CreatedAt)
		if err != nil {
			t.Fatal(err)
		}
		reader.stored, reader.rooted = changed, changedRoot
		changedHeader := operatorread.RunHeader{BundleHash: changed.BundleHash}
		if got, err := memo.load(context.Background(), reader, header, root); got != nil || err == nil || err.Error() != "operational source contradicts exact run bundle" {
			t.Fatalf("changed source answered old run: %v / %v", got, err)
		}
		changedSource, err := memo.load(context.Background(), reader, changedHeader, changedRoot.Admission().Reference())
		if err != nil {
			t.Fatal(err)
		}
		if changedBundle, _ := semanticview.Bundle(changedSource); changedBundle == firstBundle || len(changedSource.FlowStates("global")) != 1 {
			t.Fatal("changed source reused an unrelated stage catalog")
		}
		invalidArtifact, err := sourceartifact.AdmitMembers(map[string][]byte{
			"manifest.yaml":        []byte("version: 1.0.0\nplatform_version: '*'\n"),
			"global/manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
			"global/schema.yaml":   []byte("not_a_schema_field: true\n"),
		})
		if err != nil {
			t.Fatal(err)
		}
		invalidRoot, _ := sourceartifactfixture.Rooted(t, "invalid", invalidArtifact)
		invalid, err := sourceartifact.PersistedFromArtifact(invalidRoot.Artifact(), stored.CreatedAt)
		if err != nil {
			t.Fatal(err)
		}
		reader.stored, reader.rooted = invalid, invalidRoot
		if got, err := memo.load(context.Background(), reader, operatorread.RunHeader{BundleHash: invalid.BundleHash}, invalidRoot.Admission().Reference()); got != nil || err == nil || !strings.Contains(err.Error(), "not_a_schema_field") {
			t.Fatalf("changed source bypassed contract validation: %v / %v", got, err)
		}
		reader.stored, reader.rooted = stored, rooted
		restarted, err := newRunOperationalSourceMemo().load(context.Background(), reader, header, root)
		if err != nil {
			t.Fatal(err)
		}
		if bundle, _ := semanticview.Bundle(restarted); bundle == firstBundle || restarted.WorkflowName() != first.WorkflowName() {
			t.Fatal("restart reused process memory or changed source semantics")
		}
	})
	t.Run("concurrent_cold_reads_and_cancelled_queue", func(t *testing.T) {
		reader.stored = stored
		cold := newRunOperationalSourceMemo()
		type result struct {
			source semanticview.Source
			err    error
		}
		results := make(chan result, 8)
		for range cap(results) {
			go func() {
				source, err := cold.load(context.Background(), reader, header, root)
				results <- result{source, err}
			}()
		}
		var shared semanticview.Source
		for range cap(results) {
			got := <-results
			if got.err != nil {
				t.Fatal(got.err)
			}
			if shared != nil && got.source != shared {
				t.Fatal("concurrent same-source reads compiled competing semantic objects")
			}
			shared = got.source
		}
		cold.gate <- struct{}{}
		read := make(chan struct{})
		reader.onRead = func() { close(read) }
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { source, err := cold.load(ctx, reader, header, root); results <- result{source, err} }()
		<-read
		cancel()
		got := <-results
		<-cold.gate
		reader.onRead = nil
		if got.source != nil || !errors.Is(got.err, context.Canceled) {
			t.Fatalf("cancelled waiter acquired cached semantics: %v / %v", got.source, got.err)
		}
		if after, err := cold.load(context.Background(), reader, header, root); err != nil || after != shared {
			t.Fatalf("cancelled waiter replaced validated semantics: %v / %v", after, err)
		}
	})
}
