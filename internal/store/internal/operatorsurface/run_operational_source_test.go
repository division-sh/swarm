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

// Invariant: polling reuses only the exact validated immutable source, not live facts.
func TestRunOperationalSourceMemoRetainsOnlyValidatedImmutableSemantics(t *testing.T) {
	stored, err := sourceartifact.PersistedFromArtifact(sourceartifactfixture.Artifact(), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	reader := &operationalSourceReader{stored: stored}
	header := operatorread.RunHeader{BundleHash: stored.BundleHash}
	memo := newRunOperationalSourceMemo()
	first, err := memo.load(context.Background(), reader, header)
	if err != nil {
		t.Fatal(err)
	}
	second, err := memo.load(context.Background(), reader, header)
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
				if got, err := memo.load(context.Background(), reader, header); got != nil || err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("warm invalid row returned semantics: %v / %v, want %s", got, err, tc.want)
				}
			})
		}
		reader.stored, reader.err = stored, sourceartifact.ErrNotFound
		if got, err := memo.load(context.Background(), reader, header); got != nil || !errors.Is(err, sourceartifact.ErrNotFound) {
			t.Fatalf("warm missing row returned semantics: %v / %v", got, err)
		}
		reader.err = nil
		// The caller's row bytes must not alias the successfully validated memo.
		reader.stored.SourceBlob[0] ^= 1
		got, err := memo.load(context.Background(), reader, header)
		reader.stored.SourceBlob[0] ^= 1
		if got != nil || err == nil {
			t.Fatal("caller mutation changed cached identity and concealed corruption")
		}
	})
	t.Run("changed_source_validation_and_restart", func(t *testing.T) {
		changed, err := sourceartifact.PersistedFromArtifact(sourceartifactfixture.New("global/schema.yaml", []byte("name: changed\nstages:\n  ready: {}\n")), stored.CreatedAt)
		if err != nil {
			t.Fatal(err)
		}
		reader.stored = changed
		changedHeader := operatorread.RunHeader{BundleHash: changed.BundleHash}
		if got, err := memo.load(context.Background(), reader, header); got != nil || err == nil || err.Error() != "operational source contradicts exact run bundle" {
			t.Fatalf("changed source answered old run: %v / %v", got, err)
		}
		changedSource, err := memo.load(context.Background(), reader, changedHeader)
		if err != nil {
			t.Fatal(err)
		}
		if changedBundle, _ := semanticview.Bundle(changedSource); changedBundle == firstBundle || len(changedSource.FlowStates("global")) != 1 {
			t.Fatal("changed source reused an unrelated stage catalog")
		}
		invalid, err := sourceartifact.PersistedFromArtifact(sourceartifactfixture.New("global/schema.yaml", []byte("name: invalid\nnot_a_schema_field: true\n")), stored.CreatedAt)
		if err != nil {
			t.Fatal(err)
		}
		reader.stored = invalid
		if got, err := memo.load(context.Background(), reader, operatorread.RunHeader{BundleHash: invalid.BundleHash}); got != nil || err == nil || !strings.Contains(err.Error(), "not_a_schema_field") {
			t.Fatalf("changed source bypassed contract validation: %v / %v", got, err)
		}
		reader.stored = stored
		restarted, err := newRunOperationalSourceMemo().load(context.Background(), reader, header)
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
			go func() { source, err := cold.load(context.Background(), reader, header); results <- result{source, err} }()
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
		go func() { source, err := cold.load(ctx, reader, header); results <- result{source, err} }()
		<-read
		cancel()
		got := <-results
		<-cold.gate
		reader.onRead = nil
		if got.source != nil || !errors.Is(got.err, context.Canceled) {
			t.Fatalf("cancelled waiter acquired cached semantics: %v / %v", got.source, got.err)
		}
		if after, err := cold.load(context.Background(), reader, header); err != nil || after != shared {
			t.Fatalf("cancelled waiter replaced validated semantics: %v / %v", after, err)
		}
	})
}
