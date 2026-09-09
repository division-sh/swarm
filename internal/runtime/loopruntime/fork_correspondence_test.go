package loopruntime_test

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
)

func TestForkGenerationCorrespondenceContext(t *testing.T) {
	source := correspondenceActivation(t, "nested/review", "start")
	historical := source.Context()
	if _, err := source.Repeat("draft", "repeat", time.Unix(101, 0)); err != nil {
		t.Fatal(err)
	}
	c := correspondence(t, source)
	for _, original := range []map[string]any{historical, source.Context()} {
		raw, err := json.Marshal(original)
		if err != nil {
			t.Fatal(err)
		}
		var decoded map[string]any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		for _, input := range []map[string]any{original, decoded} {
			ref, err := c.AdmitSourceContext(input)
			if err != nil {
				t.Fatal(err)
			}
			child, err := c.Bind(ref)
			if err != nil {
				t.Fatal(err)
			}
			ctx, err := child.Context()
			if err != nil || ctx["attempt"] != ref.Generation().Attempt || ctx["revision_id"] != child.Generation().RevisionID || ctx["max_attempts"] != source.MaxAttempts {
				t.Fatalf("context lost exact historical reference: %#v %v", ctx, err)
			}
			if _, err := c.AdmitSourceContext(ctx); err == nil {
				t.Fatal("child context admitted as source")
			}
			ctx["attempt"] = 999
			again, err := child.Context()
			if err != nil || again["attempt"] != ref.Generation().Attempt {
				t.Fatal("context aliases owner")
			}
			if err := child.RequireDestination("child", "entity"); err != nil {
				t.Fatal(err)
			}
			if child.RequireDestination("other", "entity") == nil || child.RequireDestination("child", "other") == nil {
				t.Fatal("reference accepted wrong destination")
			}
		}
	}
	for _, field := range []string{"flow_id", "id", "activation_id", "revision_field", "revision_id", "attempt", "max_attempts"} {
		t.Run(field, func(t *testing.T) {
			ctx := source.Context()
			delete(ctx, field)
			if _, err := c.AdmitSourceContext(ctx); err == nil {
				t.Fatal("missing context coordinate accepted")
			}
			ctx = source.Context()
			ctx[field] = "wrong"
			if _, err := c.AdmitSourceContext(ctx); err == nil {
				t.Fatal("wrong context coordinate accepted")
			}
		})
	}
	for _, value := range []any{0, -1, 1.5, "2", 999} {
		ctx := source.Context()
		ctx["attempt"] = value
		if _, err := c.AdmitSourceContext(ctx); err == nil {
			t.Fatalf("bad attempt accepted: %v", value)
		}
	}
	ctx := source.Context()
	ctx["unknown"] = true
	if _, err := c.AdmitSourceContext(ctx); err == nil {
		t.Fatal("unknown context field accepted")
	}
	var zero loopruntime.ForkChildReference
	if _, err := zero.Context(); err == nil {
		t.Fatal("zero child reference yielded context")
	}
	if zero.RequireDestination("child", "entity") == nil {
		t.Fatal("zero child reference has destination authority")
	}
}

func correspondenceActivation(t *testing.T, flow, start string) loopruntime.Activation {
	t.Helper()
	a, err := loopruntime.New("source", "entity", flow, "review", "revision", start, "draft", 4, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func correspondence(t *testing.T, source ...loopruntime.Activation) *loopruntime.ForkCorrespondence {
	t.Helper()
	c, err := loopruntime.NewForkCorrespondence(source, "child", "entity")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestForkGenerationCorrespondenceDeclaredRevision(t *testing.T) {
	source := correspondenceActivation(t, "review", "start")
	first := source.Generation()
	if _, err := source.Repeat("draft", "repeat", time.Unix(101, 0)); err != nil {
		t.Fatal(err)
	}
	foreign := correspondenceActivation(t, "nested/review", "start")
	for _, order := range [][]loopruntime.Activation{{source, foreign}, {foreign, source}} {
		c := correspondence(t, order...)
		for _, generation := range []attemptgeneration.Generation{first, source.Generation()} {
			ref, err := c.AdmitSourceRevision(generation.FlowID, generation.LoopID, generation.RevisionField, generation.RevisionID)
			if err != nil || ref.Generation() != generation {
				t.Fatalf("owned revision lost: %+v %v", ref.Generation(), err)
			}
			child, err := c.Bind(ref)
			if err != nil {
				t.Fatal(err)
			}
			if child.Generation().Attempt != generation.Attempt || child.Generation().RevisionID == generation.RevisionID {
				t.Fatal("reference was not reminted at its exact attempt")
			}
			if _, err := c.AdmitSourceRevision(generation.FlowID, generation.LoopID, generation.RevisionField, child.Generation().RevisionID); err == nil {
				t.Fatal("child revision accepted as source")
			}
		}
		for _, coords := range [][4]string{
			{"review", "review", "revision", "unknown"},
			{"review", "review", "revision", foreign.RevisionID},
			{"nested/review", "review", "revision", first.RevisionID},
			{"review", "other", "revision", first.RevisionID},
			{"review", "review", "business", first.RevisionID},
			{"review", "review", "revision", first.RevisionID + " "},
			{"review", "review", "revision", ""},
		} {
			if _, err := c.AdmitSourceRevision(coords[0], coords[1], coords[2], coords[3]); err == nil {
				t.Fatalf("invalid declared revision accepted: %q", coords)
			}
		}
	}
	var zero *loopruntime.ForkCorrespondence
	if _, err := zero.AdmitSourceRevision("review", "review", "revision", first.RevisionID); err == nil {
		t.Fatal("zero correspondence accepted revision")
	}
}

func bindCorrespondence(t *testing.T, c *loopruntime.ForkCorrespondence, g attemptgeneration.Generation) loopruntime.ForkChildReference {
	t.Helper()
	s, err := c.AdmitSource(g)
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Bind(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestForkGenerationCorrespondence(t *testing.T) {
	source := correspondenceActivation(t, "", "start")
	foreign := correspondenceActivation(t, "nested/child", "start")
	wrong := correspondenceActivation(t, "", "other-start")
	for _, order := range [][]loopruntime.Activation{{source, foreign}, {foreign, source}} {
		c := correspondence(t, order...)
		ref := bindCorrespondence(t, c, source.Generation())
		want, err := loopruntime.ForkGeneration(source.Generation(), "child", "entity")
		if err != nil {
			t.Fatal(err)
		}
		if ref.Generation() != want || ref.Source().Generation() != source.Generation() {
			t.Fatal("lost exact pair")
		}
		actual := c.ProjectedActivations()
		if err := c.ValidateChild(ref, actual); err != nil {
			t.Fatal(err)
		}
		for i, j := 0, len(actual)-1; i < j; i, j = i+1, j-1 {
			actual[i], actual[j] = actual[j], actual[i]
		}
		if err := c.ValidateChild(ref, actual); err != nil {
			t.Fatal(err)
		}
		child, err := c.AdmitChild(ref.Generation(), actual)
		if err != nil || child.Generation() != ref.Generation() {
			t.Fatalf("child re-admission: %v", err)
		}
		if _, err := c.AdmitSource(child.Generation()); err == nil {
			t.Fatal("source admission reminted child")
		}
		if _, err := c.AdmitSource(wrong.Generation()); err == nil {
			t.Fatal("accepted wrong activation")
		}
		if err := c.ValidateChild(ref, nil); err == nil {
			t.Fatal("accepted missing child")
		}
	}
	foreignOnly := correspondence(t, foreign)
	if _, err := foreignOnly.AdmitSource(source.Generation()); err == nil {
		t.Fatal("foreign-only evidence accepted")
	}
	t.Run("hostile_source_coordinates", func(t *testing.T) {
		c := correspondence(t, source)
		cases := map[string]func(*attemptgeneration.Generation){
			"flow":       func(g *attemptgeneration.Generation) { g.FlowID = "nested/child" },
			"loop":       func(g *attemptgeneration.Generation) { g.LoopID = "other" },
			"activation": func(g *attemptgeneration.Generation) { g.ActivationID = wrong.ActivationID },
			"revision":   func(g *attemptgeneration.Generation) { g.RevisionID = wrong.RevisionID },
			"field":      func(g *attemptgeneration.Generation) { g.RevisionField = "other" },
			"attempt":    func(g *attemptgeneration.Generation) { g.Attempt++ },
			"whitespace": func(g *attemptgeneration.Generation) { g.LoopID += " " },
		}
		for name, mutate := range cases {
			t.Run(name, func(t *testing.T) {
				g := source.Generation()
				mutate(&g)
				if _, err := c.AdmitSource(g); err == nil {
					t.Fatal("accepted contradictory generation")
				}
			})
		}
	})
	t.Run("hostile_child_evidence", func(t *testing.T) {
		c := correspondence(t, source)
		ref := bindCorrespondence(t, c, source.Generation())
		cases := map[string]func(*loopruntime.Activation){
			"flow":       func(a *loopruntime.Activation) { a.FlowID = "other" },
			"activation": func(a *loopruntime.Activation) { *a, _ = loopruntime.Fork(wrong, "child", "entity") },
			"run":        func(a *loopruntime.Activation) { *a, _ = loopruntime.Fork(source, "other", "entity") },
			"entity":     func(a *loopruntime.Activation) { *a, _ = loopruntime.Fork(source, "child", "other") },
			"cap":        func(a *loopruntime.Activation) { a.MaxAttempts++ },
			"revision":   func(a *loopruntime.Activation) { a.RevisionID = "unknown" },
			"field":      func(a *loopruntime.Activation) { a.RevisionField = "other" },
		}
		for name, mutate := range cases {
			t.Run(name, func(t *testing.T) {
				actual := c.ProjectedActivations()
				mutate(&actual[0])
				if err := c.ValidateChild(ref, actual); err == nil {
					t.Fatal("accepted contradictory child")
				}
			})
		}
	})
	t.Run("duplicates", func(t *testing.T) {
		foreignAlias := source
		foreignAlias.FlowID = "foreign"
		if _, err := loopruntime.NewForkCorrespondence([]loopruntime.Activation{source, foreignAlias}, "child", "entity"); err == nil {
			t.Fatal("accepted one activation under multiple scopes")
		}
		for _, duplicate := range []loopruntime.Activation{source, wrong} {
			for _, order := range [][]loopruntime.Activation{{source, duplicate}, {duplicate, source}} {
				if _, err := loopruntime.NewForkCorrespondence(order, "child", "entity"); err == nil {
					t.Fatal("accepted duplicate source")
				}
			}
		}
		c := correspondence(t, source)
		ref := bindCorrespondence(t, c, source.Generation())
		child := c.ProjectedActivations()[0]
		if err := c.ValidateChild(ref, []loopruntime.Activation{child, child}); err == nil {
			t.Fatal("accepted duplicate child")
		}
	})
	t.Run("detached_and_relation_bound", func(t *testing.T) {
		input := []loopruntime.Activation{source}
		c := correspondence(t, input...)
		ref := bindCorrespondence(t, c, source.Generation())
		input[0] = wrong
		output := c.ProjectedActivations()
		output[0] = wrong
		if err := c.ValidateChild(ref, c.ProjectedActivations()); err != nil {
			t.Fatal(err)
		}
		reloaded := correspondence(t, source)
		if _, err := reloaded.Bind(ref.Source()); err == nil {
			t.Fatal("accepted reference from another admission")
		}
		if next := bindCorrespondence(t, reloaded, source.Generation()); next.Generation() != ref.Generation() {
			t.Fatal("reload changed identity")
		}
		other, err := loopruntime.NewForkCorrespondence([]loopruntime.Activation{source}, "other-child", "entity")
		if err != nil {
			t.Fatal(err)
		}
		// Replacing the public handle must not retarget a previously admitted
		// reference, even when the source generations happen to be identical.
		*c = *other
		if _, err := c.Bind(ref.Source()); err == nil {
			t.Fatal("replacing owner handle retargeted an admitted reference")
		}
	})
	t.Run("zero_and_no_loop", func(t *testing.T) {
		var nilOwner *loopruntime.ForkCorrespondence
		if err := nilOwner.RequireDestination("child", "entity"); err == nil {
			t.Fatal("nil correspondence accepted a destination")
		}
		if err := (&loopruntime.ForkCorrespondence{}).RequireDestination("", ""); err == nil {
			t.Fatal("zero correspondence accepted a destination")
		}
		if _, err := nilOwner.Bind(loopruntime.ForkSourceReference{}); err == nil {
			t.Fatal("accepted nil owner")
		}
		c := correspondence(t)
		if err := c.RequireDestination("child", "entity"); err != nil {
			t.Fatal(err)
		}
		if err := c.RequireDestination("other", "entity"); err == nil {
			t.Fatal("accepted another child run")
		}
		if err := c.RequireDestination("child", "other"); err == nil {
			t.Fatal("accepted another child entity")
		}
		if _, err := c.AdmitSource(source.Generation()); err == nil {
			t.Fatal("empty inventory hid generation reference")
		}
		if err := c.ValidateChild(loopruntime.ForkChildReference{}, nil); err == nil {
			t.Fatal("accepted zero reference")
		}
		for _, ids := range [][2]string{{"", "entity"}, {"child", ""}, {" child", "entity"}, {"child", "entity "}} {
			if _, err := loopruntime.NewForkCorrespondence(nil, ids[0], ids[1]); err == nil {
				t.Fatal("accepted noncanonical context")
			}
		}
	})
}

func TestForkGenerationCorrespondenceHistoricalOwnership(t *testing.T) {
	source := correspondenceActivation(t, "nested/static", "start")
	old := source.Generation()
	if _, err := source.Repeat("draft", "repeat", time.Unix(101, 0)); err != nil {
		t.Fatal(err)
	}
	c := correspondence(t, source)
	ref := bindCorrespondence(t, c, old)
	child := c.ProjectedActivations()[0]
	if ref.Generation().Attempt != 1 || child.Attempt != 2 {
		t.Fatal("historical attempt replaced by current")
	}
	if child.Admit(ref.Generation().RevisionID, child.CurrentStage) != loopruntime.AdmissionStale {
		t.Fatal("translation granted execution permission")
	}
	if _, err := child.Repeat("draft", "repeat-child", time.Unix(102, 0)); err != nil {
		t.Fatal(err)
	}
	if err := child.Close("done", "close-child", time.Unix(103, 0)); err != nil {
		t.Fatal(err)
	}
	if err := c.ValidateChild(ref, []loopruntime.Activation{child}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AdmitChild(child.Generation(), []loopruntime.Activation{child}); err == nil {
		t.Fatal("adopted child generation absent from source R")
	}
	if err := source.Close("done", "close-source", time.Unix(104, 0)); err != nil {
		t.Fatal(err)
	}
	closed := correspondence(t, source)
	if err := closed.ValidateChild(bindCorrespondence(t, closed, old), closed.ProjectedActivations()); err != nil {
		t.Fatal(err)
	}
	// A fork-of-fork source is valid evidence; reconstructing it with New would
	// give it the wrong activation identity.
	grand, err := loopruntime.NewForkCorrespondence([]loopruntime.Activation{child}, "grandchild", "entity")
	if err != nil {
		t.Fatal(err)
	}
	grandRef := bindCorrespondence(t, grand, ref.Generation())
	want, err := loopruntime.ForkGeneration(ref.Generation(), "grandchild", "entity")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(grandRef.Generation(), want) {
		t.Fatal("fork-of-fork identity changed")
	}
	if err := grand.ValidateChild(grandRef, grand.ProjectedActivations()); err != nil {
		t.Fatal(err)
	}
}

func TestForkGenerationCorrespondencePartialKey(t *testing.T) {
	source := correspondenceActivation(t, "", "start")
	c := correspondence(t, source)
	partial, ok := attemptgeneration.ParseKeySuffix(source.Generation().KeySuffix())
	if !ok {
		t.Fatal("canonical key failed")
	}
	ref, err := c.AdmitSourceKey(partial)
	if err != nil || ref.Generation() != source.Generation() {
		t.Fatalf("key completion: %v", err)
	}
	for _, field := range []string{"flow", "activation", "revision", "field", "attempt"} {
		g := partial
		switch field {
		case "flow":
			g.FlowID = "wrong"
		case "activation":
			g.ActivationID = "wrong"
		case "revision":
			g.RevisionID = "wrong"
		case "field":
			g.RevisionField = source.RevisionField
		case "attempt":
			g.Attempt++
		}
		if _, err := c.AdmitSourceKey(g); err == nil {
			t.Fatalf("completed contradictory %s", field)
		}
	}
}
