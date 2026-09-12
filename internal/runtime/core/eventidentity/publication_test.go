package eventidentity

import "testing"

func TestBusinessPublicationIdentity(t *testing.T) {
	for _, row := range []struct {
		name, flow, reference, sourceFlow, instance, want string
		scope                                             PublicationScope
	}{
		{"root", ".", "work.done", ".", "", "work.done", PublicationRoot},
		{"static_local", "outer/child", "work.done", "outer/child", "outer/child", "outer/child/work.done", PublicationStatic},
		{"static_reference", "outer/child", "outer/child/work.done", "outer/child", "outer/child", "outer/child/work.done", PublicationStatic},
		{"template_one", "outer/child", "work.done", "outer/child", "outer/child/ti-one", "outer/child/ti-one/work.done", PublicationTemplate},
		{"template_two", "outer/child", "outer/child/work.done", "outer/child", "outer/child/ti-two", "outer/child/ti-two/work.done", PublicationTemplate},
		{"wrong_flow", "outer/child", "work.done", "outer/other", "outer/other/ti-one", "", PublicationTemplate},
		{"wrong_instance", "outer/child", "work.done", "outer/child", "outer/other/ti-one", "", PublicationTemplate},
		{"missing_instance", "outer/child", "work.done", "outer/child", "", "", PublicationTemplate},
		{"declaration_as_instance", "outer/child", "work.done", "outer/child", "outer/child", "", PublicationTemplate},
		{"template_as_static", "outer/child", "work.done", "outer/child", "outer/child/ti-one", "", PublicationStatic},
		{"foreign_root", "outer/child", "work.done", ".", "", "", PublicationRoot},
		{"root_instance", ".", "work.done", ".", "run-id", "", PublicationRoot},
		{"missing_scope", ".", "work.done", ".", "", "", 0},
		{"unknown_scope", ".", "work.done", ".", "", "", 255},
	} {
		t.Run(row.name, func(t *testing.T) {
			declaration, err := AdmitPublicationDeclaration(row.flow, row.reference)
			if err != nil {
				t.Fatal(err)
			}
			got, err := ProjectPublication(declaration, row.scope, row.sourceFlow, row.instance)
			if row.want == "" {
				if err == nil || got != "" {
					t.Fatalf("contradictory source admitted: %q, %v", got, err)
				}
				return
			}
			if err != nil || got != row.want {
				t.Fatalf("got %q, %v; want %q", got, err, row.want)
			}
			if declaration.Flow() != row.flow || declaration.Local() != "work.done" {
				t.Fatal("projection changed declaration identity")
			}
		})
	}
}

func TestBusinessPublicationDeclarationRejectsAliases(t *testing.T) {
	for _, row := range []struct{ flow, ref string }{
		{"", "work.done"}, {" child", "work.done"}, {"outer /child", "work.done"},
		{"child", "other/work.done"}, {"child", "child/ti-one/work.done"}, {"child", "child/child/work.done"},
		{"child", " work.done"}, {"child", "child/ work.done"}, {"child", "work. done"},
		{"child", "work.done/"}, {"child", "*"}, {"child", ""}, {".", "child/work.done"},
	} {
		t.Run(row.flow+":"+row.ref, func(t *testing.T) {
			if _, err := AdmitPublicationDeclaration(row.flow, row.ref); err == nil {
				t.Fatal("invalid declaration admitted")
			}
		})
	}
	if got, err := ProjectPublication(PublicationDeclaration{}, PublicationRoot, ".", ""); err == nil || got != "" {
		t.Fatal("zero declaration admitted")
	}
}
