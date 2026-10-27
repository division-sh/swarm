package inboundpublication

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestBindingGenerationCanonicalProjectionParity(t *testing.T) {
	base := BindingGeneration{RunID: "87654321-4321-4abc-8def-abcdef123456"}
	for _, cell := range []struct {
		name string
		edit func(*BindingGeneration)
	}{
		{"valid", func(*BindingGeneration) {}},
		{"missing_run", func(b *BindingGeneration) { b.RunID = "" }},
		{"nil_run", func(b *BindingGeneration) { b.RunID = uuid.Nil.String() }},
		{"noncanonical_run", func(b *BindingGeneration) { b.RunID = strings.ToUpper(b.RunID) }},
	} {
		t.Run(cell.name, func(t *testing.T) {
			binding := base
			cell.edit(&binding)
			identity := binding.Identity("whatsapp", "actual-provider-delivery")
			if identity.BindingGeneration() != binding {
				t.Fatal("receipt identity changed the admitted binding generation")
			}
			bindingErr, identityErr := binding.Validate(), identity.Validate()
			if (bindingErr == nil) != (cell.name == "valid") || (bindingErr == nil) != (identityErr == nil) {
				t.Fatal("binding projection and full receipt disagree", bindingErr, identityErr)
			}
		})
	}
}

func TestBindingGenerationDoesNotGrantProviderReceiptIdentity(t *testing.T) {
	binding := BindingGeneration{RunID: uuid.NewString()}
	for _, identity := range []Identity{binding.Identity("", "delivery"), binding.Identity("whatsapp", "")} {
		if identity.BindingGeneration().Validate() != nil || identity.Validate() == nil {
			t.Fatal("partial binding projection became a complete provider receipt")
		}
	}
}
