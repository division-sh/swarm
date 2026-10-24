package providertriggers

import (
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packmodel"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"gopkg.in/yaml.v3"
	"testing/fstest"
)

func s1LoadProviderMembers(directory string, members map[string][]byte) (packartifact.AdmittedProvider, error) {
	files := fstest.MapFS{}
	for name, body := range members {
		files[directory+"/"+name] = &fstest.MapFile{Data: body}
	}
	return packartifact.LoadProviderFS(files, directory, "0.7.0")
}

// Authoring fixtures are mutable until admitted. Even focused request tests
// enter the lexical BODY owner instead of executing a writable DTO.
type triggerFixture manifestDefinition

func (f triggerFixture) admit() (Manifest, error) {
	key := f.Provider
	f.Provider = ""
	body, err := yaml.Marshal(manifestDefinition(f))
	if err != nil {
		return Manifest{}, err
	}
	metadata, err := f.metadata()
	if err != nil {
		return Manifest{}, err
	}
	provider, err := s1LoadProviderMembers(key, map[string][]byte{
		"manifest.yaml": metadata,
		"trigger.yaml":  body,
	})
	if err != nil {
		return Manifest{}, err
	}
	trigger, _ := provider.Body(packmodel.BodyTrigger)
	return AdmitBody(trigger)
}

func (f triggerFixture) metadata() ([]byte, error) {
	events := []string{f.EventName.Literal}
	if f.EventName.Template != "" {
		events[0] = f.EventName.Template
	}
	for _, event := range f.NormalizedEvents {
		events = append(events, event.Event)
	}
	can := packmodel.CanCapabilities{EmitEvents: events}
	if f.Secret.Required {
		can.VerifySecret = "webhook_signing_secret"
	}
	return yaml.Marshal(map[string]any{"version": "1.0.0", "platform_version": "*", "capabilities": packmodel.Capabilities{Can: can, Cannot: []string{"execute code"}}})
}

func (f triggerFixture) Validate() error {
	_, err := f.admit()
	return err
}

func (f triggerFixture) Accept(req Request) (Delivery, error) {
	manifest, err := f.admit()
	if err != nil {
		return Delivery{}, err
	}
	return manifest.Accept(req)
}

func (f triggerFixture) OutputManifest() []OutputManifest {
	return f.mustAdmit().OutputManifest()
}

func (f triggerFixture) EventCatalogEntries() map[string]runtimecontracts.EventCatalogEntry {
	return f.mustAdmit().EventCatalogEntries()
}

func (f triggerFixture) mustAdmit() Manifest {
	manifest, err := f.admit()
	if err != nil {
		panic(err)
	}
	return manifest
}
