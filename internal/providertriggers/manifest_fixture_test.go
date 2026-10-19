package providertriggers

import (
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packmodel"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"gopkg.in/yaml.v3"
)

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
	provider, err := packartifact.AdmitProviderMembers(key, map[string][]byte{
		"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {emit_events: [raw]}, cannot: [execute code]}\n"),
		"trigger.yaml":  body,
	})
	if err != nil {
		return Manifest{}, err
	}
	trigger, _ := provider.Body(packmodel.BodyTrigger)
	return AdmitBody(trigger)
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
