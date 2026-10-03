package providertriggers

import (
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"gopkg.in/yaml.v3"
)

// Authoring fixtures are mutable until admitted. Even focused request tests
// enter the lexical BODY owner instead of executing a writable DTO.
type triggerFixture manifestDefinition

func (f triggerFixture) admit() (Manifest, error) {
	body, err := yaml.Marshal(manifestDefinition(f))
	if err != nil {
		return Manifest{}, err
	}
	return ParseManifest(body)
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
