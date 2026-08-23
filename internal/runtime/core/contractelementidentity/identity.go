// Package contractelementidentity owns stable authored contract-element
// identity. Display labels, map keys, indexes, paths, and event names are not
// identity inputs.
package contractelementidentity

import (
	"fmt"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type ContractElementID struct {
	value string
}

func ParseContractElementID(raw string) (ContractElementID, error) {
	parsed, err := uuid.Parse(raw)
	if err != nil || parsed == uuid.Nil || parsed.String() != raw {
		return ContractElementID{}, fmt.Errorf("contract element_id %q must be one canonical non-zero lowercase UUID", raw)
	}
	return ContractElementID{value: raw}, nil
}

func MintContractElementID() ContractElementID {
	return ContractElementID{value: uuid.NewString()}
}

func (id ContractElementID) Valid() bool {
	parsed, err := uuid.Parse(id.value)
	return err == nil && parsed != uuid.Nil && parsed.String() == id.value
}

func (id ContractElementID) String() string {
	if !id.Valid() {
		return ""
	}
	return id.value
}

func (id ContractElementID) Equal(other ContractElementID) bool { return id == other }

func (id ContractElementID) MarshalYAML() (any, error) {
	if !id.Valid() {
		return nil, fmt.Errorf("cannot encode invalid contract element identity")
	}
	return id.value, nil
}

func (id *ContractElementID) UnmarshalYAML(node *yaml.Node) error {
	if id == nil {
		return fmt.Errorf("cannot decode contract element identity into nil target")
	}
	if node == nil || node.Kind != yaml.ScalarNode || node.Tag == "!!null" {
		return fmt.Errorf("element_id must be one canonical non-zero lowercase UUID scalar")
	}
	parsed, err := ParseContractElementID(node.Value)
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

type ContractElementRef struct {
	packageKey runtimeidentity.PackageKey
	elementID  ContractElementID
}

func NewContractElementRef(packageKey runtimeidentity.PackageKey, elementID ContractElementID) (ContractElementRef, error) {
	ref := ContractElementRef{packageKey: packageKey, elementID: elementID}
	if !ref.Valid() {
		return ContractElementRef{}, fmt.Errorf("contract element reference requires canonical package and element identity")
	}
	return ref, nil
}

func ParseContractElementRef(packageKey, elementID string) (ContractElementRef, error) {
	pkg, err := runtimeidentity.ParsePackageKey(packageKey)
	if err != nil {
		return ContractElementRef{}, err
	}
	id, err := ParseContractElementID(elementID)
	if err != nil {
		return ContractElementRef{}, err
	}
	return NewContractElementRef(pkg, id)
}

func (r ContractElementRef) Valid() bool                            { return r.packageKey.Valid() && r.elementID.Valid() }
func (r ContractElementRef) PackageKey() runtimeidentity.PackageKey { return r.packageKey }
func (r ContractElementRef) ElementID() ContractElementID           { return r.elementID }
func (r ContractElementRef) Equal(other ContractElementRef) bool    { return r == other }
