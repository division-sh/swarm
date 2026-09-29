package durabledata

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimebundleidentity "github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
)

const MaxImportShapeFields = 1024
const MaxImportShapeFieldNameBytes = 1024
const MaxImportShapeNamesBytes = 64 << 10
const MaxImportShapeEncodedBytes = 192 << 10

type ImportShapeDigest string

func (d ImportShapeDigest) Validate() error {
	return validateDigest(string(d), "resource-import-shape-v1:sha256:")
}

// ImportShapeField is one exact top-level field. Text means the compiled value
// type is plain text; the business key is still stem-owned, not assignable.
// Required describes presence, not nullability.
type ImportShapeField struct {
	Name     string `json:"name"`
	Required bool   `json:"required"`
	Text     bool   `json:"text"`
}

// ImportShape binds file-import eligibility to one immutable bundle schema.
// Non-text fields remain present so an importer can reject required omissions.
type ImportShape struct {
	BundleHash   string             `json:"bundle_hash"`
	Declaration  DeclarationRef     `json:"declaration"`
	SchemaDigest SchemaDigest       `json:"schema_digest"`
	BusinessKey  string             `json:"business_key,omitempty"`
	Fields       []ImportShapeField `json:"fields"`
}

// TextBusinessKey returns the key eligible for stem-owned directory rows.
func (s ImportShape) TextBusinessKey() string {
	for _, field := range s.Fields {
		if field.Name == s.BusinessKey && field.Required && field.Text {
			return s.BusinessKey
		}
	}
	return ""
}

// Digest seals the compiler-owned typed facts independently of the row schema
// digest, which intentionally does not encode file-import eligibility.
func (s ImportShape) Digest() (ImportShapeDigest, error) {
	if err := s.Validate(); err != nil {
		return "", err
	}
	raw, err := canonicaljson.Bytes(s)
	if err != nil {
		return "", err
	}
	return ImportShapeDigest(prefixedDigest("resource-import-shape-v1:sha256:", "swarm.resource.import_shape.v1", raw)), nil
}

// ImportShapeCatalog is the bounded, exact-bundle sidecar to Catalog.
type ImportShapeCatalog struct {
	BundleHash string        `json:"bundle_hash"`
	Shapes     []ImportShape `json:"shapes"`
}

func (c ImportShapeCatalog) Validate() error {
	if err := runtimebundleidentity.ValidateCanonicalHash(c.BundleHash); err != nil {
		return err
	}
	if len(c.Shapes) > MaxDataDeclarationsPerBundle {
		return fmt.Errorf("import shape catalog exceeds %d declarations", MaxDataDeclarationsPerBundle)
	}
	previous := DeclarationRef{}
	for index, shape := range c.Shapes {
		if err := shape.Validate(); err != nil {
			return err
		}
		if shape.BundleHash != c.BundleHash {
			return fmt.Errorf("import shape %s has a different bundle hash", shape.Declaration.Key())
		}
		if index > 0 && CompareDeclarationRef(previous, shape.Declaration) >= 0 {
			return fmt.Errorf("import shape declarations must be unique and sorted")
		}
		previous = shape.Declaration
	}
	return nil
}

func (s ImportShape) Validate() error {
	if err := runtimebundleidentity.ValidateCanonicalHash(s.BundleHash); err != nil {
		return err
	}
	if err := s.Declaration.Validate(); err != nil {
		return err
	}
	if err := s.SchemaDigest.Validate(); err != nil {
		return err
	}
	if s.Fields == nil {
		return fmt.Errorf("import shape fields must be an array")
	}
	if len(s.Fields) > MaxImportShapeFields {
		return fmt.Errorf("import shape exceeds %d fields", MaxImportShapeFields)
	}
	previous := ""
	keyFound := s.BusinessKey == ""
	totalNameBytes := 0
	for _, field := range s.Fields {
		if field.Name == "" || field.Name != strings.TrimSpace(field.Name) || strings.ContainsAny(field.Name, "\x00\r\n") {
			return fmt.Errorf("import shape field %q is not one exact top-level field", field.Name)
		}
		totalNameBytes += len(field.Name)
		if len(field.Name) > MaxImportShapeFieldNameBytes || totalNameBytes > MaxImportShapeNamesBytes {
			return fmt.Errorf("import shape field names exceed supported bounds")
		}
		if previous != "" && previous >= field.Name {
			return fmt.Errorf("import shape fields must be unique and sorted")
		}
		previous = field.Name
		if field.Name == s.BusinessKey {
			if !field.Required {
				return fmt.Errorf("import shape business key %q must be required", s.BusinessKey)
			}
			keyFound = true
		}
	}
	if !keyFound {
		return fmt.Errorf("import shape business key %q is not a top-level field", s.BusinessKey)
	}
	return nil
}
