package contracts

import (
	"strings"
	"testing"
)

func TestA2CatalogRejectsEmptyAliasInsideCollections(t *testing.T) {
	catalog := TypeCatalogDocument{Scalars: map[string]ScalarTypeDecl{
		"Empty": {Base: " "},
		"Alias": {Base: "Empty"},
	}}
	for _, ref := range []string{"Empty", "Alias", "[Empty]", "[[Alias]]", "map[text]Empty", "map[Empty]integer", "map[text][Alias]"} {
		t.Run(ref, func(t *testing.T) {
			resolved, err := (CatalogTypeReference{Type: ref, Catalog: catalog}).Resolve()
			if err == nil || !strings.Contains(err.Error(), `scalar "Empty" has empty base`) || resolved.Kind != "" {
				t.Fatalf("malformed alias admission: type=%q resolved=%+v err=%v", ref, resolved, err)
			}
		})
	}
	for _, ref := range []string{"", "json", "jsonb"} {
		t.Run("explicit_dynamic_"+ref, func(t *testing.T) {
			resolved, err := (CatalogTypeReference{Type: ref, Catalog: catalog}).Resolve()
			if err != nil || resolved.Kind != CatalogTypeDynamic {
				t.Fatalf("lawful dynamic projection changed: type=%q resolved=%+v err=%v", ref, resolved, err)
			}
		})
	}
}
