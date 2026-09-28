package durabledata

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
)

// RegisterCatalogWithImportShapesTx registers both exact-bundle catalogs in
// the caller's publication transaction. The sidecar is never inferred from a
// stored JSON Schema projection.
func RegisterCatalogWithImportShapesTx(o *Owner, ctx context.Context, tx *sql.Tx, catalog runtimedata.Catalog, shapes runtimedata.ImportShapeCatalog, now time.Time) error {
	if err := shapes.Validate(); err != nil {
		return err
	}
	if catalog.BundleHash != shapes.BundleHash || len(catalog.Declarations) != len(shapes.Shapes) {
		return fmt.Errorf("import shape catalog does not cover the exact declaration catalog")
	}
	declarations := make(map[string]runtimedata.Declaration, len(catalog.Declarations))
	for _, declaration := range catalog.Declarations {
		declarations[declaration.Ref.Key()] = declaration
	}
	for _, shape := range shapes.Shapes {
		declaration, ok := declarations[shape.Declaration.Key()]
		if !ok || declaration.SchemaDigest != shape.SchemaDigest || declaration.BusinessKey != shape.BusinessKey {
			return fmt.Errorf("import shape %s contradicts the declaration catalog", shape.Declaration.Key())
		}
		if err := validateImportShapeAgainstSchema(shape, declaration.CanonicalSchema); err != nil {
			return err
		}
	}
	catalog.ImportShapes = shapes.Shapes
	return RegisterCatalogTx(o, ctx, tx, catalog, now)
}

// RegisterImportShapeCatalogTx persists the typed sidecar after the ordinary
// catalog has been admitted, in the same transaction.
func RegisterImportShapeCatalogTx(o *Owner, ctx context.Context, tx *sql.Tx, catalog runtimedata.ImportShapeCatalog, now time.Time) error {
	if o == nil || tx == nil {
		return fmt.Errorf("durable data import-shape owner and transaction are required")
	}
	if err := catalog.Validate(); err != nil {
		return err
	}
	var count int
	if err := tx.QueryRowContext(ctx, o.query(`SELECT COUNT(*) FROM resource_bundle_declarations WHERE bundle_hash = %s`, 1), catalog.BundleHash).Scan(&count); err != nil {
		return err
	}
	if count != len(catalog.Shapes) {
		return fmt.Errorf("bundle %s import shapes do not cover the admitted declarations", catalog.BundleHash)
	}
	for _, shape := range catalog.Shapes {
		declaration, err := o.loadCatalogDeclaration(ctx, tx, catalog.BundleHash, shape.Declaration)
		if err != nil {
			return err
		}
		if declaration.SchemaDigest != shape.SchemaDigest || declaration.BusinessKey != shape.BusinessKey {
			return fmt.Errorf("import shape %s contradicts immutable declaration facts", shape.Declaration.Key())
		}
		if err := validateImportShapeAgainstSchema(shape, declaration.CanonicalSchema); err != nil {
			return err
		}
		raw, err := canonicaljson.Bytes(shape)
		if err != nil {
			return err
		}
		if len(raw) > runtimedata.MaxImportShapeEncodedBytes {
			return fmt.Errorf("import shape exceeds %d encoded bytes", runtimedata.MaxImportShapeEncodedBytes)
		}
		shapeDigest, err := shape.Digest()
		if err != nil {
			return err
		}
		var storedSchemaDigest runtimedata.SchemaDigest
		var storedShapeDigest runtimedata.ImportShapeDigest
		var stored []byte
		err = tx.QueryRowContext(ctx, o.query(`
			SELECT schema_digest, shape_digest, shape_json FROM resource_bundle_import_shapes
			WHERE bundle_hash = %s AND flow_path = %s AND event_name = %s
		`, 3), catalog.BundleHash, shape.Declaration.FlowPath, shape.Declaration.EventName).Scan(&storedSchemaDigest, &storedShapeDigest, &stored)
		if err == nil {
			if storedSchemaDigest != shape.SchemaDigest || storedShapeDigest != shapeDigest || !bytes.Equal(stored, raw) {
				return fmt.Errorf("bundle %s import shape %s conflicts with immutable catalog facts", catalog.BundleHash, shape.Declaration.Key())
			}
			if _, err := decodeImportShape(stored, storedShapeDigest, catalog.BundleHash, shape.Declaration, declaration); err != nil {
				return err
			}
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read import shape: %w", err)
		}
		_, err = tx.ExecContext(ctx, o.query(`
			INSERT INTO resource_bundle_import_shapes
			(bundle_hash, flow_path, event_name, schema_digest, shape_digest, shape_json, admitted_at)
			VALUES (%s, %s, %s, %s, %s, %s, %s)
		`, 7), catalog.BundleHash, shape.Declaration.FlowPath, shape.Declaration.EventName,
			shape.SchemaDigest, shapeDigest, raw, now.UTC().Truncate(time.Microsecond))
		if err != nil {
			return fmt.Errorf("insert import shape: %w", err)
		}
	}
	return nil
}

// GetDeclarationImportShape reads one exact detail without widening ordinary
// declaration inventory or consulting current bundle state.
func (o *Owner) GetDeclarationImportShape(ctx context.Context, bundleHash string, ref runtimedata.DeclarationRef) (runtimedata.ImportShape, error) {
	if err := o.requireCurrent(); err != nil {
		return runtimedata.ImportShape{}, err
	}
	if err := bundleidentity.ValidateCanonicalHash(bundleHash); err != nil {
		return runtimedata.ImportShape{}, err
	}
	if err := ref.Validate(); err != nil {
		return runtimedata.ImportShape{}, err
	}
	var result runtimedata.ImportShape
	err := o.runReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		declaration, err := o.loadCatalogDeclaration(txctx, tx, bundleHash, ref)
		if err != nil {
			return err
		}
		var digest runtimedata.SchemaDigest
		var shapeDigest runtimedata.ImportShapeDigest
		var raw []byte
		err = tx.QueryRowContext(txctx, o.query(`
			SELECT schema_digest, shape_digest, shape_json FROM resource_bundle_import_shapes
			WHERE bundle_hash = %s AND flow_path = %s AND event_name = %s
		`, 3), bundleHash, ref.FlowPath, ref.EventName).Scan(&digest, &shapeDigest, &raw)
		if errors.Is(err, sql.ErrNoRows) {
			return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource declaration %s is missing its import shape", ref.Key())
		}
		if err != nil {
			return err
		}
		if digest != declaration.SchemaDigest {
			return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource declaration %s import shape digest is contradictory", ref.Key())
		}
		result, err = decodeImportShape(raw, shapeDigest, bundleHash, ref, declaration)
		if err != nil {
			return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource declaration %s import shape is corrupt: %v", ref.Key(), err)
		}
		return nil
	})
	return result, err
}

func decodeImportShape(raw []byte, storedDigest runtimedata.ImportShapeDigest, bundleHash string, ref runtimedata.DeclarationRef, declaration runtimedata.Declaration) (runtimedata.ImportShape, error) {
	if len(raw) == 0 || len(raw) > runtimedata.MaxImportShapeEncodedBytes {
		return runtimedata.ImportShape{}, fmt.Errorf("import shape encoded size is outside supported bounds")
	}
	var shape runtimedata.ImportShape
	if err := json.Unmarshal(raw, &shape); err != nil {
		return runtimedata.ImportShape{}, err
	}
	canonical, err := canonicaljson.Bytes(shape)
	if err != nil || !bytes.Equal(canonical, raw) {
		return runtimedata.ImportShape{}, fmt.Errorf("import shape bytes are not canonical")
	}
	if err := shape.Validate(); err != nil {
		return runtimedata.ImportShape{}, err
	}
	actualDigest, err := shape.Digest()
	if err != nil || storedDigest.Validate() != nil || storedDigest != actualDigest {
		return runtimedata.ImportShape{}, fmt.Errorf("import shape typed digest is contradictory")
	}
	if shape.BundleHash != bundleHash || shape.Declaration != ref || shape.SchemaDigest != declaration.SchemaDigest || shape.BusinessKey != declaration.BusinessKey {
		return runtimedata.ImportShape{}, fmt.Errorf("import shape identity contradicts declaration")
	}
	if err := validateImportShapeAgainstSchema(shape, declaration.CanonicalSchema); err != nil {
		return runtimedata.ImportShape{}, err
	}
	return shape, nil
}

func validateImportShapeAgainstSchema(shape runtimedata.ImportShape, canonicalSchema []byte) error {
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
		Required   []string                   `json:"required"`
	}
	if err := json.Unmarshal(canonicalSchema, &schema); err != nil {
		return fmt.Errorf("import shape declaration schema is corrupt: %w", err)
	}
	if len(schema.Properties) != len(shape.Fields) {
		return fmt.Errorf("import shape field set contradicts declaration schema")
	}
	required := make(map[string]bool, len(schema.Required))
	for _, name := range schema.Required {
		if required[name] {
			return fmt.Errorf("declaration schema repeats required field %q", name)
		}
		required[name] = true
	}
	for _, field := range shape.Fields {
		if _, ok := schema.Properties[field.Name]; !ok || required[field.Name] != field.Required {
			return fmt.Errorf("import shape field %q contradicts declaration schema", field.Name)
		}
		delete(required, field.Name)
	}
	if len(required) != 0 {
		return fmt.Errorf("declaration schema has unprojected required fields")
	}
	return nil
}
