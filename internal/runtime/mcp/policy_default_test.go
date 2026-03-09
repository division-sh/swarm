package mcp

import (
	runtimeproductpolicy "empireai/internal/runtime/productpolicy"
	empireproductpolicy "empireai/internal/runtime/productpolicy/empire"
)

func init() {
	runtimeproductpolicy.SetDefaultFactory(func() runtimeproductpolicy.Policy {
		return empireproductpolicy.New()
	})
}
