package main

import (
	runtimeproductpolicy "empireai/internal/runtime/productpolicy"
	empireproductpolicy "empireai/internal/runtime/productpolicy/empire"
)

func ensureEmpireProductPolicy() {
	runtimeproductpolicy.SetDefaultFactory(func() runtimeproductpolicy.Policy {
		return empireproductpolicy.New()
	})
}
