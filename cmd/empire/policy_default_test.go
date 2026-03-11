package main

import (
	runtimeproductpolicy "empireai/internal/runtime/productpolicy"
)

func init() {
	runtimeproductpolicy.SetDefaultFactory(func() runtimeproductpolicy.Policy {
		return runtimeproductpolicy.NewGenericTestPolicy()
	})
}
