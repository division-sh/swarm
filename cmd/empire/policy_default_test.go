package main

import (
	"os"
	"testing"

	runtimeproductpolicy "empireai/internal/runtime/productpolicy"
)

func TestMain(m *testing.M) {
	runtimeproductpolicy.SetDefaultFactory(func() runtimeproductpolicy.Policy {
		return runtimeproductpolicy.NewGenericTestPolicy()
	})
	os.Exit(m.Run())
}
