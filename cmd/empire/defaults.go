package main

import "sync"

var empireDefaultsOnce sync.Once

func ensureEmpireDefaults() {
	empireDefaultsOnce.Do(func() {
		ensureEmpireCommgraphPolicy()
	})
}
