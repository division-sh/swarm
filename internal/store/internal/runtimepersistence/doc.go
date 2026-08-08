// Package runtimepersistence implements the selected runtime store's closed
// operation families. It is private to store: consumers use the typed facade
// in internal/store and cannot obtain backend, query, transaction, dialect, or
// callback authority from this package.
package runtimepersistence
