package store

import "errors"

var ErrUnknownSchemaType = errors.New("store: unknown schema type")

var ErrRunNotFound = errors.New("store: run not found")

var ErrInvalidRunListCursor = errors.New("store: invalid run list cursor")
