package durabledata

import (
	"context"
	"database/sql"
	"time"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
)

func (o *Owner) RegisterCatalogForTest(ctx context.Context, catalog runtimedata.Catalog) error {
	write := func(ctx context.Context, tx *sql.Tx) error {
		return RegisterCatalogTx(o, ctx, tx, catalog, time.Now().UTC())
	}
	if o.sqlite != nil {
		return o.sqlite.RunTransaction(ctx, "durable data catalog fixture", write)
	}
	return o.runTransaction(ctx, write)
}

func (o *Owner) MaterializeForkPinsForTest(ctx context.Context, sourceRunID, forkRunID, targetBundleHash string, overrides []runtimedata.ExplicitPin, replay bool) ([]runtimedata.Pin, error) {
	var pins []runtimedata.Pin
	write := func(ctx context.Context, tx *sql.Tx) error {
		var err error
		pins, err = MaterializeForkPinsTx(o, ctx, tx, sourceRunID, forkRunID, targetBundleHash, overrides, replay, time.Now().UTC())
		return err
	}
	var err error
	if o.sqlite != nil {
		err = o.sqlite.RunTransaction(ctx, "durable data fork pin fixture", write)
	} else {
		err = o.runTransaction(ctx, write)
	}
	if err != nil {
		return nil, err
	}
	return pins, nil
}
