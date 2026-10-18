package sqlite

import (
	"context"
	"database/sql/driver"
	"errors"
)

type nativeRollbackStatement interface {
	driver.Stmt
	driver.StmtExecContext
	driver.StmtQueryContext
}

type rollbackStatement struct {
	native nativeRollbackStatement
	owner  *rollbackConnection
}

func (c *rollbackConnection) wrapStatement(stmt driver.Stmt, err error) (driver.Stmt, error) {
	if err != nil {
		return nil, err
	}
	native, ok := stmt.(nativeRollbackStatement)
	if !ok {
		return nil, errors.Join(errors.New("sqlite native statement lacks context interfaces"), stmt.Close())
	}
	return &rollbackStatement{native: native, owner: c}, nil
}

func (s *rollbackStatement) Close() error  { return s.native.Close() }
func (s *rollbackStatement) NumInput() int { return s.native.NumInput() }

func (s *rollbackStatement) Exec(args []driver.Value) (driver.Result, error) {
	s.owner.beforeNativeOperation()
	return s.native.Exec(args)
}

func (s *rollbackStatement) Query(args []driver.Value) (driver.Rows, error) {
	s.owner.beforeNativeOperation()
	return s.native.Query(args)
}

func (s *rollbackStatement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	s.owner.beforeNativeOperation()
	return s.native.ExecContext(ctx, args)
}

func (s *rollbackStatement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	s.owner.beforeNativeOperation()
	return s.native.QueryContext(ctx, args)
}
