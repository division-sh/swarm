package ownership

import "context"

type Lease interface {
	Release(ctx context.Context) error
}
