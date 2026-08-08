package authoractivity

import "context"

// Reader owns ordered author-activity observation without exposing query or
// dialect mechanics to runtime consumers.
type Reader interface {
	HeadAuthorActivity(context.Context) (int64, error)
	ListAuthorActivity(context.Context, ListOptions) (ListResult, error)
}
