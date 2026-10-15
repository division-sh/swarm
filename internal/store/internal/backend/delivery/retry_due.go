package delivery

import "time"

// Normalize the due once, not its delay: adding the rounding remainder to a
// near-maximum duration could overflow before it reaches the timestamp.
func committedRetryDue(completedAt time.Time, delay time.Duration) time.Time {
	due := completedAt.Add(delay)
	if remainder := time.Duration(due.Nanosecond()) % time.Microsecond; remainder != 0 {
		due = due.Add(time.Microsecond - remainder)
	}
	return due
}
