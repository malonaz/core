package health

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// Check defines the health check function type using gRPC health check interface.
type Check func(context.Context) error

// Checks combines several checks into a single one. It runs each health check in parallel.
func CombineChecks(checks ...Check) Check {
	return func(ctx context.Context) error {
		if len(checks) == 0 {
			return nil
		}

		errGroup, groupCtx := errgroup.WithContext(ctx)

		for _, check := range checks {
			errGroup.Go(func() error {
				return check(groupCtx)
			})
		}
		return errGroup.Wait()
	}
}
