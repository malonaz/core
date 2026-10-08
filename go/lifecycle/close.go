package lifecycle

import "golang.org/x/sync/errgroup"

// CloseAll runs the close functions concurrently and returns once all have returned: closes that
// each wait on a drain would otherwise add up.
func CloseAll(closeFns ...func()) {
	var eg errgroup.Group
	for _, closeFn := range closeFns {
		eg.Go(func() error {
			closeFn()
			return nil
		})
	}
	eg.Wait()
}
