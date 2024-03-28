package sqlctrl

import (
	"context"
	"fmt"
)

// Wrapper for context usage.
// Blocks thread execution until provided @handler or @ctx context is done.
func doWithContext(ctx context.Context, handler func() error) error {
	if ctx == nil || handler == nil {
		return ErrInvalidArgument
	}

	errChan := make(chan error)

	go func() {
		defer func() {
			r := recover()
			if r == nil {
				errChan <- nil
				return
			}

			err, ok := r.(error)
			if ok {
				errChan <- err
				return
			}

			errChan <- fmt.Errorf("panic in handler: %v", r)
		}()

		errChan <- handler()
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
