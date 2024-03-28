package sqlctrl

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestDoWithContext(t *testing.T) {
	timeoutCtx, cancelFunc := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancelFunc()

	// case 1 -- no error
	err := doWithContext(timeoutCtx, func() error {
		time.Sleep(500 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Errorf("doWithContext case 1 error: %v", err)
	}

	timeoutCtx, cancelFunc = context.WithTimeout(context.Background(), 1*time.Second)
	defer cancelFunc()

	// case 2 -- deadline error because of timeout
	err = doWithContext(timeoutCtx, func() error {
		time.Sleep(1500 * time.Millisecond)
		return nil
	})
	if err == nil {
		t.Errorf("doWithContext case 2 should have error")
		t.FailNow()
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("doWithContext case 2 wrong error type")
	}

	cancelCtx, cancelFunc := context.WithCancel(context.Background())

	// case 3 -- cancel error
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err = doWithContext(cancelCtx, func() error {
			for {
				time.Sleep(1 * time.Second)
			}
		})
		wg.Done()
	}()
	cancelFunc()
	wg.Wait()

	if err == nil {
		t.Errorf("doWithContext case 3 should have error")
		t.FailNow()
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("doWithContext case 3 wrong error type: %v", err)
	}

	// case 4 -- panic in handler
	err = doWithContext(context.TODO(), func() error {
		panic("test panic")
	})
	if err == nil {
		t.Errorf("doWithContext case 4 should have error due to panic")
		t.FailNow()
	}
}
