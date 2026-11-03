package gohlslib

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

var clientVODRetryDelays = []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second}

const clientLiveRetryDelay = 1 * time.Second

type clientStatusError struct {
	code int
}

func (e clientStatusError) Error() string {
	return fmt.Sprintf("bad status code: %d", e.code)
}

func isRetryable(err error) bool {
	if statusErr, ok := errors.AsType[clientStatusError](err); ok {
		return statusErr.code >= http.StatusInternalServerError
	}
	_, ok := errors.AsType[net.Error](err)
	return ok
}

// retryDelay allows a live retry only while it can start within one target duration
// of the first attempt, so it costs at most one segment of lag.
func (d *clientStreamDownloader) retryDelay(attempt int, elapsed time.Duration) (time.Duration, bool) {
	switch {
	case d.firstPlaylist == nil:
		return 0, false

	case d.isVOD():
		if attempt >= len(clientVODRetryDelays) {
			return 0, false
		}
		return clientVODRetryDelays[attempt], true

	default:
		targetDuration := time.Duration(d.firstPlaylist.TargetDuration) * time.Second
		return clientLiveRetryDelay, attempt == 0 && elapsed+clientLiveRetryDelay < targetDuration
	}
}

func (d *clientStreamDownloader) withRetry(ctx context.Context, do func() error) error {
	start := time.Now()

	for attempt := 0; ; attempt++ {
		err := do()
		if err == nil || ctx.Err() != nil || !isRetryable(err) {
			return err
		}

		delay, ok := d.retryDelay(attempt, time.Since(start))
		if !ok {
			return err
		}

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return fmt.Errorf("terminated")
		}
	}
}
