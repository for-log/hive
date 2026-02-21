package hivedriver

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"hive/gen/hivepb"
)

// ErrDatabaseNotReady is returned when the router has no alive masters yet
// and all backoff attempts are exhausted.
var ErrDatabaseNotReady = errors.New("hivedriver: database not ready: no alive masters")

// errMsgNotReady is the exact gRPC status message the router sends when no masters are alive.
// The hivedriver matches this string to trigger backoff retries.
const errMsgNotReady = "database_not_ready: no alive masters"

var notReadyBackoff = []time.Duration{5 * time.Second, 15 * time.Second, 45 * time.Second}

func isNotReady(err error) bool {
	s, ok := status.FromError(err)
	return ok && s.Code() == codes.Unavailable && s.Message() == errMsgNotReady
}

func executeWithBackoff(ctx context.Context, log *slog.Logger, fn func() (*hivepb.ExecuteResponse, error)) (*hivepb.ExecuteResponse, error) {
	resp, err := fn()
	if err == nil {
		return resp, nil
	}

	for _, wait := range notReadyBackoff {
		if !isNotReady(err) {
			return nil, err
		}
		log.Warn("hivedriver: no alive masters, retrying", "wait", wait)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
		resp, err = fn()
		if err == nil {
			return resp, nil
		}
	}

	if isNotReady(err) {
		return nil, ErrDatabaseNotReady
	}
	return nil, err
}
