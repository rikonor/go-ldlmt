package ldlmt

import (
	"context"
	"net/http"
	"time"

	"golang.org/x/sync/semaphore"
)

// LoadLimiter limits load on a set of http handlers
type LoadLimiter struct {
	// Max load capacity
	capacity int64
	// Max wait time for a waiter
	maxWait time.Duration
	// Max allowed waiters
	maxWaiters int64

	// Semaphore for the waiters
	semw *semaphore.Weighted
	// Semaphore for the handlers
	semh *semaphore.Weighted
}

// New creates a new LoadLimiter
func New(capacity int64, maxWaiters int64, maxWait time.Duration) *LoadLimiter {
	return &LoadLimiter{
		maxWait: maxWait,
		// Initialize a semaphore for the waiters
		semw: semaphore.NewWeighted(maxWaiters),
		// Initialize a semaphore for the handlers
		semh: semaphore.NewWeighted(capacity),
	}
}

// Apply the given load limiting charateristic on the given handler
func (lmt *LoadLimiter) Apply(weight int64, handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Try and acquire a semaphore to become a waiter
		if ok := lmt.semw.TryAcquire(1); !ok {
			http.Error(w, "server is at full capacity", http.StatusServiceUnavailable)
			return
		}
		defer lmt.semw.Release(1)

		ctx, cancel := context.WithTimeout(context.Background(), lmt.maxWait)
		defer cancel()

		// Try and acquire a semaphore to perform the handler
		if err := lmt.semh.Acquire(ctx, weight); err != nil {
			http.Error(w, "server is at full capacity", http.StatusServiceUnavailable)
			return
		}
		defer lmt.semh.Release(weight)

		handler.ServeHTTP(w, r)
	})
}
