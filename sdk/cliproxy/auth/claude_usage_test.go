package auth

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type cachedUsageExecutor struct {
	schedulerTestExecutor
	calls  atomic.Int32
	status int
	body   string
	retry  string
}

func (e *cachedUsageExecutor) Identifier() string { return "claude" }
func (e *cachedUsageExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	e.calls.Add(1)
	h := make(http.Header)
	h.Set("Retry-After", e.retry)
	return &http.Response{StatusCode: e.status, Header: h, Body: io.NopCloser(strings.NewReader(e.body))}, nil
}
func cacheTestAuth(id string) *Auth {
	return &Auth{ID: id, Provider: "claude", RegistrationEpoch: 1, Metadata: map[string]any{UsageBufferPercentKey: 10}}
}

func TestClaudeUsageConcurrentReadersShareOneRequest(t *testing.T) {
	m := NewManager(nil, nil, nil)
	e := &cachedUsageExecutor{status: 200, body: `{"five_hour":null,"seven_day":{"utilization":20}}`}
	m.RegisterExecutor(e)
	now := time.Now()
	a := cacheTestAuth("a")
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			result := m.claudeUsageAt(context.Background(), a, now)
			if result.StatusCode != 200 {
				t.Error("failed request")
			}
		})
	}
	wg.Wait()
	if e.calls.Load() != 1 {
		t.Fatalf("got %d upstream calls", e.calls.Load())
	}
	// Response headers must never alias mutable cache data.
	first := m.claudeUsageAt(context.Background(), a, now)
	first.Header.Set("X-Test", "changed")
	if m.claudeUsageAt(context.Background(), a, now).Header.Get("X-Test") != "" {
		t.Fatal("headers share mutable cache")
	}
	_ = m.claudeUsageAt(context.Background(), cacheTestAuth("b"), now)
	if e.calls.Load() != 2 {
		t.Fatal("accounts shared a quota entry")
	}
}

func TestClaudeUsageRateLimitBackoffPreservesObservation(t *testing.T) {
	m := NewManager(nil, nil, nil)
	e := &cachedUsageExecutor{status: 200, body: `{"five_hour":null,"seven_day":{"utilization":20}}`}
	m.RegisterExecutor(e)
	now := time.Now()
	a := cacheTestAuth("a")
	_ = m.claudeUsageAt(context.Background(), a, now)
	e.status = 429
	e.body = `{"error":"rate limited"}`
	e.retry = "600"
	result := m.claudeUsageAt(context.Background(), a, now.Add(3*time.Minute))
	if !result.Stale || result.StatusCode != 200 || !result.ObservedAt.Equal(now) {
		t.Fatal("last-known observation mislabeled", result)
	}
	_ = m.claudeUsageAt(context.Background(), a, now.Add(12*time.Minute))
	if e.calls.Load() != 2 {
		t.Fatal("ignored Retry-After")
	}
	e.status = 200
	e.body = `{"five_hour":null,"seven_day":{"utilization":0}}`
	result = m.claudeUsageAt(context.Background(), a, now.Add(14*time.Minute))
	if result.Stale || result.StatusCode != 200 || e.calls.Load() != 3 {
		t.Fatal("did not recover after cooldown")
	}
}

func TestClaudeUsageWithoutSnapshotCaches429(t *testing.T) {
	m := NewManager(nil, nil, nil)
	e := &cachedUsageExecutor{status: 429, body: `{"error":"rate limited"}`}
	m.RegisterExecutor(e)
	now := time.Now()
	a := cacheTestAuth("a")
	for _, offset := range []time.Duration{0, time.Minute, 4 * time.Minute} {
		result := m.claudeUsageAt(context.Background(), a, now.Add(offset))
		if result.StatusCode != 429 {
			t.Fatal("invented usage data")
		}
	}
	if e.calls.Load() != 1 {
		t.Fatal("retried during default backoff")
	}
}

func TestClaudeUsageBufferWaitsForReset(t *testing.T) {
	m := NewManager(nil, nil, nil)
	now := time.Now()
	reset := now.Add(time.Hour)
	e := &cachedUsageExecutor{status: 200, body: fmt.Sprintf(`{"five_hour":null,"seven_day":{"utilization":95,"resets_at":%q}}`, reset.Format(time.RFC3339Nano))}
	m.RegisterExecutor(e)
	a := cacheTestAuth("a")
	_ = m.claudeUsageAt(context.Background(), a, now)
	result := m.claudeUsageAt(context.Background(), a, now.Add(30*time.Minute))
	if e.calls.Load() != 1 || !result.Stale {
		t.Fatal("polled paused account before reset")
	}
	state, err := usageStateFromBody(result.Body, result.ObservedAt)
	if err != nil {
		t.Fatal(err)
	}
	a.Metadata[usageBufferStateKey] = state
	if UsageBufferStatus(a, now.Add(30*time.Minute))["reason"] != "reserve_reached" {
		t.Fatal("forgot why account is paused")
	}
	e.body = `{"five_hour":null,"seven_day":{"utilization":0}}`
	result = m.claudeUsageAt(context.Background(), a, reset.Add(time.Second))
	if e.calls.Load() != 2 || result.Stale {
		t.Fatal("did not check after reset")
	}
	state, err = usageStateFromBody(result.Body, result.ObservedAt)
	if err != nil {
		t.Fatal(err)
	}
	a.Metadata[usageBufferStateKey] = state
	if usageBufferBlocked(a, reset.Add(time.Second)) {
		t.Fatal("did not resume after reset")
	}
}

func TestClaudeUsageLoadsPersistedSnapshot(t *testing.T) {
	m := NewManager(nil, nil, nil)
	now := time.Now()
	a := cacheTestAuth("a")
	body := fmt.Sprintf(`{"five_hour":null,"seven_day":{"utilization":95,"resets_at":%q}}`, now.Add(time.Hour).Format(time.RFC3339Nano))
	state, err := usageStateFromBody(body, now)
	if err != nil {
		t.Fatal(err)
	}
	a.Metadata[usageBufferStateKey] = state
	// No executor: a redundant upstream request would return 503.
	result := m.claudeUsageAt(context.Background(), a, now.Add(10*time.Minute))
	if result.StatusCode != 200 || !result.Stale || !result.ObservedAt.Equal(now) {
		t.Fatal("did not restore cached snapshot", result)
	}
}
