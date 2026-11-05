package auth

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestUsageBufferRoutingAndReset(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name        string
		five, seven any
		age         time.Duration
		paused      bool
	}{
		{"below", 89.0, 20.0, 0, false}, {"five hour threshold", 90.0, 0.0, 0, true},
		{"weekly threshold", 1.0, 90.0, 0, true}, {"reset", 0.0, 0.0, 0, false},
		{"inactive five hour", nil, 50.0, 0, false}, {"unknown", nil, nil, 0, true},
		{"stale", 10.0, 10.0, 4 * time.Minute, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &Auth{Provider: "claude", Metadata: map[string]any{UsageBufferPercentKey: 10, usageBufferStateKey: map[string]any{
				"checked_at": now.Add(-tc.age).Format(time.RFC3339Nano), "five_hour": tc.five, "seven_day": tc.seven,
			}}}
			blocked, _, _ := isAuthBlockedForModel(a, "claude-opus-5-5", now)
			if blocked != tc.paused {
				t.Fatalf("blocked=%v want %v", blocked, tc.paused)
			}
			if isAuthBlockedByQuotaOrAuthForModel(a, "claude-opus-5-5", now) != tc.paused {
				t.Fatal("affinity bypassed reserve")
			}
			a.Disabled = true
			if blocked, _, _ := isAuthBlockedForModel(a, "claude-opus-5-5", now); !blocked {
				t.Fatal("manual disable lost")
			}
		})
	}
}

func TestUsageBufferValidation(t *testing.T) {
	for _, value := range []any{-1, 100, 1.5, true, "10", json.Number("1.1")} {
		if _, err := ParseUsageBufferPercent(value); err == nil {
			t.Fatalf("accepted %v", value)
		}
	}
	for _, value := range []any{nil, 0, 10, float64(99), json.Number("10")} {
		if _, err := ParseUsageBufferPercent(value); err != nil {
			t.Fatal(err)
		}
	}
	for _, raw := range []string{`{}`, `{"five_hour":null,"seven_day":null}`, `{"five_hour":{"utilization":-1},"seven_day":null}`, `{"five_hour":{"utilization":"90"},"seven_day":null}`} {
		var windows map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &windows); err != nil {
			t.Fatal(err)
		}
		if _, err := parseUsageBufferWindows(windows, time.Now()); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	a := &Auth{Provider: "claude", Metadata: map[string]any{UsageBufferPercentKey: 0}}
	if usageBufferBlocked(a, time.Now()) {
		t.Fatal("zero reserve blocked routing")
	}
}

type usageBufferExecutor struct {
	schedulerTestExecutor
	body   string
	status int
}

func (e *usageBufferExecutor) Identifier() string { return "claude" }
func (e *usageBufferExecutor) HttpRequest(_ context.Context, _ *Auth, req *http.Request) (*http.Response, error) {
	if req.URL.Path != "/api/oauth/usage" || req.Header.Get("anthropic-beta") == "" {
		return nil, io.ErrUnexpectedEOF
	}
	return &http.Response{StatusCode: e.status, Body: io.NopCloser(strings.NewReader(e.body))}, nil
}

func TestUsageBufferMonitorPauseResumeAndFailure(t *testing.T) {
	m := NewManager(nil, nil, nil)
	e := &usageBufferExecutor{body: `{"five_hour":null,"seven_day":{"utilization":95}}`, status: 200}
	m.RegisterExecutor(e)
	ctx := context.Background()
	a, err := m.Register(ctx, &Auth{ID: "buffer-account", Provider: "claude", Metadata: map[string]any{UsageBufferPercentKey: 10}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		body    string
		status  int
		blocked bool
	}{
		{e.body, 200, true}, {`{"five_hour":{"utilization":0},"seven_day":{"utilization":1}}`, 200, false}, {`{}`, 429, true},
	} {
		e.body, e.status = tc.body, tc.status
		m.refreshUsageBuffers(ctx)
		got, _ := m.GetByID(a.ID)
		if usageBufferBlocked(got, time.Now()) != tc.blocked {
			t.Fatal("unexpected routing after observation", got.Metadata)
		}
		if got.Disabled {
			t.Fatal("reserve changed manual disabled flag")
		}
	}
}
