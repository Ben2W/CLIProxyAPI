package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
)

// ClaudeUsageSnapshot is a shared, read-only observation of subscription usage.
// ObservedAt always refers to the upstream measurement, including stale fallbacks.
type ClaudeUsageSnapshot struct {
	StatusCode int
	Header     http.Header
	Body       string
	ObservedAt time.Time
	Stale      bool
}

type claudeUsageEntry struct {
	gate     *semaphore.Weighted
	last     ClaudeUsageSnapshot
	good     ClaudeUsageSnapshot
	next     time.Time
	failures int
	percent  int
}

// ClaudeUsage coalesces dashboard and buffer-monitor reads for each credential.
// It honors upstream cooldowns and retains last-known data without making it fresh.
func (m *Manager) ClaudeUsage(ctx context.Context, auth *Auth) ClaudeUsageSnapshot {
	return m.claudeUsageAt(ctx, auth, time.Now())
}

func (m *Manager) claudeUsageAt(ctx context.Context, auth *Auth, now time.Time) ClaudeUsageSnapshot {
	key := fmt.Sprintf("%s:%d", auth.ID, auth.RegistrationEpoch)
	raw, _ := m.claudeUsageCache.LoadOrStore(key, &claudeUsageEntry{gate: semaphore.NewWeighted(1)})
	entry := raw.(*claudeUsageEntry)
	if errAcquire := entry.gate.Acquire(ctx, 1); errAcquire != nil {
		return ClaudeUsageSnapshot{StatusCode: http.StatusServiceUnavailable, Header: make(http.Header), Body: `{"error":"Quota request cancelled"}`, ObservedAt: now}
	}
	defer entry.gate.Release(1)
	percent := usageBufferPercent(auth)
	if entry.percent != percent {
		entry.next = time.Time{}
		entry.percent = percent
	}
	if entry.good.Body == "" {
		state, _ := auth.Metadata[usageBufferStateKey].(map[string]any)
		checked, _ := state["checked_at"].(string)
		at, err := time.Parse(time.RFC3339Nano, checked)
		if err == nil {
			body, _ := state["usage_payload"].(string)
			if body == "" {
				// Upgrade old persisted buffer observations without inventing reset dates.
				windows := map[string]any{}
				for _, name := range []string{"five_hour", "seven_day"} {
					windows[name] = nil
					if n, ok := state[name].(float64); ok {
						windows[name] = map[string]any{"utilization": n}
					}
				}
				encoded, _ := json.Marshal(windows)
				body = string(encoded)
			}
			if _, errParse := usageStateFromBody(body, at); errParse == nil {
				entry.good = ClaudeUsageSnapshot{StatusCode: 200, Header: make(http.Header), Body: body, ObservedAt: at}
				entry.last = entry.good
				entry.next = nextClaudeUsageCheck(body, percent, at)
			}
		}
	}
	if !now.Before(entry.next) {
		result := m.fetchClaudeUsage(ctx, auth, now)
		entry.last = result
		if result.StatusCode == http.StatusOK {
			entry.good = result
			entry.failures = 0
			entry.next = nextClaudeUsageCheck(result.Body, percent, now)
		} else {
			entry.failures++
			delay := time.Minute
			if result.StatusCode == http.StatusTooManyRequests {
				delay = 5 * time.Minute
				for i := 1; i < entry.failures && delay < time.Hour; i++ {
					delay *= 2
				}
				if delay > time.Hour {
					delay = time.Hour
				}
				if seconds, err := strconv.Atoi(result.Header.Get("Retry-After")); err == nil && seconds > 0 {
					if retry := time.Duration(seconds) * time.Second; retry > delay {
						delay = retry
					}
				} else if at, errDate := http.ParseTime(result.Header.Get("Retry-After")); errDate == nil && at.Sub(now) > delay {
					delay = at.Sub(now)
				}
			}
			entry.next = now.Add(delay)
		}
	}
	result := entry.last
	if result.StatusCode != http.StatusOK && entry.good.Body != "" {
		result = entry.good
		result.Stale = true
	}
	if now.Sub(result.ObservedAt) > 2*time.Minute {
		result.Stale = true
	}
	result.Header = result.Header.Clone()
	if result.Header == nil {
		result.Header = make(http.Header)
	}
	result.Header.Set("X-CPA-Quota-Observed-At", result.ObservedAt.UTC().Format(time.RFC3339Nano))
	if result.Stale {
		result.Header.Set("X-CPA-Quota-Stale", "true")
	}
	return result
}

func (m *Manager) fetchClaudeUsage(ctx context.Context, auth *Auth, now time.Time) ClaudeUsageSnapshot {
	result := ClaudeUsageSnapshot{StatusCode: 503, Header: make(http.Header), Body: `{"error":"Quota temporarily unavailable"}`, ObservedAt: now}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.anthropic.com/api/oauth/usage", nil)
	if err != nil {
		return result
	}
	req.Header.Set("anthropic-beta", "oauth-2025-04-20")
	resp, err := m.HttpRequest(ctx, auth, req)
	if err != nil {
		return result
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.Debug("Claude usage cache: response close failed")
		}
	}()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return result
	}
	result.StatusCode = resp.StatusCode
	result.Header = resp.Header.Clone()
	result.Body = string(body)
	if result.StatusCode == 200 {
		if _, errParse := usageStateFromBody(result.Body, now); errParse != nil {
			result.StatusCode = 502
			result.Body = `{"error":"Invalid quota response"}`
		}
	}
	return result
}

func usageStateFromBody(body string, at time.Time) (map[string]any, error) {
	var windows map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &windows); err != nil {
		return nil, err
	}
	state, err := parseUsageBufferWindows(windows, at)
	if err == nil {
		state["usage_payload"] = body
	}
	return state, err
}

func nextClaudeUsageCheck(body string, percent int, observed time.Time) time.Time {
	next := observed.Add(2 * time.Minute)
	if percent == 0 {
		return next
	}
	var windows map[string]struct {
		Utilization float64 `json:"utilization"`
		ResetsAt    string  `json:"resets_at"`
	}
	if json.Unmarshal([]byte(body), &windows) != nil {
		return next
	}
	for _, key := range []string{"five_hour", "seven_day"} {
		window := windows[key]
		if window.Utilization < float64(100-percent) {
			continue
		}
		reset, err := time.Parse(time.RFC3339Nano, window.ResetsAt)
		if err == nil && reset.After(next) && reset.Before(observed.Add(8*24*time.Hour)) {
			next = reset
		}
	}
	return next
}
