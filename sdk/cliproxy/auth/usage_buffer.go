package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

const UsageBufferPercentKey = "usage_buffer_percent"
const usageBufferStateKey = "usage_buffer_state"

// ParseUsageBufferPercent validates a per-credential reserve. Zero disables it.
func ParseUsageBufferPercent(value any) (int, error) {
	if value == nil {
		return 0, nil
	}
	var raw string
	switch v := value.(type) {
	case json.Number:
		raw = v.String()
	case float64:
		raw = strconv.FormatFloat(v, 'f', -1, 64)
	case int:
		raw = strconv.Itoa(v)
	case int64:
		raw = strconv.FormatInt(v, 10)
	default:
		return 0, fmt.Errorf("usage_buffer_percent must be an integer between 0 and 99")
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 || n > 99 {
		return 0, fmt.Errorf("usage_buffer_percent must be an integer between 0 and 99")
	}
	return n, nil
}

func usageBufferPercent(auth *Auth) int {
	if auth == nil || !strings.EqualFold(auth.Provider, "claude") {
		return 0
	}
	n, _ := ParseUsageBufferPercent(auth.Metadata[UsageBufferPercentKey])
	return n
}

// UsageBufferStatus returns only non-secret reserve settings and observations.
func UsageBufferStatus(auth *Auth, now time.Time) map[string]any {
	percent := usageBufferPercent(auth)
	if percent == 0 {
		return nil
	}
	state, _ := auth.Metadata[usageBufferStateKey].(map[string]any)
	result := map[string]any{"percent": percent, "paused": true, "reason": "awaiting_usage"}
	for _, key := range []string{"checked_at", "five_hour", "seven_day"} {
		if v, ok := state[key]; ok {
			result[key] = v
		}
	}
	if state["error"] != nil {
		result["reason"] = "usage_unavailable"
		return result
	}
	seen := false
	for _, key := range []string{"five_hour", "seven_day"} {
		raw := state[key]
		if raw == nil {
			continue
		}
		value, ok := raw.(float64)
		if !ok || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return result
		}
		seen = true
		if value >= float64(100-percent) {
			result["reason"] = "reserve_reached"
			return result
		}
	}
	checked, _ := state["checked_at"].(string)
	at, err := time.Parse(time.RFC3339Nano, checked)
	if err != nil || now.Sub(at) > 3*time.Minute || at.After(now.Add(time.Minute)) {
		return result
	}
	if seen {
		result["paused"] = false
		result["reason"] = "below_threshold"
	}
	return result
}

func usageBufferBlocked(auth *Auth, now time.Time) bool {
	state := UsageBufferStatus(auth, now)
	return state != nil && state["paused"] == true
}

// StartUsageBufferMonitor runs with the service lifetime. Unmeasured or stale
// credentials stay out of routing until a successful observation is available.
func (m *Manager) StartUsageBufferMonitor(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			m.refreshUsageBuffers(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (m *Manager) refreshUsageBuffers(ctx context.Context) {
	for _, auth := range m.List() {
		if ctx.Err() != nil {
			return
		}
		if usageBufferPercent(auth) == 0 || auth.Disabled {
			continue
		}
		state, err := m.fetchUsageBuffer(ctx, auth)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			state = map[string]any{"checked_at": time.Now().UTC().Format(time.RFC3339Nano), "error": "usage_unavailable"}
		}
		_, _, errUpdate := m.Modify(ctx, auth.ID, auth.RegistrationEpoch, func(current *Auth) error {
			if current.Metadata == nil {
				current.Metadata = make(map[string]any)
			}
			current.Metadata[usageBufferStateKey] = state
			return nil
		})
		if errUpdate != nil {
			log.Warn("usage buffer: failed to save credential observation")
		}
	}
}

func (m *Manager) fetchUsageBuffer(ctx context.Context, auth *Auth) (map[string]any, error) {
	snapshot := m.ClaudeUsage(ctx, auth)
	if snapshot.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("usage HTTP %d", snapshot.StatusCode)
	}
	return usageStateFromBody(snapshot.Body, snapshot.ObservedAt)
}

func parseUsageBufferWindows(windows map[string]json.RawMessage, now time.Time) (map[string]any, error) {
	state := map[string]any{"checked_at": now.UTC().Format(time.RFC3339Nano)}
	seen := false
	for _, key := range []string{"five_hour", "seven_day"} {
		raw, exists := windows[key]
		if !exists {
			return nil, fmt.Errorf("missing usage window")
		}
		state[key] = nil
		if string(raw) == "null" {
			continue
		}
		var window struct {
			Utilization *float64 `json:"utilization"`
		}
		if err := json.Unmarshal(raw, &window); err != nil || window.Utilization == nil {
			return nil, fmt.Errorf("invalid usage window")
		}
		value := *window.Utilization
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return nil, fmt.Errorf("invalid usage percentage")
		}
		state[key] = value
		seen = true
	}
	if !seen {
		return nil, fmt.Errorf("no measured usage window")
	}
	return state, nil
}
