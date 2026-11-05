package management

import (
	"encoding/json"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"testing"
)

func TestUsageBufferPatchValidation(t *testing.T) {
	for _, tc := range []struct {
		provider, raw string
		valid         bool
	}{
		{"claude", "10", true}, {"claude", "0", true}, {"claude", "99", true}, {"claude", "100", false},
		{"claude", "-1", false}, {"claude", "10.5", false}, {"claude", `"10"`, false}, {"codex", "10", false},
	} {
		a := &coreauth.Auth{Provider: tc.provider, Metadata: map[string]any{}}
		err := applyAuthFileFieldsPatch(a, map[string]json.RawMessage{"usage_buffer_percent": json.RawMessage(tc.raw)}, authFileRequestRetryPatch{})
		if (err == nil) != tc.valid {
			t.Fatalf("provider=%s raw=%s err=%v", tc.provider, tc.raw, err)
		}
	}
}
