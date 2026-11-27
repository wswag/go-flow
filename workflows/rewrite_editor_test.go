package workflows

import (
	"encoding/json"
	"testing"

	"github.com/wswag/go-flow/tokens"
)

func TestRewriteEditedTokenPayloadSet(t *testing.T) {
	tests := []struct {
		name     string
		original any
		edit     string
		want     string
		invalid  bool
	}{
		{"object", map[string]any{"trials": 4}, `{"trials":5,"ingredients":[]}`, `{"trials":5,"ingredients":[]}`, false},
		{"array", []int{1}, `[2,3]`, `[2,3]`, false},
		{"text containing JSON", "old", `{"trials":5}`, `"{\"trials\":5}"`, false},
		{"number", 1, `2.5`, `2.5`, false},
		{"boolean", true, `false`, `false`, false},
		{"null", nil, `null`, `null`, false},
		{"invalid JSON", map[string]any{}, `{`, "", true},
		{"wrong type", map[string]any{}, `"text"`, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := tokens.BuildTokenPayload(map[string]any{"state": tt.original, "other": "kept"}, map[string]any{"admin.primary_set": "state"})
			if err != nil {
				t.Fatal(err)
			}
			for _, rewrite := range []func() (string, error){
				func() (string, error) { return rewriteEditedTokenPayloadSet(raw, "state", tt.edit) },
				func() (string, error) { return rewritePrimaryTokenPayloadSet(raw, tt.edit) },
			} {
				got, err := rewrite()
				if tt.invalid {
					if err == nil {
						t.Fatal("expected error")
					}
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				payload, err := tokens.ParseTokenPayload(got)
				if err != nil {
					t.Fatal(err)
				}
				if string(payload.Sets["state"]) != tt.want {
					t.Fatalf("state = %s, want %s", payload.Sets["state"], tt.want)
				}
				var other string
				if err := json.Unmarshal(payload.Sets["other"], &other); err != nil || other != "kept" {
					t.Fatalf("other = %q, err = %v", other, err)
				}
				if payload.Meta["admin.primary_set"] != "state" {
					t.Fatal("metadata changed")
				}
			}
		})
	}
}
