package tokens_test

import (
	"testing"

	"github.com/wswag/go-flow/tokens"
)

func TestTokenBuilderReaderAndEditableSetUpdates(t *testing.T) {
	raw, err := tokens.NewTokenBuilder().
		Set("profile", map[string]any{"name": "Ada", "role": "engineer"}).
		Set("untouched", map[string]string{"value": "keep"}).
		EditableSet("profile").
		PrimarySet("untouched").
		Type("example").
		SetMeta("source", "test").
		Build()
	if err != nil {
		t.Fatalf("build token: %v", err)
	}

	reader := tokens.NewTokenReader(raw)
	if err := reader.Err(); err != nil {
		t.Fatalf("read token: %v", err)
	}
	if got := reader.EditableSet(); got != "profile" {
		t.Fatalf("editable set = %q, want profile", got)
	}
	if got := reader.PrimarySet(); got != "untouched" {
		t.Fatalf("primary set = %q, want untouched", got)
	}
	if got := reader.Type(); got != "example" {
		t.Fatalf("type = %q, want example", got)
	}
	if value, ok := reader.MetaValue("source"); !ok || value != "test" {
		t.Fatalf("source metadata = %#v, present=%t", value, ok)
	}

	merged, err := tokens.NewTokenBuilderFrom(raw).
		MergeEditableValue(map[string]any{"role": "architect", "active": true}).
		Build()
	if err != nil {
		t.Fatalf("merge editable set: %v", err)
	}
	mergedReader := tokens.NewTokenReader(merged)
	var profile map[string]any
	if err := mergedReader.DecodeMetaSet("admin.editable_set", &profile); err != nil {
		t.Fatalf("decode merged editable set: %v", err)
	}
	if profile["name"] != "Ada" || profile["role"] != "architect" || profile["active"] != true {
		t.Fatalf("merged profile = %#v", profile)
	}
	if mergedReader.String("untouched") != `{"value":"keep"}` {
		t.Fatalf("untouched set changed: %s", mergedReader.String("untouched"))
	}

	replaced, err := tokens.NewTokenBuilderFrom(merged).
		EditableValue(map[string]string{"name": "Grace"}).
		Build()
	if err != nil {
		t.Fatalf("replace editable set: %v", err)
	}
	replacedReader := tokens.NewTokenReader(replaced)
	profile = nil
	if err := replacedReader.Decode("profile", &profile); err != nil {
		t.Fatalf("decode replaced set: %v", err)
	}
	if len(profile) != 1 || profile["name"] != "Grace" {
		t.Fatalf("replaced profile = %#v", profile)
	}
}

func TestTokenBuilderReaderRoundTripsObjectMetadata(t *testing.T) {
	type requestMetadata struct {
		Tenant string   `json:"tenant"`
		Scopes []string `json:"scopes"`
	}
	want := requestMetadata{Tenant: "acme", Scopes: []string{"read", "write"}}
	raw, err := tokens.NewTokenBuilder().SetMeta("request", want).Build()
	if err != nil {
		t.Fatalf("build token: %v", err)
	}

	reader := tokens.NewTokenReader(raw)
	var got requestMetadata
	if err := reader.Meta("request", &got); err != nil {
		t.Fatalf("decode object metadata: %v", err)
	}
	if got.Tenant != want.Tenant || len(got.Scopes) != len(want.Scopes) || got.Scopes[0] != want.Scopes[0] || got.Scopes[1] != want.Scopes[1] {
		t.Fatalf("metadata = %#v, want %#v", got, want)
	}
}
