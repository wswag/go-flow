package workflows

import (
	"github.com/wswag/go-flow/tokens"
)

func RegisterCompatibleTokenPayloadSchema(schema string) error {
	return tokens.RegisterCompatibleTokenPayloadSchema(schema)
}
func BuildTokenPayload(sets map[string]any, meta map[string]any) (string, error) {
	return tokens.BuildTokenPayload(sets, meta)
}
func ParseTokenPayload(raw string) (TokenPayload, error) { return tokens.ParseTokenPayload(raw) }
func TokenPayloadSetString(payload TokenPayload, key string) string {
	return tokens.TokenPayloadSetString(payload, key)
}
func TokenPayloadPrimarySet(payload TokenPayload) string {
	return tokens.TokenPayloadPrimarySet(payload)
}
func RewriteTokenPayloadSet(raw, key string, value any) (string, error) {
	return tokens.RewriteTokenPayloadSet(raw, key, value)
}
func DecodeTokenPayloadSet(raw, key string, target any) error {
	return tokens.DecodeTokenPayloadSet(raw, key, target)
}
