package tokens

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// TokenPayload is the shared generic envelope for named token sets and metadata.
type TokenPayload struct {
	Schema string                     `json:"schema,omitempty"`
	Sets   map[string]json.RawMessage `json:"sets,omitempty"`
	Meta   map[string]any             `json:"meta,omitempty"`
}

// defaultTokenPayloadSchema identifies the serialized token payload format.
const defaultTokenPayloadSchema = "go-flow.token-payload.v1"

const (
	tokenPayloadPrimarySetMetaKey  = "admin.primary_set"
	tokenPayloadEditableSetMetaKey = "admin.editable_set"
)

// TokenSetFlowResult is the named token set used for a flow result.
const TokenSetFlowResult = "flow.result"

// TokenSetFlowState is the named token set used for a flow state.
const TokenSetFlowState = "flow.state"

// TokenMetaType is the metadata key that identifies a token's type.
const TokenMetaType = "token_type"

// compatibleTokenPayloadSchemas stores process-wide schema names accepted for
// reading, protected because registration and parsing may run concurrently.
var compatibleTokenPayloadSchemas = struct {
	sync.RWMutex
	names map[string]struct{}
}{names: map[string]struct{}{}}

// RegisterCompatibleTokenPayloadSchema adds a schema name that the package
// may read as a TokenPayload. Newly built or rewritten payloads always use the
// current schema name. Registration is safe to call concurrently.
func RegisterCompatibleTokenPayloadSchema(schema string) error {
	schema = strings.TrimSpace(schema)
	if schema == "" {
		return errors.New("compatible token payload schema is empty")
	}
	if schema == defaultTokenPayloadSchema {
		return nil
	}
	compatibleTokenPayloadSchemas.Lock()
	compatibleTokenPayloadSchemas.names[schema] = struct{}{}
	compatibleTokenPayloadSchemas.Unlock()
	return nil
}

// BuildTokenPayload serializes named token sets and optional metadata.
func BuildTokenPayload(sets map[string]any, meta map[string]any) (string, error) {
	// Initialize the generic container and copy metadata so callers retain
	// ownership of the map they passed in.
	payload := TokenPayload{
		Schema: defaultTokenPayloadSchema,
		Sets:   map[string]json.RawMessage{},
		Meta:   cloneTokenMetadata(meta),
	}
	for key, value := range sets {
		// Ignore blank names because sets are addressed by their non-empty keys.
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		raw, err := marshalTokenSet(value)
		if err != nil {
			return "", fmt.Errorf("marshal token set %q: %w", key, err)
		}
		payload.Sets[key] = raw
	}
	// Serialize only after all sets have been encoded successfully.
	out, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// marshalTokenSet converts a set value to JSON while preserving raw JSON
// supplied as json.RawMessage or []byte.
func marshalTokenSet(value any) (json.RawMessage, error) {
	switch typed := value.(type) {
	case json.RawMessage:
		// Validate raw values before copying them into the payload.
		if !json.Valid(typed) {
			return nil, errors.New("invalid JSON value")
		}
		return append(json.RawMessage(nil), typed...), nil
	case []byte:
		// Treat byte slices as already encoded JSON, matching RawMessage.
		if !json.Valid(typed) {
			return nil, errors.New("invalid JSON value")
		}
		return append(json.RawMessage(nil), typed...), nil
	default:
		// Ordinary Go values are encoded by the standard JSON encoder.
		raw, err := json.Marshal(value)
		return json.RawMessage(raw), err
	}
}

// ParseTokenPayload parses the generic token format using the current schema
// and any compatible schemas registered with RegisterCompatibleTokenPayloadSchema.
// Opaque strings and the former application-specific envelope format are not
// accepted. An empty or unregistered schema is invalid.
func ParseTokenPayload(raw string) (TokenPayload, error) {
	// Whitespace-only input cannot represent a serialized payload.
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return TokenPayload{}, errors.New("token is empty")
	}
	var payload TokenPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return TokenPayload{}, fmt.Errorf("decode token payload: %w", err)
	}
	// Reject valid JSON objects that do not carry any token payload fields.
	if !isTokenPayload(payload) {
		return TokenPayload{}, errors.New("invalid token payload")
	}
	// Normalize absent collections so callers can safely add sets or metadata.
	if payload.Sets == nil {
		payload.Sets = map[string]json.RawMessage{}
	}
	if payload.Meta == nil {
		payload.Meta = map[string]any{}
	}
	return payload, nil
}

// isTokenPayload reports whether the payload uses the current schema or a
// registered compatible schema. Matching the generic field shape alone is
// insufficient to accept an unknown format.
func isTokenPayload(payload TokenPayload) bool {
	schema := strings.TrimSpace(payload.Schema)
	if schema == defaultTokenPayloadSchema {
		return true
	}
	compatibleTokenPayloadSchemas.RLock()
	_, ok := compatibleTokenPayloadSchemas.names[schema]
	compatibleTokenPayloadSchemas.RUnlock()
	return schema != "" && ok
}

// cloneTokenMetadata copies metadata into a new map to prevent map mutations
// from affecting the caller's input.
func cloneTokenMetadata(meta map[string]any) map[string]any {
	cloned := make(map[string]any, len(meta))
	for key, value := range meta {
		cloned[key] = value
	}
	return cloned
}

// TokenPayloadSetString returns a set's string value, or its JSON encoding
// when the value is structured.
func TokenPayloadSetString(payload TokenPayload, key string) string {
	// Missing sets have no string representation.
	raw, ok := payload.Sets[strings.TrimSpace(key)]
	if !ok {
		return ""
	}
	// Decode JSON strings to avoid returning their serialized quotation marks;
	// structured values remain available in their original JSON form.
	var value string
	if err := json.Unmarshal(raw, &value); err == nil {
		return strings.TrimSpace(value)
	}
	return strings.TrimSpace(string(raw))
}

// TokenPayloadPrimarySet returns the set named by the standard editable-set
// metadata keys, preferring admin.primary_set.
func TokenPayloadPrimarySet(payload TokenPayload) string {
	// The primary set is authoritative when both metadata keys are present.
	if key := stringFromAny(payload.Meta[tokenPayloadPrimarySetMetaKey]); key != "" {
		return key
	}
	return TokenPayloadEditableSet(payload)
}

// SetTokenPayloadPrimarySetKey changes the set key recorded by
// admin.primary_set while preserving the other sets and metadata.
func SetTokenPayloadPrimarySetKey(raw string, key string) (string, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", errors.New("token primary set key is empty")
	}
	payload, err := ParseTokenPayload(raw)
	if err != nil {
		return "", err
	}
	payload.Meta[tokenPayloadPrimarySetMetaKey] = key
	payload.Schema = defaultTokenPayloadSchema
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

// TokenPayloadEditableSet returns the set named by admin.editable_set metadata.
func TokenPayloadEditableSet(payload TokenPayload) string {
	return stringFromAny(payload.Meta[tokenPayloadEditableSetMetaKey])
}

// TokenPayloadType returns the token type metadata value.
func TokenPayloadType(payload TokenPayload) string {
	return stringFromAny(payload.Meta[TokenMetaType])
}

// SetTokenPayloadType sets the token type metadata while
// preserving the token's sets and other metadata.
func SetTokenPayloadType(raw string, tokenType string) (string, error) {
	tokenType = strings.TrimSpace(tokenType)
	if tokenType == "" {
		return "", errors.New("workflow token type is empty")
	}
	payload, err := ParseTokenPayload(raw)
	if err != nil {
		return "", err
	}
	payload.Meta[TokenMetaType] = tokenType
	payload.Schema = defaultTokenPayloadSchema
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

// SetTokenPayloadEditableSetKey changes the set key recorded by
// admin.editable_set while preserving the other sets and metadata.
func SetTokenPayloadEditableSetKey(raw string, key string) (string, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", errors.New("token editable set key is empty")
	}
	payload, err := ParseTokenPayload(raw)
	if err != nil {
		return "", err
	}
	payload.Meta[tokenPayloadEditableSetMetaKey] = key
	payload.Schema = defaultTokenPayloadSchema
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

// RewriteTokenPayloadSet replaces one named set while preserving all other
// sets and metadata. Registered compatible payloads can be rewritten; the
// result always uses the current schema name.
func RewriteTokenPayloadSet(raw string, key string, value any) (string, error) {
	// A set key is required to identify which value should be replaced.
	key = strings.TrimSpace(key)
	if key == "" {
		return "", errors.New("token set key is empty")
	}
	payload, err := ParseTokenPayload(raw)
	if err != nil {
		return "", err
	}
	// Encode the replacement before changing the parsed payload.
	set, err := marshalTokenSet(value)
	if err != nil {
		return "", err
	}
	// Rewriting normalizes the schema to the current generic format.
	payload.Sets[key] = set
	payload.Schema = defaultTokenPayloadSchema
	out, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// SetTokenPayloadEditableSet writes an already-serialized JSON value into the
// set selected by admin.editable_set metadata.
func SetTokenPayloadEditableSet(raw string, value json.RawMessage) (string, error) {
	payload, err := ParseTokenPayload(raw)
	if err != nil {
		return "", err
	}
	key := TokenPayloadEditableSet(payload)
	if key == "" {
		return "", errors.New("token payload has no editable_set")
	}
	if !json.Valid(value) {
		return "", fmt.Errorf("set token payload set %q: invalid JSON value", key)
	}
	payload.Sets[key] = append(json.RawMessage(nil), value...)
	payload.Schema = defaultTokenPayloadSchema
	out, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// MarshalTokenPayloadEditableSet marshals value as JSON and writes it to the
// set selected by admin.editable_set metadata.
func MarshalTokenPayloadEditableSet(raw string, value any) (string, error) {
	encoded, err := marshalTokenSet(value)
	if err != nil {
		return "", fmt.Errorf("marshal editable set: %w", err)
	}
	return SetTokenPayloadEditableSet(raw, encoded)
}

// MergeTokenPayloadEditableSet merges fields into the object selected by
// admin.editable_set metadata. Incoming fields replace existing fields with
// the same name; all other payload sets and metadata are preserved.
func MergeTokenPayloadEditableSet(raw string, value any) (string, error) {
	payload, err := ParseTokenPayload(raw)
	if err != nil {
		return "", err
	}
	key := TokenPayloadEditableSet(payload)
	if key == "" {
		return "", errors.New("token payload has no editable_set")
	}
	existing, ok := payload.Sets[key]
	if !ok {
		return "", fmt.Errorf("token payload has no set %q", key)
	}
	var currentFields map[string]json.RawMessage
	if err := json.Unmarshal(existing, &currentFields); err != nil || currentFields == nil {
		if err == nil {
			err = errors.New("set is not a JSON object")
		}
		return "", fmt.Errorf("merge editable set %q: existing value: %w", key, err)
	}
	incoming, err := marshalTokenSet(value)
	if err != nil {
		return "", fmt.Errorf("merge editable set %q: %w", key, err)
	}
	var incomingFields map[string]json.RawMessage
	if err := json.Unmarshal(incoming, &incomingFields); err != nil || incomingFields == nil {
		if err == nil {
			err = errors.New("value is not a JSON object")
		}
		return "", fmt.Errorf("merge editable set %q: incoming value: %w", key, err)
	}
	for field, fieldValue := range incomingFields {
		currentFields[field] = fieldValue
	}
	merged, err := json.Marshal(currentFields)
	if err != nil {
		return "", err
	}
	payload.Sets[key] = merged
	payload.Schema = defaultTokenPayloadSchema
	out, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// MergeTokenPayload adds or replaces sets and metadata while preserving all
// existing entries. New values take precedence when keys overlap.
func MergeTokenPayload(raw string, sets map[string]any, meta map[string]any) (string, error) {
	payload, err := ParseTokenPayload(raw)
	if err != nil {
		return "", err
	}
	for key, value := range sets {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		encoded, err := marshalTokenSet(value)
		if err != nil {
			return "", fmt.Errorf("marshal token set %q: %w", key, err)
		}
		payload.Sets[key] = encoded
	}
	for key, value := range meta {
		payload.Meta[key] = value
	}
	payload.Schema = defaultTokenPayloadSchema
	out, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// DecodeTokenPayloadSet decodes a named set into target. It returns an error
// when the payload schema is unsupported, the set is absent, or its JSON
// cannot be decoded into target.
func DecodeTokenPayloadSet(raw string, key string, target any) error {
	payload, err := ParseTokenPayload(raw)
	if err != nil {
		return err
	}
	return decodeTokenPayloadSet(payload, key, target)
}

func decodeTokenPayloadSet(payload TokenPayload, key string, target any) error {
	// Decode only the requested set; other sets and metadata are independent.
	key = strings.TrimSpace(key)
	value, ok := payload.Sets[key]
	if !ok {
		return fmt.Errorf("token payload has no set %q", key)
	}
	return json.Unmarshal(value, target)
}

// DecodeTokenPayloadMetaSet resolves a set key from metadata and decodes that
// set into target. The metadata value must be a non-empty string.
func DecodeTokenPayloadMetaSet(raw string, metaKey string, target any) error {
	payload, err := ParseTokenPayload(raw)
	if err != nil {
		return err
	}
	return decodeTokenPayloadMetaSet(payload, metaKey, target)
}

func decodeTokenPayloadMetaSet(payload TokenPayload, metaKey string, target any) error {
	metaKey = strings.TrimSpace(metaKey)
	setKey := stringFromAny(payload.Meta[metaKey])
	if setKey == "" {
		return fmt.Errorf("token payload has no set key in metadata %q", metaKey)
	}
	return decodeTokenPayloadSet(payload, setKey, target)
}

// stringFromAny returns trimmed string metadata and an empty string for other
// value types.
func stringFromAny(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}
