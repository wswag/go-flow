package tokens

import (
	"encoding/json"
	"fmt"
	"strings"
)

// TokenBuilder constructs a generic token payload or applies changes to an
// existing one. Its methods can be chained; Build returns the serialized
// payload and any validation or encoding error.
type TokenBuilder struct {
	sets                  map[string]any
	meta                  map[string]any
	raw                   string
	mergeExisting         bool
	editableSet           *string
	primarySet            *string
	tokenType             *string
	editableValue         any
	hasEditableValue      bool
	mergeEditableValue    any
	hasMergeEditableValue bool
	err                   error
}

// NewTokenBuilder creates an empty token payload builder with state as the
// primary set and input as the editable set. These selections can be changed
// with PrimarySet and EditableSet.
func NewTokenBuilder() *TokenBuilder {
	b := &TokenBuilder{sets: map[string]any{}, meta: map[string]any{}}
	b.PrimarySet(TokenSetState)
	b.EditableSet(TokenSetInput)
	return b
}

// NewTokenBuilderFrom creates a builder that parses and preserves an existing
// payload's sets and metadata when Build is called. It defaults to state as
// the primary set and input as the editable set; these selections can be
// changed with PrimarySet and EditableSet. Invalid or empty input is reported
// by Build.
func NewTokenBuilderFrom(raw string) *TokenBuilder {
	b := &TokenBuilder{raw: raw, mergeExisting: true, sets: map[string]any{}, meta: map[string]any{}}
	b.PrimarySet(TokenSetState)
	b.EditableSet(TokenSetInput)
	return b
}

// Set adds or replaces a named set. A blank key or invalid value is reported
// by Build.
func (b *TokenBuilder) Set(key string, value any) *TokenBuilder {
	key = strings.TrimSpace(key)
	if key == "" {
		b.err = fmt.Errorf("token set key is empty")
		return b
	}
	if b.sets == nil {
		b.sets = map[string]any{}
	}
	b.sets[key] = value
	return b
}

// SetResult stores a node's output for the next node, or the flow's final
// result when called by the last node.
func (b *TokenBuilder) SetResult(value any) *TokenBuilder {
	return b.Set(TokenSetResult, value)
}

// SetInput stores the value a node receives. Flow starts and preceding nodes
// provide this set to the next node.
func (b *TokenBuilder) SetInput(value any) *TokenBuilder {
	return b.Set(TokenSetInput, value)
}

// SetState stores information that is needed across nodes. If any of that
// information is part of the flow's result, the last node must also write it
// with SetResult.
func (b *TokenBuilder) SetState(value any) *TokenBuilder {
	return b.Set(TokenSetState, value)
}

// SetError stores structured error information.
func (b *TokenBuilder) SetError(value FlowError) *TokenBuilder {
	return b.Set(TokenSetError, value)
}

// SetMeta adds or replaces a metadata entry. Values are serialized as JSON
// when Build is called and can be decoded with TokenReader.Meta.
func (b *TokenBuilder) SetMeta(key string, value any) *TokenBuilder {
	key = strings.TrimSpace(key)
	if key == "" {
		b.err = fmt.Errorf("token metadata key is empty")
		return b
	}
	if b.meta == nil {
		b.meta = map[string]any{}
	}
	b.meta[key] = value
	if key == tokenPayloadEditableSetMetaKey {
		b.editableSet = nil
	}
	if key == tokenPayloadPrimarySetMetaKey {
		b.primarySet = nil
	}
	if key == TokenMetaType {
		b.tokenType = nil
	}
	return b
}

// PrimarySet records key in admin.primary_set. This selector is distinct from
// admin.editable_set, which is used by EditableValue and MergeEditableValue.
func (b *TokenBuilder) PrimarySet(key string) *TokenBuilder {
	key = strings.TrimSpace(key)
	b.primarySet = &key
	return b
}

// Type records the token type metadata.
func (b *TokenBuilder) Type(tokenType string) *TokenBuilder {
	tokenType = strings.TrimSpace(tokenType)
	b.tokenType = &tokenType
	return b
}

// EditableSet records key in admin.editable_set. EditableValue and
// MergeEditableValue use this selector directly.
func (b *TokenBuilder) EditableSet(key string) *TokenBuilder {
	key = strings.TrimSpace(key)
	b.editableSet = &key
	return b
}

// EditableValue replaces the value of the set selected by EditableSet
// metadata. The selection must exist when Build runs.
func (b *TokenBuilder) EditableValue(value any) *TokenBuilder {
	b.editableValue = value
	b.hasEditableValue = true
	b.hasMergeEditableValue = false
	return b
}

// MergeEditableValue merges object fields into the set selected by
// EditableSet metadata. Incoming fields replace existing fields with the same
// name. The selection and both values must be JSON objects when Build runs.
func (b *TokenBuilder) MergeEditableValue(value any) *TokenBuilder {
	b.mergeEditableValue = value
	b.hasMergeEditableValue = true
	b.hasEditableValue = false
	return b
}

// Build serializes the builder's payload. For builders created with
// NewTokenBuilderFrom, existing entries are preserved and changed entries win.
// EditableValue replaces the selected set; MergeEditableValue shallow-merges
// object fields into it. If both PrimarySet and EditableSet are provided, both
// metadata entries are recorded independently.
func (b *TokenBuilder) Build() (string, error) {
	if b.err != nil {
		return "", b.err
	}
	var raw string
	var err error
	if b.mergeExisting {
		raw, err = MergeTokenPayload(b.raw, b.sets, b.meta)
	} else {
		raw, err = BuildTokenPayload(b.sets, b.meta)
	}
	if err != nil {
		return raw, err
	}
	if b.editableSet != nil {
		raw, err = SetTokenPayloadEditableSetKey(raw, *b.editableSet)
		if err != nil {
			return "", err
		}
	}
	if b.primarySet != nil {
		raw, err = SetTokenPayloadPrimarySetKey(raw, *b.primarySet)
		if err != nil {
			return "", err
		}
	}
	if b.tokenType != nil {
		raw, err = SetTokenPayloadType(raw, *b.tokenType)
		if err != nil {
			return "", err
		}
	}
	if b.hasEditableValue {
		return MarshalTokenPayloadEditableSet(raw, b.editableValue)
	}
	if b.hasMergeEditableValue {
		return MergeTokenPayloadEditableSet(raw, b.mergeEditableValue)
	}
	return raw, nil
}

// TokenReader provides convenient access to a parsed token payload.
type TokenReader struct {
	payload TokenPayload
	err     error
}

// NewTokenReader parses raw once so subsequent reads share the same result.
// Parse failures are available through Err and returned by decoding methods.
func NewTokenReader(raw string) *TokenReader {
	payload, err := ParseTokenPayload(raw)
	return &TokenReader{payload: payload, err: err}
}

// Err returns the error encountered while parsing the payload.
func (r *TokenReader) Err() error { return r.err }

// String returns a set as a trimmed string or its JSON representation.
func (r *TokenReader) String(key string) string {
	if r.err != nil {
		return ""
	}
	return TokenPayloadSetString(r.payload, key)
}

// Meta decodes a metadata value into target. It returns an error when the
// metadata key is absent or its JSON value cannot be decoded.
func (r *TokenReader) Meta(key string, target any) error {
	if r.err != nil {
		return r.err
	}
	key = strings.TrimSpace(key)
	value, ok := r.payload.Meta[key]
	if !ok {
		return fmt.Errorf("token payload has no metadata %q", key)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal token metadata %q: %w", key, err)
	}
	if err := json.Unmarshal(encoded, target); err != nil {
		return fmt.Errorf("decode token metadata %q: %w", key, err)
	}
	return nil
}

// MetaValue returns a metadata value and whether the key exists.
func (r *TokenReader) MetaValue(key string) (any, bool) {
	if r.err != nil {
		return nil, false
	}
	value, ok := r.payload.Meta[strings.TrimSpace(key)]
	return value, ok
}

// PrimarySet returns the set selected by the standard editable-set metadata.
func (r *TokenReader) PrimarySet() string {
	if r.err != nil {
		return ""
	}
	return TokenPayloadPrimarySet(r.payload)
}

// Type returns the token type metadata value.
func (r *TokenReader) Type() string {
	if r.err != nil {
		return ""
	}
	return TokenPayloadType(r.payload)
}

// EditableSet returns the set selected by admin.editable_set metadata.
func (r *TokenReader) EditableSet() string {
	if r.err != nil {
		return ""
	}
	return TokenPayloadEditableSet(r.payload)
}

// Decode decodes a named set into target.
func (r *TokenReader) Decode(key string, target any) error {
	if r.err != nil {
		return r.err
	}
	return decodeTokenPayloadSet(r.payload, key, target)
}

// Result decodes the previous node's output, or the flow's final result, into
// target.
func (r *TokenReader) Result(target any) error {
	return r.Decode(TokenSetResult, target)
}

// Input decodes the value provided to a node by the flow start or previous
// node.
func (r *TokenReader) Input(target any) error {
	return r.Decode(TokenSetInput, target)
}

// State decodes information carried across nodes into target. To make any of
// that information part of the flow's result, the last node must write it with
// SetResult.
func (r *TokenReader) State(target any) error {
	return r.Decode(TokenSetState, target)
}

// Error decodes and returns structured error information.
func (r *TokenReader) Error() (FlowError, error) {
	var result FlowError
	if err := r.Decode(TokenSetError, &result); err != nil {
		return FlowError{}, err
	}
	return result, nil
}

// DecodeMetaSet resolves a set key from metadata and decodes that set into
// target.
func (r *TokenReader) DecodeMetaSet(metaKey string, target any) error {
	if r.err != nil {
		return r.err
	}
	return decodeTokenPayloadMetaSet(r.payload, metaKey, target)
}
