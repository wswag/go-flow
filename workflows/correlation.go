package workflows

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
)

const (
	metadataCorrelationIDKey          = "correlationId"
	metadataCorrelationLabelKey       = "correlationLabel"
	metadataCorrelationDescriptionKey = "correlationDescription"
	metadataParentFlowInstanceIDKey   = "parentFlowInstanceId"
	metadataParentNodeIDKey           = "parentNodeId"
	metadataParentNodeIndexKey        = "parentNodeIndex"
)

// EnsureCorrelationMetadata fills missing correlation fields for a workflow.
func EnsureCorrelationMetadata(metadata map[string]any, label string, description string) map[string]any {
	out := cloneMetadataMap(metadata)
	if strings.TrimSpace(metadataString(out[metadataCorrelationIDKey])) == "" {
		out[metadataCorrelationIDKey] = newCorrelationID()
	}
	if strings.TrimSpace(metadataString(out[metadataCorrelationLabelKey])) == "" {
		out[metadataCorrelationLabelKey] = strings.TrimSpace(label)
	}
	if strings.TrimSpace(metadataString(out[metadataCorrelationDescriptionKey])) == "" {
		out[metadataCorrelationDescriptionKey] = strings.TrimSpace(description)
	}
	return out
}

// InheritCorrelationMetadata carries correlation metadata into a child flow
// and records the parent flow and node references.
func InheritCorrelationMetadata(metadata map[string]any, parentFlowInstanceID uint, parentNodeID uint, parentNodeIndex int) map[string]any {
	out := cloneMetadataMap(metadata)
	if parentFlowInstanceID > 0 {
		out[metadataParentFlowInstanceIDKey] = parentFlowInstanceID
	}
	if parentNodeID > 0 {
		out[metadataParentNodeIDKey] = parentNodeID
	}
	out[metadataParentNodeIndexKey] = parentNodeIndex
	return out
}

// cloneMetadataMap copies metadata so callers can update it independently.
func cloneMetadataMap(metadata map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range metadata {
		out[key] = value
	}
	return out
}

// metadataString returns a trimmed string value or an empty string for other
// metadata types.
func metadataString(value any) string {
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text)
	}
	return ""
}

// newCorrelationID generates a random identifier for a workflow correlation.
func newCorrelationID() string {
	var raw [8]byte
	// A failed system random read cannot safely produce a correlation ID.
	if _, err := rand.Read(raw[:]); err != nil {
		return ""
	}
	return hex.EncodeToString(raw[:])
}
