package workflows

import "context"

type flowMetadataContextKey struct{}

// WithFlowMetadata returns a context carrying workflow metadata. The flow
// runtime propagates this metadata to child flows started from the context.
func WithFlowMetadata(ctx context.Context, metadata map[string]any) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	merged := FlowMetadataFromContext(ctx)
	for key, value := range metadata {
		merged[key] = value
	}
	return context.WithValue(ctx, flowMetadataContextKey{}, merged)
}

// FlowMetadataFromContext returns a copy of metadata carried by ctx, or an
// empty map when no workflow metadata is present.
func FlowMetadataFromContext(ctx context.Context) map[string]any {
	if ctx == nil {
		return map[string]any{}
	}
	metadata, _ := ctx.Value(flowMetadataContextKey{}).(map[string]any)
	copy := make(map[string]any, len(metadata))
	for key, value := range metadata {
		copy[key] = value
	}
	return copy
}
