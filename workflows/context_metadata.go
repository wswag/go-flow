package workflows

import "context"

type flowMetadataContextKey struct{}

// withFlowMetadata returns a context carrying workflow metadata. Runtime
// metadata is loaded from persistence for each node; values added here are
// local to this context and are not persisted or propagated to child flows.
func withFlowMetadata(ctx context.Context, metadata map[string]any) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	merged := FlowMetadataFromContext(ctx)
	for key, value := range metadata {
		merged[key] = value
	}
	return context.WithValue(ctx, flowMetadataContextKey{}, merged)
}

// FlowMetadataFromContext returns a copy of the persisted flow metadata
// available to the current node through ctx, or an empty map when none is
// present. Changes to the returned map do not affect the flow or later nodes.
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
