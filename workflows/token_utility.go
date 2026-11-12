package workflows

import "github.com/wswag/go-flow/tokens"

const (
	// TokenMetaFlowName is the metadata key for a workflow name.
	TokenMetaFlowName = "flow.name"
	// TokenMetaFlowNode is the metadata key for a workflow node name.
	TokenMetaFlowNode = "flow.node"
	// FlowTokenTypeNode is the metadata value for a workflow node token.
	FlowTokenTypeNode = "workflow_node"
)

// NewFlowTokenBuilder creates a workflow node token builder with workflow
// name, node, and token-type metadata. When state is non-nil, it is added to
// the state set. Call Build on the returned builder to
// serialize it.
func NewFlowTokenBuilder(flowName, nodeName string, state any) *tokens.TokenBuilder {
	builder := tokens.NewTokenBuilder().
		Meta(TokenMetaFlowName, flowName).
		Meta(TokenMetaFlowNode, nodeName).
		Type(FlowTokenTypeNode)
	if state != nil {
		builder.SetState(state)
	}
	return builder
}
