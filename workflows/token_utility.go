package workflows

import "github.com/wswag/go-flow/tokens"

const (
	// TokenMetaFlowName is the metadata key for a workflow name.
	TokenMetaFlowName = "flow.name"
	// TokenMetaFlowNode is the metadata key for a workflow node name.
	TokenMetaFlowNode = "flow.node"
	// TokenMetaFlowStateSet is both the metadata key that identifies the
	// workflow state set and the set name used to store that state.
	TokenMetaFlowStateSet = "flow.state_set"
	// FlowTokenTypeNode is the metadata value for a workflow node token.
	FlowTokenTypeNode = "workflow_node"
)

// NewFlowTokenBuilder creates a workflow node token builder with workflow
// name, node, token-type, and state-set metadata. When state is non-nil, it is
// added under TokenMetaFlowStateSet. Call Build on the returned builder to
// serialize it.
func NewFlowTokenBuilder(flowName, nodeName string, state any) *tokens.TokenBuilder {
	builder := tokens.NewTokenBuilder().
		Meta(TokenMetaFlowName, flowName).
		Meta(TokenMetaFlowNode, nodeName).
		Meta(TokenMetaFlowStateSet, TokenMetaFlowStateSet).
		Type(FlowTokenTypeNode)
	if state != nil {
		builder.Set(TokenMetaFlowStateSet, state)
	}
	return builder
}
