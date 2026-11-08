package assistants

import "github.com/wswag/go-flow/tokens"

const (
	// TokenMetaAssistantName is the metadata key for an assistant chain name.
	TokenMetaAssistantName = "assistant.name"
	// TokenMetaAssistantStep is the metadata key for the assistant step name.
	TokenMetaAssistantStep = "assistant.step"
	// TokenMetaAssistantStateSet is both the metadata key that identifies the
	// assistant state set and the set name used to store that state.
	TokenMetaAssistantStateSet = "assistant.state_set"
	// AssistantTokenTypeStep is the token type for an assistant step token.
	AssistantTokenTypeStep = "assistant_step"
)

// NewAssistantTokenBuilder creates an assistant step token builder with
// assistant name, step, token type, and state-set metadata. When state is
// non-nil, it is added under TokenMetaAssistantStateSet. Call Build on the
// returned builder to serialize the token.
func NewAssistantTokenBuilder(assistantName, stepName string, state any) *tokens.TokenBuilder {
	builder := tokens.NewTokenBuilder().
		Meta(TokenMetaAssistantName, assistantName).
		Meta(TokenMetaAssistantStep, stepName).
		Meta(TokenMetaAssistantStateSet, TokenMetaAssistantStateSet).
		Type(AssistantTokenTypeStep)
	if state != nil {
		builder.Set(TokenMetaAssistantStateSet, state)
	}
	return builder
}
