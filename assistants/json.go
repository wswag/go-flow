package assistants

import (
	"encoding/json"
	"fmt"
	"strings"
)

func decodeAssistantJSON(field, stepName string, raw json.RawMessage, target any) error {
	if err := json.Unmarshal(normalizeJSON(raw), target); err != nil {
		if stepName == "" {
			return fmt.Errorf("decode assistant %s: %w", field, err)
		}
		return fmt.Errorf("decode assistant step %q %s: %w", stepName, field, err)
	}
	return nil
}

// DecodeCurrentInputPatch decodes the persisted input patch for the step whose
// Prepare callback is running. The patch contains only persisted changes; it
// does not include the step's defaults or prepared input. An empty raw value is
// decoded as an empty JSON object.
func (c PrepareContext) DecodeCurrentInputPatch(target any) error {
	return decodeAssistantJSON("current input patch", c.Current, c.CurrentInputPatch, target)
}

// DecodeCurrentResult decodes the last persisted commit result for the step
// whose Prepare callback is running. It is useful when preparation depends on
// a result produced earlier for this same step. An empty raw value is decoded
// as an empty JSON object.
func (c PrepareContext) DecodeCurrentResult(target any) error {
	return decodeAssistantJSON("current result", c.Current, c.CurrentResult, target)
}

// DecodeStepResult decodes a persisted result from a previous step listed in
// PrepareContext.Steps. Prepare callbacks receive only earlier steps there, so
// this method cannot read the current step or a later step. An empty result is
// decoded as an empty JSON object.
func (c PrepareContext) DecodeStepResult(stepName string, target any) error {
	step, ok := c.Step(stepName)
	if !ok {
		return fmt.Errorf("assistant previous step %q is not available", strings.TrimSpace(stepName))
	}
	return step.DecodeResult(target)
}

// DecodeInput decodes the step's effective input: defaults, prepared input, and
// persisted input patch merged in that order. Use it when a callback needs the
// same input presented by the step snapshot.
func (s StepSnapshot) DecodeInput(target any) error {
	return decodeAssistantJSON("input", s.Name, s.Input, target)
}

// DecodePreparedInput decodes only the value returned by the step's Prepare
// callback. It does not include defaults or the persisted input patch. If the
// step has no Prepare callback, the value is an empty JSON object.
func (s StepSnapshot) DecodePreparedInput(target any) error {
	return decodeAssistantJSON("prepared input", s.Name, s.PreparedInput, target)
}

// DecodeInputPatch decodes only the persisted changes supplied for the step.
// Defaults and prepared input are not included. An empty patch is decoded as
// an empty JSON object.
func (s StepSnapshot) DecodeInputPatch(target any) error {
	return decodeAssistantJSON("input patch", s.Name, s.InputPatch, target)
}

// DecodeResult decodes the result most recently persisted by the step's
// CommitOutcome. It does not decode the step input or the assistant's full
// state. An empty result is decoded as an empty JSON object.
func (s StepSnapshot) DecodeResult(target any) error {
	return decodeAssistantJSON("result", s.Name, s.Result, target)
}

// DecodeInput decodes the effective input for the step being committed. It
// combines defaults, prepared input, and persisted input patches; transient
// input supplied to CommitStepWithInput is included for that commit as well.
func (c CommitContext) DecodeInput(target any) error {
	return c.Step.DecodeInput(target)
}

// DecodePreparedInput decodes only the value returned by Prepare for the step
// being committed, without defaults or persisted patches.
func (c CommitContext) DecodePreparedInput(target any) error {
	return c.Step.DecodePreparedInput(target)
}

// DecodeInputPatch decodes the persisted changes for the step being committed,
// without defaults or prepared input. Transient input is not included.
func (c CommitContext) DecodeInputPatch(target any) error {
	return c.Step.DecodeInputPatch(target)
}

// DecodeResult decodes the result previously persisted for the step being
// committed. It does not decode a result the current Commit callback is about
// to return; that value belongs in CommitOutcome.Result.
func (c CommitContext) DecodeResult(target any) error {
	return c.Step.DecodeResult(target)
}

// DecodeStepResult decodes the persisted result of a step preceding the one
// being committed. It searches only earlier entries in CommitContext.Steps and
// returns an error when the requested step is not available there. An empty
// result is decoded as an empty JSON object.
func (c CommitContext) DecodeStepResult(stepName string, target any) error {
	currentIndex := -1
	for index, step := range c.Steps {
		if step.Name == c.Step.Name {
			currentIndex = index
			break
		}
	}
	if currentIndex < 0 {
		return fmt.Errorf("assistant current step %q is not available", c.Step.Name)
	}
	stepName = strings.TrimSpace(stepName)
	for index := 0; index < currentIndex; index++ {
		if c.Steps[index].Name == stepName {
			return c.Steps[index].DecodeResult(target)
		}
	}
	return fmt.Errorf("assistant previous step %q is not available", stepName)
}
