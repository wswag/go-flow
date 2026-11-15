package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/wswag/go-flow/tokens"
)

// DefaultWorkflowWaitTimeout begrenzt einen einzelnen Long-Poll-Aufruf. Ein
// Client startet nach einer Timeout-Antwort bei weiter aktivem Flow einfach
// einen neuen Aufruf.
const DefaultWorkflowWaitTimeout = 15 * time.Second

// WorkflowWaitOptions konfiguriert das Beobachten genau eines Workflow-Laufs.
type WorkflowWaitOptions struct {
	Timeout time.Duration
}

// WorkflowWaitResult ist der fachfreie Snapshot eines beobachteten Workflows.
// ResultSets stammen nur von einem nicht mehr aktiven Lauf; während eines
// Timeouts bleiben sie bewusst leer, damit Zwischenstände nicht als Ergebnis
// missverstanden werden.
type WorkflowWaitResult struct {
	Status       FlowStatus                 `json:"status"`
	ErrorMessage string                     `json:"errorMessage,omitempty"`
	Metadata     map[string]any             `json:"metadata"`
	ResultSets   map[string]json.RawMessage `json:"resultSets"`
}

// AwaitWorkflow waits until a workflow instance reaches a terminal state. It
// returns the raw JSON value stored in the result set, or the structured flow
// error when the instance failed. Context cancellation stops the wait.
func (s *Service) AwaitWorkflow(ctx context.Context, ref WorkflowRef) (json.RawMessage, error) {
	if ref == 0 {
		return nil, errors.New("workflow reference is required")
	}
	for {
		result, err := s.WaitForWorkflow(ctx, uint(ref), WorkflowWaitOptions{Timeout: DefaultWorkflowWaitTimeout})
		if err != nil {
			return nil, err
		}
		switch result.Status {
		case FlowStatusFinished:
			raw, ok := result.ResultSets[tokens.TokenSetResult]
			if !ok {
				return nil, errors.New("workflow result is missing")
			}
			return append(json.RawMessage(nil), raw...), nil
		case FlowStatusFailed:
			flowError, err := result.Error()
			if err == nil {
				return nil, flowError
			}
			if strings.TrimSpace(result.ErrorMessage) != "" {
				return nil, errors.New(strings.TrimSpace(result.ErrorMessage))
			}
			return nil, errors.New("workflow failed")
		default:
			// Stopped is a persisted checkpoint between two nodes. Keep waiting
			// until the execution resumes or the caller cancels the context.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(25 * time.Millisecond):
			}
		}
	}
}

// Result decodes the result set into target. The final node must write
// the flow result with TokenBuilder.SetResult for it to be available here.
func (r WorkflowWaitResult) Result(target any) error {
	raw, ok := r.ResultSets[tokens.TokenSetResult]
	if !ok {
		return errors.New("workflow result is missing")
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("decode workflow result: %w", err)
	}
	return nil
}

// Error decodes and returns the structured error set.
func (r WorkflowWaitResult) Error() (tokens.FlowError, error) {
	raw, ok := r.ResultSets[tokens.TokenSetError]
	if !ok {
		return tokens.FlowError{}, errors.New("workflow error details are missing")
	}
	var result tokens.FlowError
	if err := json.Unmarshal(raw, &result); err != nil {
		return tokens.FlowError{}, fmt.Errorf("decode workflow error details: %w", err)
	}
	return result, nil
}

// WaitForWorkflow wartet bis ein einzelner Lauf nicht mehr aktiv ist oder das
// Timeout erreicht. Running und Waiting sind aktiv, weil ein wartender Parent
// weiterhin auf seine Child-Flows wartet.
func (s *Service) WaitForWorkflow(ctx context.Context, workflowID uint, options WorkflowWaitOptions) (WorkflowWaitResult, error) {
	if s == nil || s.store == nil {
		return WorkflowWaitResult{}, errors.New("flow store is not configured")
	}
	if workflowID == 0 {
		return WorkflowWaitResult{}, errors.New("workflow instance is required")
	}
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = DefaultWorkflowWaitTimeout
	}

	initial, err := s.workflowWaitSnapshot(ctx, workflowID, false)
	if err != nil || !workflowWaitActive(initial.Status) {
		return initial, err
	}

	events, unsubscribe := s.Subscribe()
	defer unsubscribe()
	// Das zweite Lesen schließt die Lücke zwischen erstem Snapshot und
	// Subscription: Ein terminales Event darf nicht bis zum Timeout verloren
	// gehen.
	if current, snapshotErr := s.workflowWaitSnapshot(ctx, workflowID, false); snapshotErr != nil || !workflowWaitActive(current.Status) {
		return current, snapshotErr
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return WorkflowWaitResult{}, ctx.Err()
		case <-timer.C:
			return s.workflowWaitSnapshot(ctx, workflowID, true)
		case event, open := <-events:
			if !open {
				return s.workflowWaitSnapshot(ctx, workflowID, true)
			}
			if event.FlowInstanceID != workflowID {
				continue
			}
			current, snapshotErr := s.workflowWaitSnapshot(ctx, workflowID, false)
			if snapshotErr != nil {
				return WorkflowWaitResult{}, snapshotErr
			}
			if !workflowWaitActive(current.Status) {
				return current, nil
			}
		}
	}
}

// workflowWaitActive reports whether the workflow can still produce a result.
func workflowWaitActive(status FlowStatus) bool {
	return status == FlowStatusRunning || status == FlowStatusWaiting
}

// workflowWaitSnapshot loads a workflow's current status, metadata, and
// completed result sets. Active results can be suppressed for timeout replies.
func (s *Service) workflowWaitSnapshot(ctx context.Context, workflowID uint, suppressActiveSets bool) (WorkflowWaitResult, error) {
	instance, err := s.store.FindFlowInstanceByID(ctx, workflowID)
	if err != nil {
		return WorkflowWaitResult{}, err
	}
	result := WorkflowWaitResult{
		Status:       instance.Status,
		ErrorMessage: instance.ErrorMessage,
		Metadata:     cloneWorkflowWaitMetadata(parseFlowMetadata(instance.MetadataJSON).Raw),
		ResultSets:   map[string]json.RawMessage{},
	}
	if suppressActiveSets && workflowWaitActive(instance.Status) {
		return result, nil
	}
	if instance.OutputToken == nil || strings.TrimSpace(instance.OutputToken.RawData) == "" {
		return result, nil
	}
	payload, err := tokens.ParseTokenPayload(instance.OutputToken.RawData)
	if err != nil {
		return WorkflowWaitResult{}, err
	}
	// Copy raw JSON so the result does not share mutable backing arrays.
	for key, value := range payload.Sets {
		result.ResultSets[key] = append(json.RawMessage(nil), value...)
	}
	return result, nil
}

// cloneWorkflowWaitMetadata returns a shallow copy of workflow metadata.
func cloneWorkflowWaitMetadata(source map[string]any) map[string]any {
	result := map[string]any{}
	for key, value := range source {
		result[key] = value
	}
	return result
}
