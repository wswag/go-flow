package workflows

import (
	"context"
	"encoding/json"
	"errors"
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
	Status     FlowStatus                 `json:"status"`
	Metadata   map[string]any             `json:"metadata"`
	ResultSets map[string]json.RawMessage `json:"resultSets"`
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
		Status:     instance.Status,
		Metadata:   cloneWorkflowWaitMetadata(parseFlowMetadata(instance.MetadataJSON).Raw),
		ResultSets: map[string]json.RawMessage{},
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
