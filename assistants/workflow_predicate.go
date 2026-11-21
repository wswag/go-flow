package assistants

import (
	"context"
	"errors"
	"fmt"

	"github.com/wswag/go-flow/workflows"
)

// WorkflowInstanceReader reads a persisted workflow instance. A
// *workflows.Repository satisfies this interface.
type WorkflowInstanceReader interface {
	FindFlowInstanceByID(ctx context.Context, id uint) (workflows.FlowInstance, error)
}

// AllWorkflowsCompleted returns an auto-commit predicate that is true when
// every referenced workflow has finished or failed. An empty reference list
// does not trigger a commit; a zero reference or a read error is returned as an
// error. Pending, running, waiting and stopped workflows are not complete.
func AllWorkflowsCompleted(reader WorkflowInstanceReader) AutoCommitPredicate {
	return func(ctx context.Context, prepare PrepareContext) (bool, error) {
		if len(prepare.WorkflowIDs) == 0 {
			return false, nil
		}
		if isNilAssistantDependency(reader) {
			return false, errors.New("workflow instance reader is not configured")
		}
		for _, id := range prepare.WorkflowIDs {
			if id == 0 {
				return false, errors.New("workflow reference is required")
			}
			instance, err := reader.FindFlowInstanceByID(ctx, id)
			if err != nil {
				return false, fmt.Errorf("read workflow %d: %w", id, err)
			}
			if instance.Status != workflows.FlowStatusFinished && instance.Status != workflows.FlowStatusFailed {
				return false, nil
			}
		}
		return true, nil
	}
}
