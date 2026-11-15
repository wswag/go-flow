package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wswag/go-flow/tokens"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// TestServiceResumeStoppedFlowsContinuesPersistedCheckpoint verifies restart
// recovery resumes at the persisted node rather than rerunning completed work.
func TestServiceResumeStoppedFlowsContinuesPersistedCheckpoint(t *testing.T) {
	db := openFlowTestDB(t)
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}

	repo := NewRepository(db)
	svc := NewService(repo)
	flowDef := Flow{
		Name: "rezeptplanung",
		Nodes: []FlowNode{
			{
				Name: "analyse",
				Action: func(ctx context.Context, rawInput string, runtime Runtime) (string, error) {
					return rawInput + "-analyse", nil
				},
			},
			{
				Name: "bild",
				Action: func(ctx context.Context, rawInput string, runtime Runtime) (string, error) {
					return rawInput + "-bild", nil
				},
			},
		},
	}
	if err := svc.RegisterFlow(flowDef); err != nil {
		t.Fatalf("register flow: %v", err)
	}

	flowInstanceID := seedStoppedFlowInstance(t, repo, flowDef.Name)
	resumed, err := svc.ResumeStoppedFlows(context.Background())
	if err != nil {
		t.Fatalf("resume stopped flows: %v", err)
	}
	if len(resumed) != 1 {
		t.Fatalf("expected 1 resumed flow, got %d", len(resumed))
	}

	exec := resumed[0]
	waitForDone(t, exec.Done())
	if exec.Status != FlowStatusFinished {
		t.Fatalf("expected finished execution, got %s", exec.Status)
	}
	if exec.EndToken != "start-token-analyse-bild" {
		t.Fatalf("unexpected end token: %q", exec.EndToken)
	}

	persisted, err := repo.FindFlowInstanceByID(context.Background(), flowInstanceID)
	if err != nil {
		t.Fatalf("load persisted flow: %v", err)
	}
	if persisted.Status != FlowStatusFinished {
		t.Fatalf("expected persisted flow to be finished, got %s", persisted.Status)
	}
	if persisted.OutputToken == nil || persisted.OutputToken.RawData != "start-token-analyse-bild" {
		t.Fatalf("unexpected persisted output token: %#v", persisted.OutputToken)
	}
	if len(persisted.NodeInstances) != 2 {
		t.Fatalf("expected 2 node instances, got %d", len(persisted.NodeInstances))
	}
	if persisted.NodeInstances[1].Status != NodeStatusFinished {
		t.Fatalf("expected second node to finish, got %s", persisted.NodeInstances[1].Status)
	}
}

// TestRepositoryScrubExpiredTokenDataClearsOldInactivePayloads verifies old
// inactive token contents are scrubbed while their records remain available.
func TestRepositoryScrubExpiredTokenDataClearsOldInactivePayloads(t *testing.T) {
	db := openFlowTestDB(t)
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}

	repo := NewRepository(db)
	ctx := context.Background()
	now := time.Now().UTC()
	oldCreatedAt := now.AddDate(0, 0, -35)
	recentCreatedAt := now.AddDate(0, 0, -7)

	oldToken := Token{RawData: "altes-token-payload", CreatedAt: oldCreatedAt}
	activeToken := Token{RawData: "aktives-token-payload", CreatedAt: oldCreatedAt}
	recentToken := Token{RawData: "frisches-token-payload", CreatedAt: recentCreatedAt}
	for _, item := range []*Token{&oldToken, &activeToken, &recentToken} {
		if err := repo.CreateToken(ctx, item); err != nil {
			t.Fatalf("create token: %v", err)
		}
	}

	finishedAt := oldCreatedAt.Add(2 * time.Hour)
	finishedFlow := &FlowInstance{
		Name:         "abgeschlossener-flow",
		Status:       FlowStatusFinished,
		InputTokenID: &oldToken.ID,
		StartedAt:    oldCreatedAt,
		UpdatedAt:    finishedAt,
		FinishedAt:   &finishedAt,
	}
	if err := repo.CreateFlowInstance(ctx, finishedFlow); err != nil {
		t.Fatalf("create finished flow: %v", err)
	}
	if err := repo.CreateNodeInstance(ctx, &NodeInstance{
		Name:           "warte-weiter",
		FlowInstanceID: finishedFlow.ID,
		NodeIndex:      0,
		Status:         NodeStatusQueued,
		InputTokenID:   &activeToken.ID,
		StartedAt:      oldCreatedAt,
		UpdatedAt:      oldCreatedAt,
		Active:         true,
	}); err != nil {
		t.Fatalf("create active node: %v", err)
	}

	rows, err := repo.ScrubExpiredTokenData(ctx, now.AddDate(0, 0, -28))
	if err != nil {
		t.Fatalf("scrub expired token data: %v", err)
	}
	if rows != 1 {
		t.Fatalf("expected exactly one scrubbed token, got %d", rows)
	}

	persistedOld, err := repo.FindTokenByID(ctx, oldToken.ID)
	if err != nil {
		t.Fatalf("load old token: %v", err)
	}
	if persistedOld.RawData != "" {
		t.Fatalf("expected old token payload to be scrubbed, got %q", persistedOld.RawData)
	}
	persistedActive, err := repo.FindTokenByID(ctx, activeToken.ID)
	if err != nil {
		t.Fatalf("load active token: %v", err)
	}
	if persistedActive.RawData != "aktives-token-payload" {
		t.Fatalf("expected active token payload to stay intact, got %q", persistedActive.RawData)
	}
	persistedRecent, err := repo.FindTokenByID(ctx, recentToken.ID)
	if err != nil {
		t.Fatalf("load recent token: %v", err)
	}
	if persistedRecent.RawData != "frisches-token-payload" {
		t.Fatalf("expected recent token payload to stay intact, got %q", persistedRecent.RawData)
	}
}

// TestServiceStartFlowReusesActiveIdempotentFlow verifies matching
// active starts share one persisted execution.
func TestServiceStartFlowReusesActiveIdempotentFlow(t *testing.T) {
	db := openFlowTestDB(t)
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}

	repo := NewRepository(db)
	svc := NewService(repo)
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	if err := svc.RegisterFlow(Flow{
		Name: "idempotent-flow",
		Nodes: []FlowNode{
			{
				Name: "wait",
				Action: func(ctx context.Context, rawInput string, runtime Runtime) (string, error) {
					started <- struct{}{}
					<-release
					return rawInput + "-done", nil
				},
			},
		},
	}); err != nil {
		t.Fatalf("register flow: %v", err)
	}

	first, err := svc.startFlowExecution(context.Background(), "idempotent-flow", "start-token", StartOptions{
		IdempotencyKey: "scope:7:image:abc",
	})
	if err != nil {
		t.Fatalf("start first flow: %v", err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("expected first flow to start")
	}

	second, err := svc.startFlowExecution(context.Background(), "idempotent-flow", "different-token", StartOptions{
		IdempotencyKey: "scope:7:image:abc",
	})
	if err != nil {
		t.Fatalf("start second flow: %v", err)
	}
	if first.FlowInstanceID == 0 || second.FlowInstanceID == 0 {
		t.Fatal("expected persisted flow instance ids")
	}
	if first.FlowInstanceID != second.FlowInstanceID {
		t.Fatalf("expected second start to reuse flow %d, got %d", first.FlowInstanceID, second.FlowInstanceID)
	}
	if !second.Reused {
		t.Fatal("expected reused execution marker on second start")
	}

	close(release)
	waitForDone(t, first.Done())
	if first.Status != FlowStatusFinished {
		t.Fatalf("expected first execution to finish, got %s", first.Status)
	}

	var count int64
	if err := repo.db.WithContext(context.Background()).Model(&FlowInstance{}).
		Where("name = ? AND idempotency_key = ?", "idempotent-flow", "scope:7:image:abc").
		Count(&count).Error; err != nil {
		t.Fatalf("count flows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly one persisted idempotent flow, got %d", count)
	}
}

// TestServiceStartFlowDoesNotReuseFailedIdempotentFlow verifies a
// failed run does not block a fresh request with the same idempotency key.
func TestServiceStartFlowDoesNotReuseFailedIdempotentFlow(t *testing.T) {
	db := openFlowTestDB(t)
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}
	repo := NewRepository(db)
	svc := NewService(repo)
	svc.RegisterStartMiddleware(svc.IdempotencyMiddleware())
	if err := svc.RegisterFlow(Flow{
		Name: "failed-idempotent-flow",
		Nodes: []FlowNode{{Name: "fail", Action: func(context.Context, string, Runtime) (string, error) {
			return "", errors.New("expected retry failure")
		}}},
	}); err != nil {
		t.Fatalf("register flow: %v", err)
	}
	first, err := svc.startFlowExecution(context.Background(), "failed-idempotent-flow", "first", StartOptions{IdempotencyKey: "retry-key"})
	if err != nil {
		t.Fatalf("start first flow: %v", err)
	}
	waitForDone(t, first.Done())
	if first.Status != FlowStatusFailed {
		t.Fatalf("first flow status = %s, want failed", first.Status)
	}
	second, err := svc.startFlowExecution(context.Background(), "failed-idempotent-flow", "second", StartOptions{IdempotencyKey: "retry-key"})
	if err != nil {
		t.Fatalf("start second flow: %v", err)
	}
	waitForDone(t, second.Done())
	if second.FlowInstanceID == first.FlowInstanceID || second.Reused {
		t.Fatalf("failed flow was reused: first=%d second=%d reused=%t", first.FlowInstanceID, second.FlowInstanceID, second.Reused)
	}
}

// TestServiceRetryFlowStartsFreshRunAndSupersedesFailure verifies
// retry creates a new run and hides the superseded failure from latest lookups.
func TestServiceRetryFlowStartsFreshRunAndSupersedesFailure(t *testing.T) {
	db := openFlowTestDB(t)
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}
	repo := NewRepository(db)
	svc := NewService(repo)
	var attempts atomic.Int32
	if err := svc.RegisterFlow(Flow{
		Name:           "fresh-retry-flow",
		RetryNodeNames: []string{"run"},
		Nodes: []FlowNode{{Name: "run", Action: func(_ context.Context, rawInput string, _ Runtime) (string, error) {
			if attempts.Add(1) == 1 {
				return "", errors.New("first attempt failed")
			}
			return rawInput + "-ok", nil
		}}},
	}); err != nil {
		t.Fatalf("register flow: %v", err)
	}
	first, err := svc.startFlowExecution(context.Background(), "fresh-retry-flow", testEditablePayload(t, "original"))
	if err != nil {
		t.Fatalf("start first flow: %v", err)
	}
	waitForDone(t, first.Done())
	failed, err := repo.FindFlowInstanceByID(context.Background(), first.FlowInstanceID)
	if err != nil {
		t.Fatalf("load failed flow: %v", err)
	}
	if failed.Status != FlowStatusFailed || len(failed.NodeInstances) != 1 {
		t.Fatalf("failed flow state = status %s nodes %d", failed.Status, len(failed.NodeInstances))
	}
	retried, err := svc.RetryFlow(context.Background(), FlowRef(failed.ID), failed.NodeInstances[0].ID, "", "edited")
	if err != nil {
		t.Fatalf("retry flow: %v", err)
	}
	waitForFlowInstanceByNameAndStatus(t, repo, failed.Name, FlowStatusFinished)
	retriedInstance, err := repo.FindFlowInstanceByID(context.Background(), uint(retried))
	if err != nil {
		t.Fatalf("load retried flow: %v", err)
	}
	if retriedInstance.ID == failed.ID {
		t.Fatalf("retry reused failed flow %d", failed.ID)
	}
	if retriedInstance.Status != FlowStatusFinished {
		t.Fatalf("retried flow status = %s, want finished", retriedInstance.Status)
	}
	updatedFailed, err := repo.FindFlowInstanceByID(context.Background(), failed.ID)
	if err != nil {
		t.Fatalf("reload failed flow: %v", err)
	}
	if updatedFailed.DiscardedAt == nil {
		t.Fatal("superseded failed flow is still visible")
	}
	payload, err := tokens.ParseTokenPayload(retriedInstance.InputToken.RawData)
	if err != nil || tokens.TokenPayloadSetString(payload, "value") != "edited" {
		t.Fatalf("retried start token = %q, want edited value (error: %v)", retriedInstance.InputToken.RawData, err)
	}
}

// TestServiceRetryFlowReusesPersistedCheckpointWithoutRerunningPreviousNodes
// verifies retry resumes from stored checkpoint input without repeating earlier actions.
func TestServiceRetryFlowReusesPersistedCheckpointWithoutRerunningPreviousNodes(t *testing.T) {
	db := openFlowTestDB(t)
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}
	repo := NewRepository(db)
	svc := NewService(repo)
	var generationAttempts atomic.Int32
	var checkpointAttempts atomic.Int32
	generated := testEditablePayload(t, "persisted-generation")
	if err := svc.RegisterFlow(Flow{
		Name:           "checkpoint-retry-flow",
		RetryNodeNames: []string{"apply"},
		Nodes: []FlowNode{
			{Name: "generate", Action: func(_ context.Context, _ string, _ Runtime) (string, error) {
				generationAttempts.Add(1)
				return generated, nil
			}},
			{Name: "apply", Action: func(_ context.Context, rawInput string, _ Runtime) (string, error) {
				if checkpointAttempts.Add(1) == 1 {
					return "", errors.New("checkpoint failed")
				}
				return rawInput + "-applied", nil
			}},
		},
	}); err != nil {
		t.Fatalf("register flow: %v", err)
	}
	first, err := svc.startFlowExecution(context.Background(), "checkpoint-retry-flow", "request")
	if err != nil {
		t.Fatalf("start first flow: %v", err)
	}
	waitForDone(t, first.Done())
	failed, err := repo.FindFlowInstanceByID(context.Background(), first.FlowInstanceID)
	if err != nil {
		t.Fatalf("load failed flow: %v", err)
	}
	if failed.Status != FlowStatusFailed || failed.CurrentNodeIndex != 1 {
		t.Fatalf("failed flow state = status %s node %d", failed.Status, failed.CurrentNodeIndex)
	}
	checkpoint := findNodeInstanceByIndex(failed.NodeInstances, 1)
	if checkpoint.ID == 0 {
		t.Fatal("retry checkpoint node missing")
	}
	retried, err := svc.RetryFlow(context.Background(), FlowRef(failed.ID), checkpoint.ID, "", "ignored-downstream-edit")
	if err != nil {
		t.Fatalf("retry flow: %v", err)
	}
	waitForFlowInstanceByNameAndStatus(t, repo, failed.Name, FlowStatusFinished)
	retriedInstance, err := repo.FindFlowInstanceByID(context.Background(), uint(retried))
	if err != nil {
		t.Fatalf("load retried flow: %v", err)
	}
	if retriedInstance.Status != FlowStatusFinished {
		t.Fatalf("retried flow status = %s, want finished", retriedInstance.Status)
	}
	payload, err := tokens.ParseTokenPayload(retriedInstance.InputToken.RawData)
	if err != nil || tokens.TokenPayloadSetString(payload, "value") != "ignored-downstream-edit" {
		t.Fatalf("retried checkpoint token = %q, want edited value (error: %v)", retriedInstance.InputToken.RawData, err)
	}
	if generationAttempts.Load() != 1 {
		t.Fatalf("generation attempts = %d, want one persisted generation", generationAttempts.Load())
	}
	if checkpointAttempts.Load() != 2 {
		t.Fatalf("checkpoint attempts = %d, want two checkpoint attempts", checkpointAttempts.Load())
	}
}

// TestNearestRetryNodeIndexUsesClosestDeclaredCheckpoint verifies checkpoint
// selection chooses the nearest eligible node.
func TestNearestRetryNodeIndexUsesClosestDeclaredCheckpoint(t *testing.T) {
	definition := Flow{
		Name:           "nearest-checkpoint-flow",
		RetryNodeNames: []string{"prepare", "apply"},
		Nodes: []FlowNode{
			{Name: "generate"},
			{Name: "prepare"},
			{Name: "apply"},
			{Name: "follow-up"},
		},
	}

	if got, ok := nearestRetryNodeIndex(definition, 3); !ok || got != 2 {
		t.Fatalf("nearest retry node for index 3 = (%d, %t), want (2, true)", got, ok)
	}
	if got, ok := nearestRetryNodeIndex(definition, 1); !ok || got != 1 {
		t.Fatalf("nearest retry node for index 1 = (%d, %t), want (1, true)", got, ok)
	}
	if _, ok := nearestRetryNodeIndex(definition, 0); ok {
		t.Fatal("expected no retry node before the first declared checkpoint")
	}
}

// TestRepositoryTreatsWaitingFlowAsResumableAndActive verifies waiting parents
// remain active while child flows are pending.
func TestRepositoryTreatsWaitingFlowAsResumableAndActive(t *testing.T) {
	db := openFlowTestDB(t)
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}

	repo := NewRepository(db)
	now := time.Now().UTC()
	instance := &FlowInstance{
		Name:           "waiting-flow",
		Status:         FlowStatusWaiting,
		IdempotencyKey: "scope:7",
		StartedAt:      now,
		UpdatedAt:      now,
	}
	if err := repo.CreateFlowInstance(context.Background(), instance); err != nil {
		t.Fatalf("create waiting flow: %v", err)
	}

	resumable, err := repo.ListResumableFlowInstances(context.Background(), 20)
	if err != nil {
		t.Fatalf("list resumable flows: %v", err)
	}
	if len(resumable) != 1 || resumable[0].ID != instance.ID {
		t.Fatalf("expected waiting flow %d to be resumable, got %#v", instance.ID, resumable)
	}

	active, err := repo.FindActiveFlowInstanceByNameAndIdempotencyKey(context.Background(), instance.Name, instance.IdempotencyKey)
	if err != nil {
		t.Fatalf("find active waiting flow: %v", err)
	}
	if active.ID != instance.ID {
		t.Fatalf("expected waiting flow %d to be active, got %d", instance.ID, active.ID)
	}
}

// TestServiceParentAwaitsChildByReference verifies
// the parent node receives the child result through its explicit reference.
func TestServiceParentAwaitsChildByReference(t *testing.T) {
	db := openFlowTestDB(t)
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}

	repo := NewRepository(db)
	svc := NewService(repo)
	releaseChild := make(chan struct{})
	if err := svc.RegisterFlow(Flow{
		Name: "joined-child",
		Nodes: []FlowNode{{
			Name: "run",
			Action: func(ctx context.Context, rawInput string, runtime Runtime) (string, error) {
				<-releaseChild
				return tokens.NewTokenBuilder().SetResult(rawInput + "-child").Build()
			},
		}},
	}); err != nil {
		t.Fatalf("register child flow: %v", err)
	}
	if err := svc.RegisterFlow(Flow{
		Name: "joined-parent",
		Nodes: []FlowNode{
			{
				Name: "trigger",
				Action: func(ctx context.Context, rawInput string, runtime Runtime) (string, error) {
					ref, err := runtime.StartFlow(ctx, "joined-child", rawInput)
					if err != nil {
						return "", err
					}
					result, err := runtime.AwaitFlow(ctx, ref)
					if err != nil {
						return "", err
					}
					var childResult string
					if err := json.Unmarshal(result, &childResult); err != nil {
						return "", err
					}
					return rawInput + "-waiting|" + childResult, nil
				},
			},
			{
				Name:   "apply",
				Action: func(_ context.Context, rawInput string, _ Runtime) (string, error) { return rawInput, nil },
			},
		},
	}); err != nil {
		t.Fatalf("register parent flow: %v", err)
	}

	exec, err := svc.startFlowExecution(context.Background(), "joined-parent", "start")
	if err != nil {
		t.Fatalf("start parent: %v", err)
	}
	close(releaseChild)
	waitForDone(t, exec.Done())
	if exec.Status != FlowStatusFinished {
		t.Fatalf("expected joined parent to finish, got %s: %v", exec.Status, exec.Error)
	}
	if exec.EndToken != "start-waiting|start-child" {
		t.Fatalf("unexpected joined parent output: %q", exec.EndToken)
	}
}

func TestInheritFlowMetadataInStartTokenPreservesExplicitValues(t *testing.T) {
	startToken, err := tokens.NewTokenBuilder().
		SetInput("child-input").
		Meta("shared", "child").
		Meta("child-only", true).
		Build()
	if err != nil {
		t.Fatalf("build child start token: %v", err)
	}

	got, err := inheritFlowMetadataInStartToken(startToken, map[string]any{
		"shared":      "parent",
		"parent-only": "inherited",
	})
	if err != nil {
		t.Fatalf("inherit parent metadata: %v", err)
	}
	reader := tokens.NewTokenReader(got)
	if err := reader.Err(); err != nil {
		t.Fatalf("read child start token: %v", err)
	}
	for key, want := range map[string]any{
		"shared":      "child",
		"child-only":  true,
		"parent-only": "inherited",
	} {
		if value, ok := reader.Meta(key); !ok || value != want {
			t.Errorf("metadata %q = %#v, %v; want %#v", key, value, ok, want)
		}
	}
}

func TestInheritFlowMetadataInStartTokenPreservesLegacyInput(t *testing.T) {
	got, err := inheritFlowMetadataInStartToken("plain input", map[string]any{"source": "parent"})
	if err != nil {
		t.Fatalf("inherit parent metadata: %v", err)
	}
	if got != "plain input" {
		t.Fatalf("start token = %q, want unchanged plain input", got)
	}
}

// TestServiceMissingChildStartFailsParent verifies child dispatch errors
// are propagated to the waiting parent flow.
func TestServiceMissingChildStartFailsParent(t *testing.T) {
	db := openFlowTestDB(t)
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}

	repo := NewRepository(db)
	svc := NewService(repo)
	if err := svc.RegisterFlow(Flow{
		Name: "parent-with-missing-child",
		Nodes: []FlowNode{{
			Name: "trigger",
			Action: func(ctx context.Context, rawInput string, runtime Runtime) (string, error) {
				_, err := runtime.StartFlow(ctx, "missing-child", rawInput)
				if err != nil {
					return "", err
				}
				return rawInput, nil
			},
		}},
	}); err != nil {
		t.Fatalf("register parent flow: %v", err)
	}

	exec, err := svc.startFlowExecution(context.Background(), "parent-with-missing-child", "start")
	if err != nil {
		t.Fatalf("start parent: %v", err)
	}
	waitForDone(t, exec.Done())
	if exec.Status != FlowStatusFailed {
		t.Fatalf("expected parent to fail after child dispatch failure, got %s", exec.Status)
	}
	persisted, err := repo.FindFlowInstanceByID(context.Background(), exec.FlowInstanceID)
	if err != nil {
		t.Fatalf("load failed parent: %v", err)
	}
	if persisted.Status != FlowStatusFailed {
		t.Fatalf("expected persisted parent failure, got %s", persisted.Status)
	}
}

// TestServiceFindLatestFlowByNameAndCorrelationIDReturnsNewestMatch verifies
// lookup selects the newest run in the requested correlation chain.
func TestServiceFindLatestFlowByNameAndCorrelationIDReturnsNewestMatch(t *testing.T) {
	db := openFlowTestDB(t)
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}

	repo := NewRepository(db)
	svc := NewService(repo)
	if err := svc.RegisterFlow(Flow{
		Name: "inventory_photo_import",
		Nodes: []FlowNode{
			{
				Name: "finished",
				Action: func(ctx context.Context, rawInput string, runtime Runtime) (string, error) {
					return rawInput, nil
				},
			},
		},
	}); err != nil {
		t.Fatalf("register flow: %v", err)
	}

	now := time.Now().UTC()
	first := FlowInstance{
		Name:         "inventory_photo_import",
		Status:       FlowStatusFinished,
		MetadataJSON: `{"correlationId":"corr-1","scopeId":7}`,
		StartedAt:    now.Add(-2 * time.Hour),
		UpdatedAt:    now.Add(-2 * time.Hour),
	}
	second := FlowInstance{
		Name:         "inventory_photo_import",
		Status:       FlowStatusFinished,
		MetadataJSON: `{"correlationId":"corr-1","scopeId":7}`,
		StartedAt:    now.Add(-time.Hour),
		UpdatedAt:    now.Add(-time.Hour),
	}
	for _, item := range []*FlowInstance{&first, &second} {
		if err := repo.CreateFlowInstance(context.Background(), item); err != nil {
			t.Fatalf("create flow: %v", err)
		}
	}

	exec, err := svc.FindLatestFlowByNameAndCorrelationID(context.Background(), "inventory_photo_import", "corr-1")
	if err != nil {
		t.Fatalf("find latest flow by correlation: %v", err)
	}
	if exec.FlowInstanceID != second.ID {
		t.Fatalf("expected latest flow id %d, got %d", second.ID, exec.FlowInstanceID)
	}
}

// TestServiceCorrelationFailureWaitsForAllParallelFlows verifies failure
// notification waits until every parallel flow in the correlation is terminal.
func TestServiceCorrelationFailureWaitsForAllParallelFlows(t *testing.T) {
	db := openFlowTestDB(t)
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate flow: %v", err)
	}
	repo := NewRepository(db)
	svc := NewService(repo)
	if err := svc.RegisterFlow(Flow{
		Name: "failed-child",
		Nodes: []FlowNode{{Name: "run", Action: func(context.Context, string, Runtime) (string, error) {
			return "", nil
		}}},
	}); err != nil {
		t.Fatalf("register failed child: %v", err)
	}
	var calls []CorrelationFailureEvent
	svc.RegisterCorrelationFailureHook(func(_ context.Context, _ *Service, event CorrelationFailureEvent) {
		calls = append(calls, event)
	})
	now := time.Now().UTC()
	failed := &FlowInstance{
		Name:         "failed-child",
		Status:       FlowStatusFailed,
		ErrorMessage: "child failed",
		MetadataJSON: `{"correlationId":"corr-parallel"}`,
		StartedAt:    now,
		UpdatedAt:    now,
	}
	running := &FlowInstance{
		Name:         "running-child",
		Status:       FlowStatusRunning,
		MetadataJSON: `{"correlationId":"corr-parallel"}`,
		StartedAt:    now,
		UpdatedAt:    now,
	}
	for _, instance := range []*FlowInstance{failed, running} {
		if err := repo.CreateFlowInstance(context.Background(), instance); err != nil {
			t.Fatalf("create correlation flow: %v", err)
		}
	}

	svc.broadcastCorrelationFailure(context.Background(), FlowEvent{
		FlowInstanceID: running.ID,
		FlowName:       running.Name,
		CorrelationID:  "corr-parallel",
		Status:         FlowStatusFinished,
	})
	if len(calls) != 0 {
		t.Fatalf("failure hook called while sibling was running: %#v", calls)
	}

	running.Status = FlowStatusFinished
	if err := repo.UpdateFlowInstance(context.Background(), running); err != nil {
		t.Fatalf("finish running sibling: %v", err)
	}
	svc.broadcastCorrelationFailure(context.Background(), FlowEvent{
		FlowInstanceID: running.ID,
		FlowName:       running.Name,
		CorrelationID:  "corr-parallel",
		Status:         FlowStatusFinished,
	})
	if len(calls) != 1 {
		t.Fatalf("failure hook calls = %d, want 1", len(calls))
	}
	if calls[0].FlowEvent.FlowInstanceID != failed.ID || calls[0].FlowEvent.FlowName != failed.Name || calls[0].FlowEvent.Status != FlowStatusFailed {
		t.Fatalf("failure event = %#v, want failed child %d", calls[0].FlowEvent, failed.ID)
	}
}

func TestTerminalHookCanDecodeStartTokenSetThroughService(t *testing.T) {
	db := openFlowTestDB(t)
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}
	svc := NewService(NewRepository(db))
	if err := svc.RegisterFlow(Flow{Name: "terminal-hook-token", Nodes: []FlowNode{{Name: "run", Action: func(_ context.Context, input string, _ Runtime) (string, error) {
		return input, nil
	}}}}); err != nil {
		t.Fatalf("register flow: %v", err)
	}
	type state struct {
		Value string `json:"value"`
	}
	gotState := make(chan state, 1)
	hookErr := make(chan error, 1)
	svc.RegisterTerminalHook(func(ctx context.Context, engine *Service, event FlowEvent) {
		var decoded state
		err := engine.DecodeFlowStartTokenSet(ctx, FlowRef(event.FlowInstanceID), tokens.TokenSetState, &decoded)
		if err != nil {
			hookErr <- err
			return
		}
		gotState <- decoded
	})

	startToken, err := NewFlowTokenBuilder("terminal-hook-token", "run", state{Value: "from-start"}).Build()
	if err != nil {
		t.Fatalf("build start token: %v", err)
	}
	exec, err := svc.startFlowExecution(context.Background(), "terminal-hook-token", startToken)
	if err != nil {
		t.Fatalf("start flow: %v", err)
	}
	waitForDone(t, exec.Done())
	select {
	case err := <-hookErr:
		t.Fatalf("decode start token set in hook: %v", err)
	case got := <-gotState:
		if got.Value != "from-start" {
			t.Fatalf("decoded start state = %#v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("terminal hook did not decode start token set")
	}
}

// TestServiceRetryRestartsFailedChildWorkflow verifies retry starts a new child
// run while retaining the failed child as history.
func TestServiceRetryRestartsFailedChildWorkflow(t *testing.T) {
	db := openFlowTestDB(t)
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate flow: %v", err)
	}
	repo := NewRepository(db)
	svc := NewService(repo)
	var childAttempts atomic.Int32
	if err := svc.RegisterFlow(Flow{
		Name: "retry-child",
		Nodes: []FlowNode{{Name: "run", Action: func(context.Context, string, Runtime) (string, error) {
			if childAttempts.Add(1) == 1 {
				return "", errors.New("first child attempt failed")
			}
			return tokens.NewTokenBuilder().SetResult("child-ok").Build()
		}}},
	}); err != nil {
		t.Fatalf("register child flow: %v", err)
	}
	if err := svc.RegisterFlow(Flow{
		Name: "retry-parent",
		Nodes: []FlowNode{
			{Name: "trigger", Action: func(ctx context.Context, rawInput string, runtime Runtime) (string, error) {
				ref, err := runtime.StartFlow(ctx, "retry-child", rawInput)
				if err != nil {
					return "", err
				}
				result, err := runtime.AwaitFlow(ctx, ref)
				return string(result), err
			}},
			{Name: "finalize", Action: func(context.Context, string, Runtime) (string, error) {
				return "parent-ok", nil
			}},
		},
	}); err != nil {
		t.Fatalf("register parent flow: %v", err)
	}
	startToken, err := tokens.BuildTokenPayload(map[string]any{"retry.request": map[string]any{"request": "original"}}, map[string]any{"admin.primary_set": "retry.request"})
	if err != nil {
		t.Fatalf("build start token: %v", err)
	}
	parentExec, err := svc.startFlowExecution(context.Background(), "retry-parent", startToken, StartOptions{Metadata: EnsureCorrelationMetadata(nil, "Retry-Test", "Retry-Test")})
	if err != nil {
		t.Fatalf("start parent flow: %v", err)
	}
	waitForDone(t, parentExec.Done())
	failedParent, err := repo.FindFlowInstanceByID(context.Background(), parentExec.FlowInstanceID)
	if err != nil {
		t.Fatalf("load failed parent: %v", err)
	}
	if failedParent.Status != FlowStatusFailed || failedParent.CurrentNodeIndex != 0 {
		t.Fatalf("failed parent state = status %s node %d, want failed node 0", failedParent.Status, failedParent.CurrentNodeIndex)
	}
	var retryNode NodeInstance
	if err := db.Where("flow_instance_id = ? AND node_index = ?", failedParent.ID, failedParent.CurrentNodeIndex).First(&retryNode).Error; err != nil {
		t.Fatalf("load retry node: %v", err)
	}
	oldCorrelation := parseFlowMetadata(failedParent.MetadataJSON).CorrelationID
	if err := svc.UpdateCurrentNodeStartToken(context.Background(), FlowRef(failedParent.ID), retryNode.ID, startToken); err != nil {
		t.Fatalf("update retry token: %v", err)
	}
	if _, err := svc.ResumeFlowInstance(context.Background(), FlowRef(failedParent.ID)); err != nil {
		t.Fatalf("resume retried parent: %v", err)
	}
	retriedParent := waitForFlowInstanceByNameAndStatus(t, repo, "retry-parent", FlowStatusFinished)
	if retriedParent.ID != failedParent.ID {
		t.Fatalf("retry created unexpected parent instance %d, want existing %d", retriedParent.ID, failedParent.ID)
	}
	if got := parseFlowMetadata(retriedParent.MetadataJSON).CorrelationID; got == "" || got == oldCorrelation {
		t.Fatalf("retry correlation id = %q, want a new non-empty id", got)
	}
	var children []FlowInstance
	if err := db.Where("name = ?", "retry-child").Order("id asc").Find(&children).Error; err != nil {
		t.Fatalf("list child attempts: %v", err)
	}
	if len(children) != 2 || children[0].ID == children[1].ID {
		t.Fatalf("child attempts = %#v, want two distinct instances", children)
	}
	if children[0].Status != FlowStatusFailed || children[1].Status != FlowStatusFinished {
		t.Fatalf("child attempt statuses = %s/%s, want failed/finished", children[0].Status, children[1].Status)
	}
	if oldChildMetadata := parseFlowMetadata(children[0].MetadataJSON); oldChildMetadata.ParentFlowInstanceID != failedParent.ID || oldChildMetadata.CorrelationID != oldCorrelation {
		t.Fatalf("first child parent/correlation metadata = %#v", oldChildMetadata)
	}
	if newChildMetadata := parseFlowMetadata(children[1].MetadataJSON); newChildMetadata.ParentFlowInstanceID != failedParent.ID || newChildMetadata.CorrelationID != parseFlowMetadata(retriedParent.MetadataJSON).CorrelationID {
		t.Fatalf("retried child parent/correlation metadata = %#v", newChildMetadata)
	}
}

func testEditablePayload(t *testing.T, value string) string {
	t.Helper()
	payload, err := tokens.BuildTokenPayload(map[string]any{"value": value}, map[string]any{"admin.primary_set": "value"})
	if err != nil {
		t.Fatalf("build token payload: %v", err)
	}
	return payload
}

// openFlowTestDB creates an isolated SQLite database for repository tests.
func openFlowTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("get sqlite connection pool: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	return db
}

// seedStoppedFlowInstance creates a persisted flow and node checkpoint for
// resume and retry tests.
func seedStoppedFlowInstance(t *testing.T, repo *Repository, flowName string) uint {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	var flowInstanceID uint
	if err := repo.Transaction(ctx, func(tx *Repository) error {
		startToken := &Token{RawData: "start-token"}
		if err := tx.CreateToken(ctx, startToken); err != nil {
			return err
		}
		firstOutput := &Token{RawData: "start-token-analyse"}
		if err := tx.CreateToken(ctx, firstOutput); err != nil {
			return err
		}
		instance := &FlowInstance{
			Name:             flowName,
			Status:           FlowStatusStopped,
			CurrentNodeIndex: 1,
			InputTokenID:     &startToken.ID,
			OutputTokenID:    &firstOutput.ID,
			StartedAt:        now,
			UpdatedAt:        now,
		}
		if err := tx.CreateFlowInstance(ctx, instance); err != nil {
			return err
		}
		firstNode := &NodeInstance{
			Name:           "analyse",
			FlowInstanceID: instance.ID,
			NodeIndex:      0,
			Status:         NodeStatusFinished,
			InputTokenID:   &startToken.ID,
			OutputTokenID:  &firstOutput.ID,
			StartedAt:      now,
			UpdatedAt:      now,
			FinishedAt:     &now,
		}
		if err := tx.CreateNodeInstance(ctx, firstNode); err != nil {
			return err
		}
		secondNode := &NodeInstance{
			Name:                   "bild",
			FlowInstanceID:         instance.ID,
			NodeIndex:              1,
			Status:                 NodeStatusQueued,
			InputTokenID:           &firstOutput.ID,
			PreviousNodeInstanceID: &firstNode.ID,
			Active:                 true,
			StartedAt:              now,
			UpdatedAt:              now,
		}
		if err := tx.CreateNodeInstance(ctx, secondNode); err != nil {
			return err
		}
		firstNode.NextNodeInstanceID = &secondNode.ID
		if err := tx.UpdateNodeInstance(ctx, firstNode); err != nil {
			return err
		}
		instance.OutputTokenID = &firstOutput.ID
		if err := tx.UpdateFlowInstance(ctx, instance); err != nil {
			return err
		}
		flowInstanceID = instance.ID
		return nil
	}); err != nil {
		t.Fatalf("seed resumable flow: %v", err)
	}
	return flowInstanceID
}

// waitForDone fails the test if an asynchronous flow does not finish promptly.
func waitForDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
		return
	case <-time.After(3 * time.Second):
		t.Fatal("workflow did not finish in time")
	}
}

// waitForFlowInstanceByNameAndStatus polls persistence until a flow reaches a
// requested status or the test timeout expires.
func waitForFlowInstanceByNameAndStatus(t *testing.T, repo *Repository, name string, status FlowStatus) FlowInstance {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var instances []FlowInstance
		if err := repo.db.WithContext(context.Background()).
			Preload("OutputToken").
			Preload("InputToken").
			Where("name = ? AND status = ?", name, status).
			Order("id asc").
			Find(&instances).Error; err != nil {
			t.Fatalf("query flow instances: %v", err)
		}
		if len(instances) > 0 {
			return instances[0]
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for flow %q with status %s", name, status)
	return FlowInstance{}
}

// TestServiceActionCanStartAnotherFlow verifies a node can start and await a
// child flow through its runtime.
func TestServiceActionCanStartAnotherFlow(t *testing.T) {
	db := openFlowTestDB(t)
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}

	repo := NewRepository(db)
	svc := NewService(repo)

	if err := svc.RegisterFlow(Flow{
		Name: "child",
		Nodes: []FlowNode{
			{
				Name: "child-node",
				Action: func(ctx context.Context, rawInput string, runtime Runtime) (string, error) {
					return tokens.NewTokenBuilder().SetResult(rawInput + "-child").Build()
				},
			},
		},
	}); err != nil {
		t.Fatalf("register child flow: %v", err)
	}

	if err := svc.RegisterFlow(Flow{
		Name: "parent",
		Nodes: []FlowNode{
			{
				Name: "start-child",
				Action: func(ctx context.Context, rawInput string, runtime Runtime) (string, error) {
					ref, err := runtime.StartFlow(ctx, "child", rawInput)
					if err != nil {
						return "", err
					}
					result, err := runtime.AwaitFlow(ctx, ref)
					if err != nil {
						return "", err
					}
					var childResult string
					if err := json.Unmarshal(result, &childResult); err != nil {
						return "", err
					}
					return fmt.Sprintf("%s|%s", rawInput, childResult), nil
				},
			},
		},
	}); err != nil {
		t.Fatalf("register parent flow: %v", err)
	}

	exec, err := svc.startFlowExecution(context.Background(), "parent", "start-token")
	if err != nil {
		t.Fatalf("start parent flow: %v", err)
	}
	waitForDone(t, exec.Done())
	if exec.Status != FlowStatusFinished {
		t.Fatalf("expected finished parent flow, got %s", exec.Status)
	}
	persistedParent, err := repo.FindFlowInstanceByID(context.Background(), exec.FlowInstanceID)
	if err != nil {
		t.Fatalf("load parent flow: %v", err)
	}
	if persistedParent.OutputToken == nil {
		t.Fatal("parent output token missing")
	}
	if persistedParent.OutputToken.RawData != "start-token|start-token-child" {
		t.Fatalf("unexpected parent output token: %q", persistedParent.OutputToken.RawData)
	}

	child := waitForFlowInstanceByNameAndStatus(t, repo, "child", FlowStatusFinished)
	if child.OutputToken == nil {
		t.Fatalf("unexpected child output token: %#v", child.OutputToken)
	}
	parentMetadata := parseFlowMetadata(persistedParent.MetadataJSON)
	childMetadata := parseFlowMetadata(child.MetadataJSON)
	if childMetadata.ParentFlowInstanceID != persistedParent.ID || childMetadata.ParentNodeID == 0 {
		t.Fatalf("child parent metadata = %#v", childMetadata)
	}
	if childMetadata.CorrelationID == "" || childMetadata.CorrelationID != parentMetadata.CorrelationID {
		t.Fatalf("parent/child correlation = %q/%q", parentMetadata.CorrelationID, childMetadata.CorrelationID)
	}
}

// TestServiceDiscardFlowInstanceMarksCheckpointAsHidden verifies discard hides
// the failed run from user-facing resumable results.
func TestServiceDiscardFlowInstanceMarksCheckpointAsHidden(t *testing.T) {
	db := openFlowTestDB(t)
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}

	repo := NewRepository(db)
	svc := NewService(repo)
	if err := svc.RegisterFlow(Flow{
		Name: "single-node",
		Nodes: []FlowNode{
			{
				Name: "only",
				Action: func(ctx context.Context, rawInput string, runtime Runtime) (string, error) {
					return rawInput + "-done", nil
				},
			},
		},
	}); err != nil {
		t.Fatalf("register flow: %v", err)
	}

	exec, err := svc.startFlowExecution(context.Background(), "single-node", "start-token")
	if err != nil {
		t.Fatalf("start flow: %v", err)
	}
	waitForDone(t, exec.Done())
	if exec.Status != FlowStatusFinished {
		t.Fatalf("expected finished execution, got %s", exec.Status)
	}

	if err := svc.DiscardFlowInstance(context.Background(), FlowRef(exec.FlowInstanceID)); err != nil {
		t.Fatalf("discard flow: %v", err)
	}

	persisted, err := repo.FindFlowInstanceByID(context.Background(), exec.FlowInstanceID)
	if err != nil {
		t.Fatalf("load discarded flow: %v", err)
	}
	if persisted.DiscardedAt == nil {
		t.Fatal("expected discarded_at to be set")
	}
	if persisted.Status != FlowStatusFinished {
		t.Fatalf("discard must not change finished status, got %s", persisted.Status)
	}
	event, err := svc.FlowEventForInstance(context.Background(), FlowRef(exec.FlowInstanceID))
	if err != nil {
		t.Fatalf("load workflow event: %v", err)
	}
	if !event.Discarded {
		t.Fatal("expected workflow event to be marked discarded")
	}
	if event.BannerAvailable {
		t.Fatal("discarded workflow must not keep the failure banner available")
	}
}
