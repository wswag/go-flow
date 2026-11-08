package workflows

import (
	"context"
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
)

var errInvalidRepository = errors.New("flow repository is not configured")

type Repository struct {
	db *gorm.DB
}

// NewRepository verdrahtet die Flow-Persistenz über GORM.
func NewRepository(db *gorm.DB) *Repository {
	return &Repository{db: db}
}

// AutoMigrate legt die Tabellen für Flows, Nodes und Tokens an.
func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(&Token{}, &FlowInstance{}, &NodeInstance{}, &TokenRewrite{})
}

// Transaction führt mehrere Flow-Änderungen atomar aus.
func (r *Repository) Transaction(ctx context.Context, fn func(*Repository) error) error {
	if r == nil || r.db == nil {
		return errInvalidRepository
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return fn(&Repository{db: tx})
	})
}

// CreateToken speichert einen neuen Token-Wert.
func (r *Repository) CreateToken(ctx context.Context, token *Token) error {
	if r == nil || r.db == nil || token == nil {
		return errInvalidRepository
	}
	token.RawData = strings.TrimSpace(token.RawData)
	originalRawData := token.RawData
	if token.CreatedAt.IsZero() {
		token.CreatedAt = time.Now().UTC()
	}
	if err := r.db.WithContext(ctx).Create(token).Error; err != nil {
		return err
	}
	token.RawData = originalRawData
	return nil
}

// FindTokenByID lädt einen gespeicherten Token.
func (r *Repository) FindTokenByID(ctx context.Context, id uint) (Token, error) {
	var token Token
	err := r.db.WithContext(ctx).First(&token, id).Error
	if err == nil {
		normalizeLoadedToken(&token)
	}
	return token, err
}

// ScrubExpiredTokenData entfernt alte Token-Rohdaten, sobald sie nicht mehr für aktive Flows benötigt werden.
// Die Token-Datensätze bleiben erhalten, damit Workflow-Referenzen und Support-Historie konsistent bleiben.
func (r *Repository) ScrubExpiredTokenData(ctx context.Context, cutoff time.Time) (int64, error) {
	if r == nil || r.db == nil {
		return 0, errInvalidRepository
	}
	result := r.db.WithContext(ctx).Exec(`
		UPDATE tokens
		SET raw_data = ''
		WHERE raw_data <> ''
		  AND created_at < ?
		  AND NOT EXISTS (
		    SELECT 1
		    FROM flow_instances
		    WHERE status IN (?, ?, ?)
		      AND (
		        input_token_id = tokens.id
		        OR output_token_id = tokens.id
		        OR caller_end_token_id = tokens.id
		      )
		  )
		  AND NOT EXISTS (
		    SELECT 1
		    FROM node_instances
		    WHERE status IN (?, ?)
		      AND (
		        input_token_id = tokens.id
		        OR output_token_id = tokens.id
		        OR caller_end_token_id = tokens.id
		      )
		  )
	`, cutoff.UTC(), FlowStatusRunning, FlowStatusWaiting, FlowStatusStopped, NodeStatusQueued, NodeStatusRunning)
	return result.RowsAffected, result.Error
}

// CreateFlowInstance speichert einen neuen Flow-Lauf.
func (r *Repository) CreateFlowInstance(ctx context.Context, flow *FlowInstance) error {
	if r == nil || r.db == nil || flow == nil {
		return errInvalidRepository
	}
	normalizeFlowInstance(flow)
	return r.db.WithContext(ctx).Create(flow).Error
}

// UpdateFlowInstance aktualisiert den aktuellen Status eines Flow-Laufs.
func (r *Repository) UpdateFlowInstance(ctx context.Context, flow *FlowInstance) error {
	if r == nil || r.db == nil || flow == nil {
		return errInvalidRepository
	}
	normalizeFlowInstance(flow)
	return r.db.WithContext(ctx).Save(flow).Error
}

// CreateNodeInstance speichert einen neuen Workflow-Knoten.
func (r *Repository) CreateNodeInstance(ctx context.Context, node *NodeInstance) error {
	if r == nil || r.db == nil || node == nil {
		return errInvalidRepository
	}
	normalizeNodeInstance(node)
	return r.db.WithContext(ctx).Create(node).Error
}

// UpdateNodeInstance aktualisiert einen bestehenden Workflow-Knoten.
func (r *Repository) UpdateNodeInstance(ctx context.Context, node *NodeInstance) error {
	if r == nil || r.db == nil || node == nil {
		return errInvalidRepository
	}
	normalizeNodeInstance(node)
	return r.db.WithContext(ctx).Save(node).Error
}

// FindFlowInstanceByID lädt einen Workflow-Lauf samt Knoten und Tokens.
func (r *Repository) FindFlowInstanceByID(ctx context.Context, id uint) (FlowInstance, error) {
	var instance FlowInstance
	err := r.flowQuery(ctx).
		Where("id = ?", id).
		First(&instance).Error
	if err == nil {
		normalizeLoadedFlowInstance(&instance)
	}
	return instance, err
}

// FindFlowInstanceSummaryByID lädt einen Workflow-Lauf ohne gekoppelte Preloads.
func (r *Repository) FindFlowInstanceSummaryByID(ctx context.Context, id uint) (FlowInstance, error) {
	var instance FlowInstance
	err := r.db.WithContext(ctx).
		Preload("InputToken").
		Preload("OutputToken").
		Preload("CallerEndToken").
		First(&instance, id).Error
	if err == nil {
		normalizeLoadedFlowInstance(&instance)
	}
	return instance, err
}

// FindFlowInstanceByParentSystemNodeID findet den eindeutig mit einem
// System-Knoten verknüpften Kindlauf. Der Link verhindert doppelte Kindläufe,
// wenn ein Dispatch nach einem Prozessabbruch wiederholt wird.
func (r *Repository) FindFlowInstanceByParentSystemNodeID(ctx context.Context, parentSystemNodeID uint) (FlowInstance, error) {
	var instance FlowInstance
	err := r.flowQuery(ctx).
		Where("parent_system_node_id = ?", parentSystemNodeID).
		Order("id desc").
		First(&instance).Error
	if err == nil {
		normalizeLoadedFlowInstance(&instance)
	}
	return instance, err
}

// FindNodeInstanceByID lädt einen einzelnen Knotendatensatz.
func (r *Repository) FindNodeInstanceByID(ctx context.Context, id uint) (NodeInstance, error) {
	var node NodeInstance
	err := r.db.WithContext(ctx).
		Preload("InputToken").
		Preload("OutputToken").
		Preload("CallerEndToken").
		Preload("FlowInstance").
		First(&node, id).Error
	if err == nil {
		normalizeLoadedNodeInstance(&node)
		normalizeLoadedFlowInstance(node.FlowInstance)
	}
	return node, err
}

// ListQueuedSystemNodes liefert alle noch zu dispatchenden oder nach einem
// Prozessabbruch wiederaufzunehmenden System-Knoten für Folge-Flows.
func (r *Repository) ListQueuedSystemNodes(ctx context.Context, limit int) ([]NodeInstance, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	items := make([]NodeInstance, 0, limit)
	err := r.db.WithContext(ctx).
		Preload("InputToken").
		Preload("OutputToken").
		Preload("CallerEndToken").
		Preload("FlowInstance").
		Where("system_node = ? AND status IN ?", true, []NodeStatus{NodeStatusQueued, NodeStatusRunning}).
		Order("updated_at asc").
		Order("id asc").
		Limit(limit).
		Find(&items).Error
	if err == nil {
		normalizeLoadedNodeInstances(items)
	}
	return items, err
}

// ListQueuedSystemNodesByFlowInstance liefert wartende System-Knoten eines Flow-Laufs.
func (r *Repository) ListQueuedSystemNodesByFlowInstance(ctx context.Context, flowInstanceID uint) ([]NodeInstance, error) {
	items := make([]NodeInstance, 0, 4)
	err := r.db.WithContext(ctx).
		Preload("InputToken").
		Preload("OutputToken").
		Preload("CallerEndToken").
		Preload("FlowInstance").
		Where("flow_instance_id = ? AND system_node = ? AND status = ?", flowInstanceID, true, NodeStatusQueued).
		Order("node_index asc").
		Find(&items).Error
	if err == nil {
		normalizeLoadedNodeInstances(items)
	}
	return items, err
}

// ListSystemNodesByPreviousNode liefert die aus einem fachlichen Node
// gestarteten Kindflows samt ihren persistierten Ergebnissen.
func (r *Repository) ListSystemNodesByPreviousNode(ctx context.Context, flowInstanceID uint, previousNodeID uint) ([]NodeInstance, error) {
	items := make([]NodeInstance, 0, 4)
	err := r.db.WithContext(ctx).
		Preload("OutputToken").
		Where("flow_instance_id = ? AND system_node = ? AND previous_node_instance_id = ?", flowInstanceID, true, previousNodeID).
		Order("node_index asc").
		Find(&items).Error
	if err == nil {
		normalizeLoadedNodeInstances(items)
	}
	return items, err
}

// ListResumableFlowInstances liefert alle pausierten oder nicht abgeschlossenen Flows.
func (r *Repository) ListResumableFlowInstances(ctx context.Context, limit int) ([]FlowInstance, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}
	items := make([]FlowInstance, 0, limit)
	err := r.flowQuery(ctx).
		Where("status IN ?", []FlowStatus{FlowStatusRunning, FlowStatusWaiting, FlowStatusStopped}).
		Order("updated_at asc").
		Order("id asc").
		Limit(limit).
		Find(&items).Error
	if err == nil {
		for index := range items {
			normalizeLoadedFlowInstance(&items[index])
		}
	}
	return items, err
}

// FindActiveFlowInstanceByNameAndIdempotencyKey sucht einen noch aktiven Flow mit fachlichem Schlüssel.
func (r *Repository) FindActiveFlowInstanceByNameAndIdempotencyKey(ctx context.Context, name string, idempotencyKey string) (FlowInstance, error) {
	var instance FlowInstance
	err := r.flowQuery(ctx).
		Where("name = ? AND idempotency_key = ? AND status IN ?", strings.TrimSpace(name), strings.TrimSpace(idempotencyKey), []FlowStatus{FlowStatusRunning, FlowStatusWaiting, FlowStatusStopped}).
		Order("id desc").
		First(&instance).Error
	if err == nil {
		normalizeLoadedFlowInstance(&instance)
	}
	return instance, err
}

// FindLatestFlowInstanceByNameAndCorrelationID sucht den jüngsten Lauf eines
// Workflows innerhalb derselben Correlation-Kette.
func (r *Repository) FindLatestFlowInstanceByNameAndCorrelationID(ctx context.Context, name string, correlationID string) (FlowInstance, error) {
	var instance FlowInstance
	err := r.flowQuery(ctx).
		Where("name = ? AND metadata_json IS NOT NULL AND trim(metadata_json) <> '' AND json_valid(metadata_json) AND json_extract(metadata_json, '$.correlationId') = ?", strings.TrimSpace(name), strings.TrimSpace(correlationID)).
		Order("updated_at desc").
		Order("id desc").
		First(&instance).Error
	if err == nil {
		normalizeLoadedFlowInstance(&instance)
	}
	return instance, err
}

// ListLatestFlowInstancesByName liefert die jüngsten gespeicherten Läufe eines
// fachlich beliebigen Workflows. Die Engine interpretiert deren Payload nicht;
// fachliche Caller prüfen selbst, welche ihrer eigenen Token-Verträge passen.
func (r *Repository) ListLatestFlowInstancesByName(ctx context.Context, name string, limit int) ([]FlowInstance, error) {
	if r == nil || r.db == nil {
		return nil, errInvalidRepository
	}
	if limit <= 0 {
		return []FlowInstance{}, nil
	}
	items := make([]FlowInstance, 0, limit)
	err := r.flowQuery(ctx).
		Where("name = ?", strings.TrimSpace(name)).
		Order("updated_at desc").
		Order("id desc").
		Limit(limit).
		Find(&items).Error
	if err == nil {
		for index := range items {
			normalizeLoadedFlowInstance(&items[index])
		}
	}
	return items, err
}

// ListFlowInstancesByCorrelationID lädt alle Workflow-Läufe einer Correlation-Kette.
func (r *Repository) ListFlowInstancesByCorrelationID(ctx context.Context, correlationID string) ([]FlowInstance, error) {
	items := make([]FlowInstance, 0)
	err := r.flowQuery(ctx).
		Where("metadata_json IS NOT NULL AND trim(metadata_json) <> '' AND json_valid(metadata_json) AND json_extract(metadata_json, '$.correlationId') = ?", strings.TrimSpace(correlationID)).
		Order("updated_at asc").
		Order("id asc").
		Find(&items).Error
	if err == nil {
		for index := range items {
			normalizeLoadedFlowInstance(&items[index])
		}
	}
	return items, err
}

// listFlowInstanceSummariesByCorrelationID lädt für interne Statusentscheidungen
// nur die Flow-Läufe selbst. Die vollständige Correlation-Abfrage oben bleibt
// für API- und Fachkontexte inklusive ihrer Preloads erhalten.
func (r *Repository) listFlowInstanceSummariesByCorrelationID(ctx context.Context, correlationID string) ([]FlowInstance, error) {
	items := make([]FlowInstance, 0)
	err := r.db.WithContext(ctx).
		Where("metadata_json IS NOT NULL AND trim(metadata_json) <> '' AND json_valid(metadata_json) AND json_extract(metadata_json, '$.correlationId') = ?", strings.TrimSpace(correlationID)).
		Order("updated_at asc").
		Order("id asc").
		Find(&items).Error
	if err == nil {
		for index := range items {
			normalizeLoadedFlowInstance(&items[index])
		}
	}
	return items, err
}

// CountFlowInstancesByNameAndHouseholdIDAndStatus zählt persistierte Workflow-Läufe
// für einen konkreten Haushalt und Status.
func (r *Repository) CountFlowInstancesByNameAndHouseholdIDAndStatus(ctx context.Context, name string, householdID uint, status FlowStatus) (int64, error) {
	if r == nil || r.db == nil {
		return 0, errInvalidRepository
	}
	if householdID == 0 {
		return 0, nil
	}
	var count int64
	err := r.db.WithContext(ctx).
		Model(&FlowInstance{}).
		Where("name = ? AND status = ?", strings.TrimSpace(name), status).
		Where("metadata_json IS NOT NULL AND trim(metadata_json) <> '' AND json_valid(metadata_json) AND json_extract(metadata_json, '$.householdId') = ?", householdID).
		Count(&count).Error
	return count, err
}

// normalizeLoadedFlowInstance repairs optional associations on a loaded flow
// so callers can traverse them without nil checks.
func normalizeLoadedFlowInstance(instance *FlowInstance) {
	if instance == nil {
		return
	}
	normalizeLoadedToken(instance.InputToken)
	normalizeLoadedToken(instance.OutputToken)
	normalizeLoadedToken(instance.CallerEndToken)
	for index := range instance.NodeInstances {
		normalizeLoadedNodeInstance(&instance.NodeInstances[index])
	}
}

// normalizeLoadedNodeInstance repairs optional associations on a loaded node.
func normalizeLoadedNodeInstance(node *NodeInstance) {
	if node == nil {
		return
	}
	normalizeLoadedToken(node.InputToken)
	normalizeLoadedToken(node.OutputToken)
	normalizeLoadedToken(node.CallerEndToken)
}

// normalizeLoadedNodeInstances normalizes each node in a loaded result set.
func normalizeLoadedNodeInstances(items []NodeInstance) {
	for index := range items {
		normalizeLoadedNodeInstance(&items[index])
		normalizeLoadedFlowInstance(items[index].FlowInstance)
	}
}

// normalizeLoadedToken ensures a loaded token has a usable raw-data value.
func normalizeLoadedToken(token *Token) {
	if token == nil {
		return
	}
	decoded, err := decodeTokenRawData(token.RawData)
	if err != nil {
		return
	}
	token.RawData = decoded
}

// flowQuery returns the base flow-instance query bound to the request context.
func (r *Repository) flowQuery(ctx context.Context) *gorm.DB {
	return r.db.WithContext(ctx).
		Preload("InputToken").
		Preload("OutputToken").
		Preload("CallerEndToken").
		Preload("NodeInstances", func(tx *gorm.DB) *gorm.DB {
			return tx.Order("node_index asc")
		}).
		Preload("NodeInstances.InputToken").
		Preload("NodeInstances.OutputToken").
		Preload("NodeInstances.PreviousNodeInstance").
		Preload("NodeInstances.NextNodeInstance")
}

// normalizeFlowInstance initializes absent flow collections after loading.
func normalizeFlowInstance(flow *FlowInstance) {
	if flow == nil {
		return
	}
	flow.Name = strings.TrimSpace(flow.Name)
	flow.IdempotencyKey = strings.TrimSpace(flow.IdempotencyKey)
	if flow.Status == "" {
		flow.Status = FlowStatusPending
	}
	if flow.StartedAt.IsZero() {
		flow.StartedAt = time.Now().UTC()
	}
	flow.UpdatedAt = time.Now().UTC()
}

// normalizeNodeInstance initializes absent node associations after loading.
func normalizeNodeInstance(node *NodeInstance) {
	if node == nil {
		return
	}
	node.Name = strings.TrimSpace(node.Name)
	if node.Status == "" {
		node.Status = NodeStatusQueued
	}
	if node.StartedAt.IsZero() {
		node.StartedAt = time.Now().UTC()
	}
	node.UpdatedAt = time.Now().UTC()
}
