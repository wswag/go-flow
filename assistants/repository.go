package assistants

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Repository kapselt die persistierte Ablage von Schrittkettenzuständen.
type Repository struct {
	db *gorm.DB
}

func NewRepository(db *gorm.DB) *Repository {
	return &Repository{db: db}
}

func AutoMigrate(db *gorm.DB) error {
	// The previous identity allowed only one state per chain/scope/actor. Drop
	// that index before migration so multiple DraftID runs can coexist.
	if db.Migrator().HasIndex(&ChainRecord{}, "idx_assistant_chain_scope_actor") {
		if err := db.Migrator().DropIndex(&ChainRecord{}, "idx_assistant_chain_scope_actor"); err != nil {
			return err
		}
	}
	if err := db.AutoMigrate(&ChainRecord{}, &ActionClaim{}); err != nil {
		return err
	}
	return ensureMariaDBLongTextColumn(db, &ChainRecord{}, "state_json")
}

func ensureMariaDBLongTextColumn(db *gorm.DB, model any, column string) error {
	if db == nil || !strings.EqualFold(db.Dialector.Name(), "mysql") {
		return nil
	}
	stmt := &gorm.Statement{DB: db}
	if err := stmt.Parse(model); err != nil {
		return err
	}
	field := stmt.Schema.LookUpField("StateJSON")
	columnName := strings.TrimSpace(column)
	if field == nil || columnName == "" {
		return nil
	}
	if !db.Migrator().HasColumn(model, field.DBName) {
		return nil
	}
	columnTypes, err := db.Migrator().ColumnTypes(model)
	if err != nil {
		return err
	}
	foundColumn := false
	for _, current := range columnTypes {
		if !strings.EqualFold(current.Name(), columnName) {
			continue
		}
		foundColumn = true
		if strings.EqualFold(current.DatabaseTypeName(), "LONGTEXT") {
			return nil
		}
		break
	}
	// Bestehende MariaDB-Installationen konnten hier noch ein altes varchar-Layout behalten.
	// Dann wächst der Assistant-State über die Spaltengröße hinaus und derselbe Onboarding-Schritt erscheint erneut.
	// Die Änderung läuft bewusst über den modellierten Feldtyp, damit künftige Spaltenattribute mitgeführt werden.
	if !foundColumn {
		return db.Migrator().AlterColumn(model, field.Name)
	}
	return db.Migrator().AlterColumn(model, field.Name)
}

// FindRecord lädt den gespeicherten Zustand für Kettenname und Schlüssel.
// Ist noch kein Zustand vorhanden, wird `gorm.ErrRecordNotFound` zurückgegeben.
func (r *Repository) FindRecord(ctx context.Context, name string, key StateKey) (ChainRecord, error) {
	var record ChainRecord
	err := r.db.WithContext(ctx).
		Where("name = ? AND scope_key = ? AND actor_key = ? AND draft_id = ? AND (status IS NULL OR status = '' OR status <> ?)", strings.TrimSpace(name), strings.TrimSpace(key.ScopeKey), strings.TrimSpace(key.ActorKey), strings.TrimSpace(key.DraftID), string(RunStatusDiscarded)).
		First(&record).Error
	return record, err
}

// FindRecordsByKey liefert alle gespeicherten Chains eines generischen Kontextes.
func (r *Repository) FindRecordsByKey(ctx context.Context, key StateKey) ([]ChainRecord, error) {
	var records []ChainRecord
	err := r.db.WithContext(ctx).
		Where("scope_key = ? AND actor_key = ?", strings.TrimSpace(key.ScopeKey), strings.TrimSpace(key.ActorKey)).
		Find(&records).Error
	return records, err
}

// FindRecordsByAccessRef finds states sharing an application-defined selector.
func (r *Repository) FindRecordsByAccessRef(ctx context.Context, accessRef string) ([]ChainRecord, error) {
	var records []ChainRecord
	err := r.db.WithContext(ctx).Where("access_ref = ?", strings.TrimSpace(accessRef)).Find(&records).Error
	return records, err
}

// UpdateAccess changes opaque application-owned access metadata for one state.
func (r *Repository) UpdateAccess(ctx context.Context, name string, key StateKey, accessRef, accessToken string) error {
	return r.db.WithContext(ctx).Model(&ChainRecord{}).
		Where("name = ? AND scope_key = ? AND actor_key = ? AND draft_id = ?", strings.TrimSpace(name), strings.TrimSpace(key.ScopeKey), strings.TrimSpace(key.ActorKey), strings.TrimSpace(key.DraftID)).
		Updates(map[string]any{"access_ref": strings.TrimSpace(accessRef), "access_token": accessToken, "updated_at": time.Now().UTC()}).Error
}

// ClaimAction atomically reserves an action key once within the assistant run.
func (r *Repository) ClaimAction(ctx context.Context, name string, key StateKey, actionKey string) (bool, error) {
	claim := ActionClaim{DraftID: strings.TrimSpace(key.DraftID), ActionKey: strings.TrimSpace(actionKey), CreatedAt: time.Now().UTC()}
	if claim.DraftID == "" || claim.ActionKey == "" {
		return false, errors.New("assistant action claim requires draft id and action key")
	}
	err := r.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&claim).Error
	return claim.ID != 0, err
}

func (r *Repository) ReleaseAction(ctx context.Context, draftID, actionKey string) error {
	return r.db.WithContext(ctx).Where("draft_id = ? AND action_key = ?", strings.TrimSpace(draftID), strings.TrimSpace(actionKey)).Delete(&ActionClaim{}).Error
}

// SaveRecord persistiert den aktuellen Zustand einer Schrittkette per atomarem Upsert.
//
// Der vorherige Read-then-Create-Pfad war bei gleichzeitig geöffneten Assistant-Views
// nicht sicher: Beide Requests konnten einen fehlenden Datensatz sehen und einer lief
// anschließend in den Unique-Index. OnConflict wird von GORM für SQLite als ON CONFLICT
// und für MariaDB als ON DUPLICATE KEY UPDATE ausgegeben.
func (r *Repository) SaveRecord(ctx context.Context, record *ChainRecord) error {
	if record == nil {
		return nil
	}
	now := time.Now().UTC()
	record.Name = strings.TrimSpace(record.Name)
	record.ScopeKey = strings.TrimSpace(record.ScopeKey)
	record.ActorKey = strings.TrimSpace(record.ActorKey)
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	// SaveRecord identifiziert die Kette ausschließlich über ihren fachlichen
	// Unique-Key. Ein vom Caller mitgegebener Primärschlüssel darf den Insert-Teil
	// des Upserts daher nicht auf einen anderen Datensatz festlegen.
	record.ID = 0
	record.UpdatedAt = now
	return r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "name"},
				{Name: "scope_key"},
				{Name: "actor_key"},
				{Name: "draft_id"},
			},
			DoUpdates: clause.AssignmentColumns([]string{"state_json", "status", "updated_at"}),
		}).
		Create(record).Error
}

func (r *Repository) CreateRun(ctx context.Context, record *ChainRecord) error {
	if record == nil || strings.TrimSpace(record.DraftID) == "" {
		return errors.New("assistant run draft id is required")
	}
	now := time.Now().UTC()
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.UpdatedAt = now
	return r.db.WithContext(ctx).Create(record).Error
}

func (r *Repository) SetRunExpiry(ctx context.Context, name string, key StateKey, expiresAt time.Time) error {
	return r.db.WithContext(ctx).Model(&ChainRecord{}).Where("name = ? AND scope_key = ? AND actor_key = ? AND draft_id = ?", strings.TrimSpace(name), strings.TrimSpace(key.ScopeKey), strings.TrimSpace(key.ActorKey), strings.TrimSpace(key.DraftID)).Update("access_expires_at", expiresAt.UTC()).Error
}

// CleanupExpiredRuns deletes expired run records and their action claims.
func (r *Repository) CleanupRuns(ctx context.Context, options CleanupRunsOptions) (int64, error) {
	if !options.Expired && options.MinimumAge <= 0 && len(options.Statuses) == 0 {
		return 0, errors.New("at least one run cleanup filter is required")
	}
	if options.Limit <= 0 {
		options.Limit = 100
	}
	query := r.db.WithContext(ctx).Model(&ChainRecord{})
	if options.Expired {
		query = query.Where("access_expires_at IS NOT NULL AND access_expires_at <= ?", time.Now().UTC())
	}
	if options.MinimumAge > 0 {
		query = query.Where("created_at <= ?", time.Now().UTC().Add(-options.MinimumAge))
	}
	if len(options.Statuses) > 0 {
		values := make([]string, 0, len(options.Statuses))
		for _, status := range options.Statuses {
			values = append(values, string(status))
		}
		query = query.Where("status IN ?", values)
	}
	var runs []ChainRecord
	if err := query.Order("created_at asc").Limit(options.Limit).Find(&runs).Error; err != nil {
		return 0, err
	}
	var deleted int64
	for _, run := range runs {
		if err := r.DeleteRecord(ctx, run.Name, StateKey{ScopeKey: run.ScopeKey, ActorKey: run.ActorKey, DraftID: run.DraftID}); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func (r *Repository) CleanupExpiredRuns(ctx context.Context, limit int) (int64, error) {
	return r.CleanupRuns(ctx, CleanupRunsOptions{Expired: true, Limit: limit})
}

// DiscardOpenRuns removes unfinished runs for a chain under the same scope and actor.
func (r *Repository) DiscardOpenRuns(ctx context.Context, name string, key StateKey) error {
	var records []ChainRecord
	err := r.db.WithContext(ctx).Where("name = ? AND scope_key = ? AND actor_key = ? AND draft_id <> ?", strings.TrimSpace(name), strings.TrimSpace(key.ScopeKey), strings.TrimSpace(key.ActorKey), strings.TrimSpace(key.DraftID)).Find(&records).Error
	if err != nil {
		return err
	}
	for _, record := range records {
		var state struct {
			CompletedAt *time.Time `json:"completedAt"`
		}
		if err := json.Unmarshal([]byte(record.StateJSON), &state); err != nil {
			return err
		}
		if state.CompletedAt == nil && record.Status != string(RunStatusDiscarded) {
			if err := r.db.WithContext(ctx).Model(&ChainRecord{}).Where("id = ?", record.ID).Update("status", string(RunStatusDiscarded)).Error; err != nil {
				return err
			}
		}
	}
	return nil
}

// DeleteRecord verwirft den persistierten Zustand einer Schrittkette vollständig.
func (r *Repository) DeleteRecord(ctx context.Context, name string, key StateKey) error {
	if err := r.db.WithContext(ctx).Where("draft_id = ?", strings.TrimSpace(key.DraftID)).Delete(&ActionClaim{}).Error; err != nil {
		return err
	}
	return r.db.WithContext(ctx).
		Where("name = ? AND scope_key = ? AND actor_key = ? AND draft_id = ?", strings.TrimSpace(name), strings.TrimSpace(key.ScopeKey), strings.TrimSpace(key.ActorKey), strings.TrimSpace(key.DraftID)).
		Delete(&ChainRecord{}).Error
}

func (r *Repository) MarkDiscarded(ctx context.Context, name string, key StateKey) error {
	return r.db.WithContext(ctx).Model(&ChainRecord{}).Where("name = ? AND scope_key = ? AND actor_key = ? AND draft_id = ?", strings.TrimSpace(name), strings.TrimSpace(key.ScopeKey), strings.TrimSpace(key.ActorKey), strings.TrimSpace(key.DraftID)).Update("status", string(RunStatusDiscarded)).Error
}
