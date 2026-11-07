package assistants

import (
	"context"
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
	if err := db.AutoMigrate(&ChainRecord{}); err != nil {
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
		Where("name = ? AND scope_key = ? AND actor_key = ?", strings.TrimSpace(name), strings.TrimSpace(key.ScopeKey), strings.TrimSpace(key.ActorKey)).
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
			},
			DoUpdates: clause.AssignmentColumns([]string{"state_json", "updated_at"}),
		}).
		Create(record).Error
}

// DeleteRecord verwirft den persistierten Zustand einer Schrittkette vollständig.
func (r *Repository) DeleteRecord(ctx context.Context, name string, key StateKey) error {
	return r.db.WithContext(ctx).
		Where("name = ? AND scope_key = ? AND actor_key = ?", strings.TrimSpace(name), strings.TrimSpace(key.ScopeKey), strings.TrimSpace(key.ActorKey)).
		Delete(&ChainRecord{}).Error
}
