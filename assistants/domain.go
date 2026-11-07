package assistants

import (
	"time"

	"github.com/wswag/go-flow/tokens"
	_ "github.com/wswag/go-flow/tokens/gzip"
	"gorm.io/gorm"
)

// ChainRecord speichert den persistierten UI-Zustand einer Schrittkette pro Schlüssel.
// `ScopeKey` und `ActorKey` bleiben generisch, damit Fachkontexte außerhalb des Packages definiert werden.
type ChainRecord struct {
	ID        uint      `gorm:"primaryKey"`
	Name      string    `gorm:"type:varchar(120);index;uniqueIndex:idx_assistant_chain_scope_actor,priority:1"`
	ScopeKey  string    `gorm:"type:varchar(255);index;uniqueIndex:idx_assistant_chain_scope_actor,priority:2"`
	ActorKey  string    `gorm:"type:varchar(120);index;uniqueIndex:idx_assistant_chain_scope_actor,priority:3"`
	StateJSON string    `gorm:"type:longtext"`
	CreatedAt time.Time `gorm:"index"`
	UpdatedAt time.Time `gorm:"index"`
}

func (r *ChainRecord) BeforeSave(_ *gorm.DB) error {
	encoded, err := tokens.Encode(r.StateJSON)
	if err == nil {
		r.StateJSON = encoded
	}
	return err
}

func (r *ChainRecord) AfterSave(_ *gorm.DB) error {
	decoded, err := tokens.Decode(r.StateJSON)
	if err == nil {
		r.StateJSON = decoded
	}
	return err
}

func (r *ChainRecord) AfterFind(_ *gorm.DB) error {
	decoded, err := tokens.Decode(r.StateJSON)
	if err == nil {
		r.StateJSON = decoded
	}
	return err
}

// StateKey beschreibt den fachlichen Zielkontext einer Schrittkette.
// Die konkreten Schlüssel werden vom jeweiligen Aufrufer definiert.
type StateKey struct {
	ScopeKey string
	ActorKey string
}
