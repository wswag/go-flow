package assistants

import (
	"context"
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestRepositorySaveRecordUpsertsAssistantChain(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("migrate assistant db: %v", err)
	}
	repo := NewRepository(db)
	key := StateKey{ScopeKey: "scope:6", ActorKey: "user:13"}

	if err := repo.SaveRecord(context.Background(), &ChainRecord{
		Name: "example_chain", ScopeKey: key.ScopeKey, ActorKey: key.ActorKey, StateJSON: `{"currentStep":"choose"}`,
	}); err != nil {
		t.Fatalf("create chain: %v", err)
	}
	if err := repo.SaveRecord(context.Background(), &ChainRecord{
		Name: "example_chain", ScopeKey: key.ScopeKey, ActorKey: key.ActorKey, StateJSON: `{"currentStep":"review"}`,
	}); err != nil {
		t.Fatalf("upsert chain: %v", err)
	}

	var count int64
	if err := db.Model(&ChainRecord{}).Where("name = ? AND scope_key = ? AND actor_key = ?", "example_chain", key.ScopeKey, key.ActorKey).Count(&count).Error; err != nil {
		t.Fatalf("count chains: %v", err)
	}
	if count != 1 {
		t.Fatalf("chain count = %d, want 1", count)
	}
	record, err := repo.FindRecord(context.Background(), "example_chain", key)
	if err != nil {
		t.Fatalf("find chain: %v", err)
	}
	if record.StateJSON != `{"currentStep":"review"}` {
		t.Fatalf("state = %s, want updated state", record.StateJSON)
	}
}
