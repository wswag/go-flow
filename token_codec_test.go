package flow

import (
	"context"
	"strings"
	"testing"
)

// TestTokenCodecRoundTripsCompressedPayloads verifies compressed token data is
// restored exactly after database persistence.
func TestTokenCodecRoundTripsCompressedPayloads(t *testing.T) {
	raw := strings.Repeat(`{"rawResponse":"sehr-langes-token-payload"}`, 40)

	encoded, err := encodeTokenRawData(raw)
	if err != nil {
		t.Fatalf("encode token raw data: %v", err)
	}
	if !strings.HasPrefix(encoded, compressedTokenPrefix) {
		t.Fatalf("expected compressed token prefix, got %q", encoded[:min(len(encoded), 8)])
	}

	decoded, err := decodeTokenRawData(encoded)
	if err != nil {
		t.Fatalf("decode token raw data: %v", err)
	}
	if decoded != raw {
		t.Fatalf("decoded payload mismatch")
	}
}

// TestRepositoryReadsLegacyAndCompressedTokens verifies repository reads
// transparently handle both plain stored text and marked compressed data.
func TestRepositoryReadsLegacyAndCompressedTokens(t *testing.T) {
	db := openFlowTestDB(t)
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}

	repo := NewRepository(db)
	ctx := context.Background()

	originalCompressedRaw := strings.Repeat("token-payload-", 50)
	compressed := &Token{RawData: originalCompressedRaw}
	if err := repo.CreateToken(ctx, compressed); err != nil {
		t.Fatalf("create compressed token: %v", err)
	}
	loadedCompressed, err := repo.FindTokenByID(ctx, compressed.ID)
	if err != nil {
		t.Fatalf("load compressed token: %v", err)
	}
	if loadedCompressed.RawData != originalCompressedRaw {
		t.Fatalf("compressed token roundtrip mismatch")
	}

	legacy := "legacy-token-payload"
	if err := db.WithContext(ctx).Exec("INSERT INTO tokens (raw_data, created_at) VALUES (?, ?)", legacy, "2026-06-14T12:00:00Z").Error; err != nil {
		t.Fatalf("seed legacy token: %v", err)
	}
	var legacyToken Token
	if err := db.WithContext(ctx).Order("id desc").First(&legacyToken).Error; err != nil {
		t.Fatalf("load legacy token: %v", err)
	}
	if legacyToken.RawData != legacy {
		t.Fatalf("legacy token raw data = %q, want %q", legacyToken.RawData, legacy)
	}
}

// TestRepositoryCreateTokenKeepsCallerFacingRawDataReadable verifies save hooks
// do not replace the caller's in-memory token with its storage encoding.
func TestRepositoryCreateTokenKeepsCallerFacingRawDataReadable(t *testing.T) {
	db := openFlowTestDB(t)
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}

	repo := NewRepository(db)
	ctx := context.Background()
	raw := strings.Repeat("caller-facing-token-", 40)
	token := &Token{RawData: raw}
	if err := repo.CreateToken(ctx, token); err != nil {
		t.Fatalf("create token: %v", err)
	}
	if token.RawData != raw {
		t.Fatalf("token raw data after create = %q, want original readable payload", token.RawData)
	}
}

// min returns the smaller of two integers for test fixture sizing.
func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
