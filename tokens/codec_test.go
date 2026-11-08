package tokens_test

import (
	"strings"
	"testing"

	"github.com/wswag/go-flow/tokens"
	_ "github.com/wswag/go-flow/tokens/gzip"
)

// TestCodecRoundTripsCompressedPayloads verifies the registered gzip codec
// restores encoded token text exactly.
func TestCodecRoundTripsCompressedPayloads(t *testing.T) {
	raw := strings.Repeat(`{"rawResponse":"sehr-langes-token-payload"}`, 40)

	encoded, err := tokens.Encode(raw)
	if err != nil {
		t.Fatalf("encode token raw data: %v", err)
	}
	if !strings.HasPrefix(encoded, "gz:") {
		t.Fatalf("expected compressed token prefix, got %q", encoded[:min(len(encoded), 8)])
	}

	decoded, err := tokens.Decode(encoded)
	if err != nil {
		t.Fatalf("decode token raw data: %v", err)
	}
	if decoded != raw {
		t.Fatalf("decoded payload mismatch")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
