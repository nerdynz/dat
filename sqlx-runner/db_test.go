package runner

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersion(t *testing.T) {
	// require at least 9.3+ for testing
	assert.True(t, testDB.Version > 90300)
}
