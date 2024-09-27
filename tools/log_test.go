package tools

import (
	"testing"
)

func TestNewLogger(t *testing.T) {
	// test cases
	tests := []struct {
		name string
		pars map[string]interface{}
	}{

		{
			name: "Test case 01",
			pars: map[string]interface{}{
				"enabled": true,
				"level":   "debug",
				"output":  "stdout,./tmp/test.log",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := NewLogger(tt.pars)

			// log some info
			logger.Info().Msg("Test info")
		})
	}
}
