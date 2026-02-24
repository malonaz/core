package canonicalize

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmailAddress(t *testing.T) {
	tests := []struct {
		name string
		addr string
		want string
	}{
		{"lowercase", "user@example.com", "user@example.com"},
		{"uppercase", "USER@EXAMPLE.COM", "user@example.com"},
		{"mixed case", "User@Example.Com", "user@example.com"},
		{"gmail dots", "first.last@gmail.com", "firstlast@gmail.com"},
		{"gmail plus tag", "user+tag@gmail.com", "user@gmail.com"},
		{"gmail dots and plus", "first.last+tag@gmail.com", "firstlast@gmail.com"},
		{"googlemail alias", "user@googlemail.com", "user@gmail.com"},
		{"non-gmail dots preserved", "first.last@example.com", "first.last@example.com"},
		{"non-gmail plus preserved", "user+tag@example.com", "user+tag@example.com"},
		{"whitespace trimmed", " user@example.com ", "user@example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EmailAddress(tt.addr)
			require.Equal(t, tt.want, got)
		})
	}
}
