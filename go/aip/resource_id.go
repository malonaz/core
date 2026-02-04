package aip

import (
	"encoding/base32"

	"github.com/malonaz/core/go/uuid"
)

//nolint:gochecknoglobals
var base32Encoding = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// NewSystemGenerated returns a new system-generated resource ID.
func NewSystemGeneratedResourceID() string {
	return uuid.MustNewV7().String()
}

// NewSystemGenerated returns a new system-generated resource ID encoded as base32 lowercase.
func NewSystemGeneratedBase32ResourceID() string {
	id := uuid.MustNewV7()
	return base32Encoding.EncodeToString(id[:])
}
