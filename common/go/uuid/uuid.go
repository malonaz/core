package uuid

import (
	"github.com/satori/go.uuid"
)

// UUID is wrapper around the UUID object.
type UUID = uuid.UUID

// Nil is the nil UUID type.
var Nil = UUID{}

// MustNew returns a new v4 uuid or panics if an error occurs.
func MustNew() string {
	return uuid.NewV4().String()
}

// MustNewV5 returns a new v5 uuid or panics if an error occurs.
func MustNewV5(ns UUID, name string) string {
	return uuid.NewV5(ns, name).String()
}
