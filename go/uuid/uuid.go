package uuid

import (
	"github.com/google/uuid"
)

// UUID aliases the uuid.
type UUID = uuid.UUID

// NewV5 returns a new v5 uuid using SHA-1 hashing.
func NewV5(ns UUID, name string) UUID {
	return uuid.NewSHA1(ns, []byte(name))
}

// NewV7 returns a new v7 uuid.
func NewV7() (UUID, error) {
	return uuid.NewV7()
}

// MustNewV7 returns a new v7 uuid or panics if an error occurs.
func MustNewV7() UUID {
	id, err := NewV7()
	if err != nil {
		panic(err)
	}
	return id
}

// Parse parses a UUID from string.
func Parse(s string) (UUID, error) {
	return uuid.Parse(s)
}

// MustParse parses a UUID from string or panics if an error occurs.
func MustParse(s string) UUID {
	id, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return id
}
