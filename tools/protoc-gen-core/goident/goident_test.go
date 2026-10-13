package goident

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVar(t *testing.T) {
	for name, expected := range map[string]string{
		"shelf":    "shelf",
		"Shelf":    "shelf",
		"book_id":  "bookId",
		"BookCase": "bookCase",
		"case":     "case_",
		"Case":     "case_",
		"Type":     "type_",
		"case_id":  "caseId",
	} {
		require.Equal(t, expected, Var(name), name)
	}
}
