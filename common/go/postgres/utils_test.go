package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetDBColumns(t *testing.T) {
	type sample struct {
		B int    `db:"ya"`
		A int    `db:"yo"`
		C string `db:"bla"`
		D []string
		E int  `dbbb:"notThisOne"`
		f bool `db:"notThisOneEither"`
	}
	tags := GetDBColumns(sample{})
	require.Equal(t, []string{"ya", "yo", "bla"}, tags)
}

func GetNewNullString(t *testing.T) {
	t.Run("valid string", func(t *testing.T) {
		str := "validString"
		ns := NewNullString(str)
		require.True(t, ns.Valid)
		require.Equal(t, str, ns.String)
	})

	t.Run("invalid string", func(t *testing.T) {
		ns := NewNullString("")
		require.False(t, ns.Valid)
		require.Equal(t, "", ns.String)
	})
}
