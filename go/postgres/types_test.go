package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDictionary(t *testing.T) {
	t.Run("ValidMap", func(t *testing.T) {
		dictionary := Dictionary(map[string]string{"yo": "yo"})
		v, err := dictionary.Value()
		require.NoError(t, err)
		var scannedDictionary Dictionary
		err = (&scannedDictionary).Scan(v)
		require.NoError(t, err)
		require.Equal(t, dictionary, scannedDictionary)
	})

	t.Run("EmptyMap", func(t *testing.T) {
		dictionary := Dictionary(map[string]string{"yo": "yo"})
		v, err := dictionary.Value()
		require.NoError(t, err)
		var scannedDictionary Dictionary
		err = (&scannedDictionary).Scan(v)
		require.NoError(t, err)
		require.Equal(t, dictionary, scannedDictionary)
	})

	t.Run("NilMap", func(t *testing.T) {
		dictionary := Dictionary(nil)
		v, err := dictionary.Value()
		require.NoError(t, err)
		var scannedDictionary Dictionary
		err = (&scannedDictionary).Scan(v)
		require.NoError(t, err)
		require.Equal(t, dictionary, scannedDictionary)
	})
}
