package aip

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

func TestTypedAnnotation(t *testing.T) {
	synced := librarypb.Annotations.Synced

	t.Run("Absent", func(t *testing.T) {
		value, ok, err := synced.Get(&librarypb.Author{})
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, value)
	})

	t.Run("RoundTrip", func(t *testing.T) {
		author := &librarypb.Author{}
		at := timestamppb.New(time.Date(2026, 9, 14, 8, 0, 0, 0, time.UTC))
		require.NoError(t, synced.Set(author, at))
		// Opaque on the wire: base64 of the proto encoding.
		require.Equal(t, "CIDTntUG", author.GetAnnotations()[synced.Key])
		got, ok, err := synced.Get(author)
		require.NoError(t, err)
		require.True(t, ok)
		require.True(t, at.AsTime().Equal(got.AsTime()))
	})

	t.Run("MessageValue", func(t *testing.T) {
		author := &librarypb.Author{}
		profile := &librarypb.AuthorProfile{AuthorDisplayName: "Ada"}
		require.NoError(t, librarypb.Annotations.ProfileSnapshot.Set(author, profile))
		got, ok, err := librarypb.Annotations.ProfileSnapshot.Get(author)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "Ada", got.GetAuthorDisplayName())
	})

	t.Run("Malformed", func(t *testing.T) {
		author := &librarypb.Author{Annotations: map[string]string{synced.Key: "not-a-timestamp"}}
		_, ok, err := synced.Get(author)
		require.True(t, ok)
		require.Error(t, err)
	})

	t.Run("Delete", func(t *testing.T) {
		author := &librarypb.Author{}
		require.NoError(t, synced.Set(author, timestamppb.Now()))
		synced.Delete(author)
		require.False(t, synced.Has(author))
	})
}

func TestStringAnnotation(t *testing.T) {
	externalID := librarypb.Annotations.ExternalId
	author := &librarypb.Author{}
	externalID.Set(author, "abc")
	value, ok := externalID.Get(author)
	require.True(t, ok)
	require.Equal(t, "abc", value)
	externalID.Delete(author)
	require.False(t, externalID.Has(author))
}
