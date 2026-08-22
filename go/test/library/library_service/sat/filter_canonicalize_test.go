package sat

import (
	"testing"

	"github.com/stretchr/testify/require"

	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

// Covers canonicalization of filter values: emails and phone numbers in
// filter expressions are canonicalized the same way stored values are.
func TestFilter_Canonicalization(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()

	author := createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Canonical Filter Author"
		a.EmailAddress = "canonical.filter+tag@gmail.com"
		a.PhoneNumber = "+12025550147"
		a.EmailAddresses = []string{"repeated.canonical@gmail.com"}
		a.PhoneNumbers = []string{"+14155550152"}
		a.Metadata = &librarypb.AuthorMetadata{
			Country:        "US",
			EmailAddresses: []string{"nested.canonical@gmail.com"},
		}
	})
	// Stored values are canonicalized on create (gmail dots/plus-tag stripped).
	require.Equal(t, "canonicalfilter@gmail.com", author.EmailAddress)

	findAuthor := func(t *testing.T, filter string) bool {
		t.Helper()
		for _, a := range listAuthors(t, parent, filter) {
			if a.Name == author.Name {
				return true
			}
		}
		return false
	}

	t.Run("EmailEqualityCanonicalized", func(t *testing.T) {
		t.Parallel()
		// Uppercase, dotted, plus-tagged form matches stored canonical form.
		require.True(t, findAuthor(t, `email_address = "Canonical.Filter+tag@GMAIL.com"`))
	})

	t.Run("EmailInequalityCanonicalized", func(t *testing.T) {
		t.Parallel()
		require.False(t, findAuthor(t, `email_address != "Canonical.Filter+tag@GMAIL.com" AND display_name = "Canonical Filter Author"`))
	})

	t.Run("PhoneEqualityCanonicalized", func(t *testing.T) {
		t.Parallel()
		// National format matches stored E.164 form.
		require.True(t, findAuthor(t, `phone_number = "(202) 555-0147"`))
	})

	t.Run("RepeatedEmailHasCanonicalized", func(t *testing.T) {
		t.Parallel()
		require.True(t, findAuthor(t, `email_addresses:"Repeated.Canonical@GoogleMail.com"`))
	})

	t.Run("RepeatedPhoneHasCanonicalized", func(t *testing.T) {
		t.Parallel()
		require.True(t, findAuthor(t, `phone_numbers:"415-555-0152"`))
	})

	t.Run("NestedRepeatedEmailHasCanonicalized", func(t *testing.T) {
		t.Parallel()
		require.True(t, findAuthor(t, `metadata.email_addresses:"Nested.Canonical@GMAIL.COM"`))
	})

	t.Run("WildcardEmailLowercased", func(t *testing.T) {
		t.Parallel()
		// Wildcard patterns are lowercased (not fully normalized), so mixed
		// casing still matches; gmail dot-stripping is not applied to them.
		require.True(t, findAuthor(t, `email_address = "CanonicalFilter*"`))
		require.True(t, findAuthor(t, `email_address = "*@GMAIL.COM"`))
	})

	t.Run("InvalidPhonePassesThrough", func(t *testing.T) {
		t.Parallel()
		// Invalid phone values are left unchanged and simply don't match.
		require.False(t, findAuthor(t, `phone_number = "not-a-phone"`))
	})
}
