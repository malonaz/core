package canonicalize

import (
	"strings"

	normalizer "github.com/dimuska139/go-email-normalizer/v5"
)

var (
	emailAddressNormalizer = normalizer.NewNormalizer()
)

func EmailAddress(address string) string {
	return strings.ToLower(emailAddressNormalizer.Normalize(address))
}
