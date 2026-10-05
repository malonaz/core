package aip

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseStandardMethodType(t *testing.T) {
	for _, tc := range []struct {
		methodName string
		singular   string
		plural     string
		expected   StandardMethodType
	}{
		{"CreateBook", "book", "books", StandardMethodTypeCreate},
		{"BatchCreateBooks", "book", "books", StandardMethodTypeBatchCreate},
		{"GetBook", "book", "books", StandardMethodTypeGet},
		{"BatchGetBooks", "book", "books", StandardMethodTypeBatchGet},
		{"UpdateBook", "book", "books", StandardMethodTypeUpdate},
		{"DeleteBook", "book", "books", StandardMethodTypeDelete},
		{"UndeleteBook", "book", "books", StandardMethodTypeUndelete},
		{"ListBooks", "book", "books", StandardMethodTypeList},
		{"SearchBooks", "book", "books", StandardMethodTypeSearch},
		{"ImportBooks", "book", "books", StandardMethodTypeImport},
		{"ExportBooks", "book", "books", StandardMethodTypeExport},
		{"ExportLoanCommissions", "loanCommission", "loanCommissions", StandardMethodTypeExport},
		// Go casing capitalizes a letter after a digit.
		{"GetS3Bucket", "s3bucket", "s3buckets", StandardMethodTypeGet},
		{"ListOauth2Tokens", "oauth2token", "oauth2tokens", StandardMethodTypeList},
		// Wrong cardinality or unknown verb.
		{"GetBooks", "book", "books", StandardMethodTypeUnspecified},
		{"ListBook", "book", "books", StandardMethodTypeUnspecified},
		{"PurgeBooks", "book", "books", StandardMethodTypeUnspecified},
	} {
		t.Run(tc.methodName, func(t *testing.T) {
			require.Equal(t, tc.expected, ParseStandardMethodType(tc.methodName, tc.singular, tc.plural))
		})
	}
}
