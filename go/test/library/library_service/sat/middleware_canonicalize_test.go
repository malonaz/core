package sat

import (
	"testing"
	"time"

	validatepb "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

func TestCanonicalize_AppliedOnCreate(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("Email", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.EmailAddress = "CREATE@EXAMPLE.COM"
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "create@example.com", createdAuthor.EmailAddress)
	})

	t.Run("PhoneNumber", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.PhoneNumber = "(202) 555-1234"
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "+12025551234", createdAuthor.PhoneNumber)
	})

	t.Run("RepeatedEmails", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.EmailAddresses = []string{"ALICE@TEST.COM", "BOB@TEST.COM"}
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, []string{"alice@test.com", "bob@test.com"}, createdAuthor.EmailAddresses)
	})

	t.Run("RepeatedPhoneNumbers", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.PhoneNumbers = []string{"(415) 555-0001", "+33 6 12 34 56 78"}
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, []string{"+14155550001", "+33612345678"}, createdAuthor.PhoneNumbers)
	})

	t.Run("NestedRepeatedEmails", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.Metadata.EmailAddresses = []string{"META@TEST.COM"}
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, []string{"meta@test.com"}, createdAuthor.Metadata.EmailAddresses)
	})

	t.Run("NestedRepeatedPhoneNumbers", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.Metadata.PhoneNumbers = []string{"+44 20 7946 0958"}
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, []string{"+442079460958"}, createdAuthor.Metadata.PhoneNumbers)
	})
}

func TestCanonicalize_AppliedOnUpdate(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("Email", func(t *testing.T) {
		t.Parallel()
		original := createTestAuthor(t, organizationParent, "Canon Update Email")
		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name:         original.Name,
				EmailAddress: "UPDATE@EXAMPLE.COM",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"email_address"}},
		}
		updatedAuthor, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "update@example.com", updatedAuthor.EmailAddress)
	})

	t.Run("PhoneNumber", func(t *testing.T) {
		t.Parallel()
		original := createTestAuthor(t, organizationParent, "Canon Update Phone")
		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name:        original.Name,
				PhoneNumber: "+33 6 98 76 54 32",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"phone_number"}},
		}
		updatedAuthor, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "+33698765432", updatedAuthor.PhoneNumber)
	})
}

// requireCanonicalizationViolation asserts a canonicalization failure carries
// both machine-readable details: buf.validate.Violations (protovalidate's
// dialect, request-relative FieldPath) and google.rpc.BadRequest (AIP-193,
// dotted request-relative path).
func requireCanonicalizationViolation(t *testing.T, err error, wantFieldNames []string, wantPath string) {
	t.Helper()
	grpcStatus, ok := status.FromError(err)
	require.True(t, ok)

	var violations *validatepb.Violations
	var badRequest *errdetails.BadRequest
	for _, detail := range grpcStatus.Details() {
		switch detail := detail.(type) {
		case *validatepb.Violations:
			violations = detail
		case *errdetails.BadRequest:
			badRequest = detail
		}
	}

	require.NotNil(t, violations)
	require.Len(t, violations.GetViolations(), 1)
	violation := violations.GetViolations()[0]
	require.Equal(t, "canonicalize.phone_number", violation.GetRuleId())
	require.NotEmpty(t, violation.GetMessage())
	fieldNames := []string{}
	for _, element := range violation.GetField().GetElements() {
		require.NotZero(t, element.GetFieldNumber())
		fieldNames = append(fieldNames, element.GetFieldName())
	}
	require.Equal(t, wantFieldNames, fieldNames)

	require.NotNil(t, badRequest)
	require.Len(t, badRequest.GetFieldViolations(), 1)
	require.Equal(t, wantPath, badRequest.GetFieldViolations()[0].GetField())
}

func TestCanonicalize_InvalidValueReturnsError(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("InvalidPhoneNumber", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.PhoneNumber = "not-a-phone"
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
		requireCanonicalizationViolation(t, err, []string{"author", "phone_number"}, "author.phone_number")
	})

	t.Run("InvalidPhoneInRepeatedField", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.PhoneNumbers = []string{"+14155550001", "invalid"}
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
		// The list subscript lands on the last element: phone_numbers[1].
		requireCanonicalizationViolation(t, err, []string{"author", "phone_numbers"}, "author.phone_numbers[1]")
	})
}

func TestCanonicalize_NonAnnotatedFieldUnchanged(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := validAuthor()
	author.DisplayName = "UPPERCASE DisplayName"
	author.Biography = "MiXeD CaSe Biography"
	createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
		Parent: organizationParent,
		Author: author,
	}
	createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
	require.NoError(t, err)
	require.Equal(t, "UPPERCASE DisplayName", createdAuthor.DisplayName)
	require.Equal(t, "MiXeD CaSe Biography", createdAuthor.Biography)
}

func TestCanonicalize_Persisted(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := validAuthor()
	author.EmailAddress = "PERSIST@EXAMPLE.COM"
	author.PhoneNumber = "(415) 555-0199"
	createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
		Parent: organizationParent,
		Author: author,
	}
	createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
	require.NoError(t, err)

	getAuthorRequest := &libraryservicepb.GetAuthorRequest{Name: createdAuthor.Name}
	gotAuthor, err := libraryServiceClient.GetAuthor(ctx, getAuthorRequest)
	require.NoError(t, err)
	require.Equal(t, "persist@example.com", gotAuthor.EmailAddress)
	require.Equal(t, "+14155550199", gotAuthor.PhoneNumber)
}

func TestCanonicalize_SkippedOnCreateBook(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Canon Skip Author")
	shelf := createTestShelf(t, organizationParent, "Canon Skip Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	createBookRequest := &libraryservicepb.CreateBookRequest{
		Parent: shelf.Name,
		Book: &librarypb.Book{
			Title:    "Canon Skip Book",
			Author:   author.Name,
			Duration: durationpb.New(100 * time.Second),
			Metadata: &librarypb.BookMetadata{
				PhoneNumber: "(415) 555-0001",
			},
		},
	}
	createdBook, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
	require.NoError(t, err)
	require.Equal(t, "(415) 555-0001", createdBook.Metadata.PhoneNumber)

	updateBookRequest := &libraryservicepb.UpdateBookRequest{
		Book: &librarypb.Book{
			Name: createdBook.Name,
			Metadata: &librarypb.BookMetadata{
				PhoneNumber: "(202) 555-9876",
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"metadata.phone_number"}},
	}
	updatedBook, err := libraryServiceClient.UpdateBook(ctx, updateBookRequest)
	require.NoError(t, err)
	require.Equal(t, "+12025559876", updatedBook.Metadata.PhoneNumber)
}
