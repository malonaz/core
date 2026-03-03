package aip

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	libraryservicepb "github.com/malonaz/core/genproto/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/library/v1"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
)

func TestUpdateRequestParser_NewParser(t *testing.T) {
	t.Run("Author", func(t *testing.T) {
		parser, err := NewUpdateRequestParser[*libraryservicepb.UpdateAuthorRequest, *librarypb.Author]()
		require.NoError(t, err)
		require.NotNil(t, parser)
	})

	t.Run("Book", func(t *testing.T) {
		parser, err := NewUpdateRequestParser[*libraryservicepb.UpdateBookRequest, *librarypb.Book]()
		require.NoError(t, err)
		require.NotNil(t, parser)
	})

	t.Run("Shelf", func(t *testing.T) {
		parser, err := NewUpdateRequestParser[*libraryservicepb.UpdateShelfRequest, *librarypb.Shelf]()
		require.NoError(t, err)
		require.NotNil(t, parser)
	})
}

func TestUpdateRequestParser_AuthorizedPaths(t *testing.T) {
	parser := MustNewUpdateRequestParser[*libraryservicepb.UpdateAuthorRequest, *librarypb.Author]()

	tests := []struct {
		name           string
		fieldMaskPaths []string
		wantColumns    []string
		wantErr        bool
	}{
		{
			name:           "single authorized field",
			fieldMaskPaths: []string{"display_name"},
			wantColumns:    []string{"display_name"},
		},
		{
			name:           "multiple authorized fields",
			fieldMaskPaths: []string{"display_name", "biography"},
			wantColumns:    []string{"biography", "display_name"},
		},
		{
			name:           "all simple authorized fields",
			fieldMaskPaths: []string{"biography", "display_name", "email_address", "phone_number"},
			wantColumns:    []string{"biography", "display_name", "email_address", "phone_number"},
		},
		{
			name:           "labels field (JSON bytes)",
			fieldMaskPaths: []string{"labels"},
			wantColumns:    []string{"labels"},
		},
		{
			name:           "unauthorized - name (IDENTIFIER)",
			fieldMaskPaths: []string{"name"},
			wantErr:        true,
		},
		{
			name:           "unauthorized - create_time (OUTPUT_ONLY)",
			fieldMaskPaths: []string{"create_time"},
			wantErr:        true,
		},
		{
			name:           "unauthorized - delete_time",
			fieldMaskPaths: []string{"delete_time"},
			wantErr:        true,
		},
		{
			name:           "mix of authorized and unauthorized",
			fieldMaskPaths: []string{"display_name", "name"},
			wantErr:        true,
		},
		{
			name:           "field not in update paths - email_addresses",
			fieldMaskPaths: []string{"email_addresses"},
			wantErr:        true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
				Author:     &librarypb.Author{},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: tc.fieldMaskPaths},
			}
			parsedRequest, err := parser.Parse(updateAuthorRequest)

			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantColumns, parsedRequest.GetSQLColumns())
		})
	}
}

func TestUpdateRequestParser_WildcardPaths(t *testing.T) {
	parser := MustNewUpdateRequestParser[*libraryservicepb.UpdateAuthorRequest, *librarypb.Author]()

	tests := []struct {
		name           string
		fieldMaskPaths []string
		wantColumns    []string
		wantErr        bool
	}{
		{
			name:           "nested field via wildcard - single field",
			fieldMaskPaths: []string{"metadata.country"},
			wantColumns:    []string{"metadata"},
		},
		{
			name:           "nested field via wildcard - multiple fields same parent",
			fieldMaskPaths: []string{"metadata.country", "metadata.email_addresses"},
			wantColumns:    []string{"metadata"},
		},
		{
			name:           "full metadata object",
			fieldMaskPaths: []string{"metadata"},
			wantColumns:    []string{"metadata"},
		},
		{
			name:           "nested and simple fields combined",
			fieldMaskPaths: []string{"metadata.country", "display_name"},
			wantColumns:    []string{"metadata", "display_name"},
		},
		{
			name:           "all nested metadata fields",
			fieldMaskPaths: []string{"metadata.country", "metadata.email_addresses", "metadata.phone_numbers"},
			wantColumns:    []string{"metadata"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
				Author:     &librarypb.Author{},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: tc.fieldMaskPaths},
			}
			parsedRequest, err := parser.Parse(updateAuthorRequest)

			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.ElementsMatch(t, tc.wantColumns, parsedRequest.GetSQLColumns())
		})
	}
}

func TestUpdateRequestParser_AutoAuthorizedFields(t *testing.T) {
	parser := MustNewUpdateRequestParser[*libraryservicepb.UpdateAuthorRequest, *librarypb.Author]()

	tests := []struct {
		name           string
		fieldMaskPaths []string
		wantColumns    []string
	}{
		{
			name:           "update_time alone",
			fieldMaskPaths: []string{"update_time"},
			wantColumns:    []string{"update_time"},
		},
		{
			name:           "update_time with other fields",
			fieldMaskPaths: []string{"display_name", "update_time"},
			wantColumns:    []string{"display_name", "update_time"},
		},
		{
			name:           "etag alone",
			fieldMaskPaths: []string{"etag"},
			wantColumns:    []string{"etag"},
		},
		{
			name:           "etag with other fields",
			fieldMaskPaths: []string{"display_name", "etag"},
			wantColumns:    []string{"display_name", "etag"},
		},
		{
			name:           "both update_time and etag",
			fieldMaskPaths: []string{"update_time", "etag"},
			wantColumns:    []string{"update_time", "etag"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
				Author:     &librarypb.Author{},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: tc.fieldMaskPaths},
			}
			parsedRequest, err := parser.Parse(updateAuthorRequest)
			require.NoError(t, err)
			require.ElementsMatch(t, tc.wantColumns, parsedRequest.GetSQLColumns())
		})
	}
}

func TestUpdateRequestParser_SQLClauses(t *testing.T) {
	parser := MustNewUpdateRequestParser[*libraryservicepb.UpdateAuthorRequest, *librarypb.Author]()

	tests := []struct {
		name             string
		fieldMaskPaths   []string
		wantSQLColumns   []string
		wantUpsertClause string
		wantUpdateClause string
	}{
		{
			name:             "single field",
			fieldMaskPaths:   []string{"display_name"},
			wantSQLColumns:   []string{"display_name"},
			wantUpsertClause: "display_name = EXCLUDED.display_name",
			wantUpdateClause: "display_name = $1",
		},
		{
			name:             "multiple fields",
			fieldMaskPaths:   []string{"display_name", "biography"},
			wantSQLColumns:   []string{"biography", "display_name"},
			wantUpsertClause: "biography = EXCLUDED.biography, display_name = EXCLUDED.display_name",
			wantUpdateClause: "biography = $1, display_name = $2",
		},
		{
			name:             "nested via wildcard collapses to parent",
			fieldMaskPaths:   []string{"metadata.country", "metadata.email_addresses"},
			wantSQLColumns:   []string{"metadata"},
			wantUpsertClause: "metadata = EXCLUDED.metadata",
			wantUpdateClause: "metadata = $1",
		},
		{
			name:             "mixed nested and simple",
			fieldMaskPaths:   []string{"display_name", "metadata.country"},
			wantSQLColumns:   []string{"display_name", "metadata"},
			wantUpsertClause: "display_name = EXCLUDED.display_name, metadata = EXCLUDED.metadata",
			wantUpdateClause: "display_name = $1, metadata = $2",
		},
		{
			name:             "three fields",
			fieldMaskPaths:   []string{"display_name", "biography", "email_address"},
			wantSQLColumns:   []string{"biography", "display_name", "email_address"},
			wantUpsertClause: "biography = EXCLUDED.biography, display_name = EXCLUDED.display_name, email_address = EXCLUDED.email_address",
			wantUpdateClause: "biography = $1, display_name = $2, email_address = $3",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
				Author:     &librarypb.Author{},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: tc.fieldMaskPaths},
			}
			parsedRequest, err := parser.Parse(updateAuthorRequest)
			require.NoError(t, err)
			require.Equal(t, tc.wantSQLColumns, parsedRequest.GetSQLColumns())
			require.Equal(t, tc.wantUpsertClause, parsedRequest.GetSQLUpsertClause())
			require.Equal(t, tc.wantUpdateClause, parsedRequest.GetSQLUpdateClause())
		})
	}
}

func TestParsedUpdateRequest_ApplyFieldMask(t *testing.T) {
	t.Run("simple fields only", func(t *testing.T) {
		existingAuthor := &librarypb.Author{
			DisplayName:  "Original Name",
			Biography:    "Original Bio",
			EmailAddress: "original@test.com",
		}
		newAuthor := &librarypb.Author{
			DisplayName:  "Updated Name",
			Biography:    "Updated Bio",
			EmailAddress: "updated@test.com",
		}

		parsedRequest := &ParsedUpdateRequest{
			fieldMask: pbfieldmask.FromPaths("display_name"),
		}
		parsedRequest.ApplyFieldMask(existingAuthor, newAuthor)

		require.Equal(t, "Updated Name", existingAuthor.DisplayName)
		require.Equal(t, "Original Bio", existingAuthor.Biography)
		require.Equal(t, "original@test.com", existingAuthor.EmailAddress)
	})

	t.Run("entire nested message", func(t *testing.T) {
		existingAuthor := &librarypb.Author{
			DisplayName: "Original Name",
			Metadata: &librarypb.AuthorMetadata{
				Country:        "USA",
				EmailAddresses: []string{"old@test.com"},
			},
		}
		newAuthor := &librarypb.Author{
			DisplayName: "Updated Name",
			Metadata: &librarypb.AuthorMetadata{
				Country:        "UK",
				EmailAddresses: []string{"new@test.com"},
			},
		}

		parsedRequest := &ParsedUpdateRequest{
			fieldMask: pbfieldmask.FromPaths("metadata"),
		}
		parsedRequest.ApplyFieldMask(existingAuthor, newAuthor)

		require.Equal(t, "Original Name", existingAuthor.DisplayName)
		require.Equal(t, "UK", existingAuthor.Metadata.Country)
		require.Equal(t, []string{"new@test.com"}, existingAuthor.Metadata.EmailAddresses)
	})

	t.Run("specific nested field", func(t *testing.T) {
		existingAuthor := &librarypb.Author{
			Metadata: &librarypb.AuthorMetadata{
				Country:        "USA",
				EmailAddresses: []string{"old@test.com"},
			},
		}
		newAuthor := &librarypb.Author{
			Metadata: &librarypb.AuthorMetadata{
				Country:        "UK",
				EmailAddresses: []string{"new@test.com"},
			},
		}

		parsedRequest := &ParsedUpdateRequest{
			fieldMask: pbfieldmask.FromPaths("metadata.country"),
		}
		parsedRequest.ApplyFieldMask(existingAuthor, newAuthor)

		require.Equal(t, "UK", existingAuthor.Metadata.Country)
		require.Equal(t, []string{"old@test.com"}, existingAuthor.Metadata.EmailAddresses)
	})

	t.Run("multiple fields", func(t *testing.T) {
		existingAuthor := &librarypb.Author{
			DisplayName:  "Original Name",
			Biography:    "Original Bio",
			EmailAddress: "original@test.com",
			PhoneNumber:  "+1234567890",
		}
		newAuthor := &librarypb.Author{
			DisplayName:  "Updated Name",
			Biography:    "Updated Bio",
			EmailAddress: "updated@test.com",
			PhoneNumber:  "+0987654321",
		}

		parsedRequest := &ParsedUpdateRequest{
			fieldMask: pbfieldmask.FromPaths("display_name", "phone_number"),
		}
		parsedRequest.ApplyFieldMask(existingAuthor, newAuthor)

		require.Equal(t, "Updated Name", existingAuthor.DisplayName)
		require.Equal(t, "Original Bio", existingAuthor.Biography)
		require.Equal(t, "original@test.com", existingAuthor.EmailAddress)
		require.Equal(t, "+0987654321", existingAuthor.PhoneNumber)
	})

	t.Run("map field - labels", func(t *testing.T) {
		existingAuthor := &librarypb.Author{
			DisplayName: "Original Name",
			Labels:      map[string]string{"env": "prod", "team": "alpha"},
		}
		newAuthor := &librarypb.Author{
			DisplayName: "Updated Name",
			Labels:      map[string]string{"env": "staging", "version": "v2"},
		}

		parsedRequest := &ParsedUpdateRequest{
			fieldMask: pbfieldmask.FromPaths("labels"),
		}
		parsedRequest.ApplyFieldMask(existingAuthor, newAuthor)

		require.Equal(t, "Original Name", existingAuthor.DisplayName)
		require.Equal(t, map[string]string{"env": "staging", "version": "v2"}, existingAuthor.Labels)
	})
}

func TestUpdateRequestParser_EmptyFieldMask(t *testing.T) {
	parser := MustNewUpdateRequestParser[*libraryservicepb.UpdateAuthorRequest, *librarypb.Author]()

	updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
		Author:     &librarypb.Author{},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{}},
	}
	_, err := parser.Parse(updateAuthorRequest)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no mask paths")
}

func TestUpdateRequestParser_InvalidFieldMask(t *testing.T) {
	parser := MustNewUpdateRequestParser[*libraryservicepb.UpdateAuthorRequest, *librarypb.Author]()

	updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
		Author:     &librarypb.Author{},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"nonexistent_field"}},
	}
	_, err := parser.Parse(updateAuthorRequest)
	require.Error(t, err)
}

func TestUpdateRequestParser_BookPaths(t *testing.T) {
	parser := MustNewUpdateRequestParser[*libraryservicepb.UpdateBookRequest, *librarypb.Book]()

	tests := []struct {
		name           string
		fieldMaskPaths []string
		wantColumns    []string
		wantErr        bool
	}{
		{
			name:           "title field",
			fieldMaskPaths: []string{"title"},
			wantColumns:    []string{"title"},
		},
		{
			name:           "author reference field",
			fieldMaskPaths: []string{"author"},
			wantColumns:    []string{"author"},
		},
		{
			name:           "numeric fields",
			fieldMaskPaths: []string{"publication_year", "page_count"},
			wantColumns:    []string{"publication_year", "page_count"},
		},
		{
			name:           "metadata nested field",
			fieldMaskPaths: []string{"metadata.summary"},
			wantColumns:    []string{"metadata"},
		},
		{
			name:           "metadata multiple nested fields",
			fieldMaskPaths: []string{"metadata.summary", "metadata.language"},
			wantColumns:    []string{"metadata"},
		},
		{
			name:           "all book update paths",
			fieldMaskPaths: []string{"title", "author", "isbn", "publication_year", "page_count"},
			wantColumns:    []string{"title", "author", "isbn", "publication_year", "page_count"},
		},
		{
			name:           "labels not in book update paths",
			fieldMaskPaths: []string{"labels"},
			wantErr:        true,
		},
		{
			name:           "name unauthorized",
			fieldMaskPaths: []string{"name"},
			wantErr:        true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			updateBookRequest := &libraryservicepb.UpdateBookRequest{
				Book:       &librarypb.Book{},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: tc.fieldMaskPaths},
			}
			parsedRequest, err := parser.Parse(updateBookRequest)

			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.ElementsMatch(t, tc.wantColumns, parsedRequest.GetSQLColumns())
		})
	}
}

func TestUpdateRequestParser_ShelfPaths(t *testing.T) {
	parser := MustNewUpdateRequestParser[*libraryservicepb.UpdateShelfRequest, *librarypb.Shelf]()

	tests := []struct {
		name           string
		fieldMaskPaths []string
		wantColumns    []string
		wantErr        bool
	}{
		{
			name:           "display_name field",
			fieldMaskPaths: []string{"display_name"},
			wantColumns:    []string{"display_name"},
		},
		{
			name:           "genre enum field",
			fieldMaskPaths: []string{"genre"},
			wantColumns:    []string{"genre"},
		},
		{
			name:           "metadata nested field (with name change)",
			fieldMaskPaths: []string{"metadata.capacity"},
			wantColumns:    []string{"legacy_meta"},
		},
		{
			name:           "multiple authorized fields",
			fieldMaskPaths: []string{"display_name", "genre"},
			wantColumns:    []string{"display_name", "genre"},
		},
		{
			name:           "labels not in shelf update paths",
			fieldMaskPaths: []string{"labels"},
			wantErr:        true,
		},
		{
			name:           "delete_time unauthorized",
			fieldMaskPaths: []string{"delete_time"},
			wantErr:        true,
		},
		{
			name:           "mixed authorized and unauthorized",
			fieldMaskPaths: []string{"display_name", "genre", "metadata"},
			wantErr:        true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			updateShelfRequest := &libraryservicepb.UpdateShelfRequest{
				Shelf:      &librarypb.Shelf{},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: tc.fieldMaskPaths},
			}
			parsedRequest, err := parser.Parse(updateShelfRequest)

			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantColumns, parsedRequest.GetSQLColumns())
		})
	}
}

func TestUpdateRequestParser_ColumnNameReplacement(t *testing.T) {
	parser := MustNewUpdateRequestParser[*libraryservicepb.UpdateShelfRequest, *librarypb.Shelf]()

	tests := []struct {
		name             string
		fieldMaskPaths   []string
		wantColumns      []string
		wantUpsertClause string
		wantUpdateClause string
	}{
		{
			name:             "external_id uses ext_id column",
			fieldMaskPaths:   []string{"external_id"},
			wantColumns:      []string{"ext_id"},
			wantUpsertClause: "ext_id = EXCLUDED.ext_id",
			wantUpdateClause: "ext_id = $1",
		},
		{
			name:             "correlation_id_2 uses correlation_id column",
			fieldMaskPaths:   []string{"correlation_id_2"},
			wantColumns:      []string{"correlation_id"},
			wantUpsertClause: "correlation_id = EXCLUDED.correlation_id",
			wantUpdateClause: "correlation_id = $1",
		},
		{
			name:             "multiple renamed columns",
			fieldMaskPaths:   []string{"external_id", "correlation_id_2"},
			wantColumns:      []string{"correlation_id", "ext_id"},
			wantUpsertClause: "correlation_id = EXCLUDED.correlation_id, ext_id = EXCLUDED.ext_id",
			wantUpdateClause: "correlation_id = $1, ext_id = $2",
		},
		{
			name:             "mixed standard and renamed columns",
			fieldMaskPaths:   []string{"display_name", "external_id", "genre"},
			wantColumns:      []string{"display_name", "ext_id", "genre"},
			wantUpsertClause: "display_name = EXCLUDED.display_name, ext_id = EXCLUDED.ext_id, genre = EXCLUDED.genre",
			wantUpdateClause: "display_name = $1, ext_id = $2, genre = $3",
		},
		{
			name:             "nested field with parent column replacement",
			fieldMaskPaths:   []string{"metadata.capacity"},
			wantColumns:      []string{"legacy_meta"},
			wantUpsertClause: "legacy_meta = EXCLUDED.legacy_meta",
			wantUpdateClause: "legacy_meta = $1",
		},
		{
			name:             "all renamed columns together",
			fieldMaskPaths:   []string{"external_id", "correlation_id_2", "metadata.capacity"},
			wantColumns:      []string{"correlation_id", "ext_id", "legacy_meta"},
			wantUpsertClause: "correlation_id = EXCLUDED.correlation_id, ext_id = EXCLUDED.ext_id, legacy_meta = EXCLUDED.legacy_meta",
			wantUpdateClause: "correlation_id = $1, ext_id = $2, legacy_meta = $3",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			updateShelfRequest := &libraryservicepb.UpdateShelfRequest{
				Shelf:      &librarypb.Shelf{},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: tc.fieldMaskPaths},
			}
			parsedRequest, err := parser.Parse(updateShelfRequest)
			require.NoError(t, err)
			require.Equal(t, tc.wantColumns, parsedRequest.GetSQLColumns())
			require.Equal(t, tc.wantUpsertClause, parsedRequest.GetSQLUpsertClause())
			require.Equal(t, tc.wantUpdateClause, parsedRequest.GetSQLUpdateClause())
		})
	}
}

func TestUpdateRequestParser_MustNewPanics(t *testing.T) {
	require.NotPanics(t, func() {
		MustNewUpdateRequestParser[*libraryservicepb.UpdateAuthorRequest, *librarypb.Author]()
	})
	require.NotPanics(t, func() {
		MustNewUpdateRequestParser[*libraryservicepb.UpdateBookRequest, *librarypb.Book]()
	})
	require.NotPanics(t, func() {
		MustNewUpdateRequestParser[*libraryservicepb.UpdateShelfRequest, *librarypb.Shelf]()
	})
}
