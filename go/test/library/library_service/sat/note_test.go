package sat

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
	"github.com/malonaz/core/go/aip"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/uuid"
)

func createTestNote(t *testing.T, parent, displayName string) *librarypb.Note {
	t.Helper()
	createNoteRequest := &libraryservicepb.CreateNoteRequest{
		Parent: parent,
		Note: &librarypb.Note{
			DisplayName: displayName,
			Content:     "Test note content.",
			Labels:      map[string]string{"kind": "test"},
		},
	}
	note, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
	require.NoError(t, err)
	return note
}

func getNote(t *testing.T, name string) *librarypb.Note {
	t.Helper()
	getNoteRequest := &libraryservicepb.GetNoteRequest{Name: name}
	note, err := libraryServiceClient.GetNote(ctx, getNoteRequest)
	require.NoError(t, err)
	return note
}

func updateNote(t *testing.T, note *librarypb.Note, paths []string) *librarypb.Note {
	t.Helper()
	updateNoteRequest := &libraryservicepb.UpdateNoteRequest{
		Note:       note,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: paths},
	}
	updatedNote, err := libraryServiceClient.UpdateNote(ctx, updateNoteRequest)
	require.NoError(t, err)
	return updatedNote
}

func listNotes(t *testing.T, parent, filter string) []*librarypb.Note {
	t.Helper()
	listNotesRequest := &libraryservicepb.ListNotesRequest{
		Parent: parent,
		Filter: filter,
	}
	listNotesResponse, err := libraryServiceClient.ListNotes(ctx, listNotesRequest)
	require.NoError(t, err)
	return listNotesResponse.Notes
}

// ===================== Create =====================

func TestNoteCreate_UnderAuthor(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Note Author Parent")

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		before := time.Now().UTC()
		createNoteRequest := &libraryservicepb.CreateNoteRequest{
			Parent: author.Name,
			Note: &librarypb.Note{
				DisplayName: "Author Note",
				Content:     "Written under an author.",
				Labels:      map[string]string{"source": "author"},
			},
		}
		createdNote, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		after := time.Now().UTC()

		require.NoError(t, err)
		require.True(t, strings.HasPrefix(createdNote.Name, author.Name+"/notes/"))
		require.Equal(t, "Author Note", createdNote.DisplayName)
		require.Equal(t, "Written under an author.", createdNote.Content)
		require.Equal(t, "author", createdNote.Labels["source"])
		require.NotEmpty(t, createdNote.Etag)
		require.Nil(t, createdNote.DeleteTime)

		createTime := createdNote.CreateTime.AsTime()
		require.True(t, !createTime.Before(before))
		require.True(t, !createTime.After(after))
		require.Equal(t, createdNote.CreateTime.AsTime(), createdNote.UpdateTime.AsTime())
	})

	t.Run("GetMatchesCreateResponse", func(t *testing.T) {
		t.Parallel()
		createdNote := createTestNote(t, author.Name, "Author Note GetMatch")
		gotNote := getNote(t, createdNote.Name)
		grpcrequire.Equal(t, createdNote, gotNote)
	})

	t.Run("WithCustomID", func(t *testing.T) {
		t.Parallel()
		createNoteRequest := &libraryservicepb.CreateNoteRequest{
			Parent: author.Name,
			NoteId: "custom-author-note-id",
			Note:   &librarypb.Note{DisplayName: "Custom ID Author Note"},
		}
		createdNote, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		require.NoError(t, err)
		require.Equal(t, author.Name+"/notes/custom-author-note-id", createdNote.Name)
	})

	t.Run("DuplicateCustomID_AlreadyExists", func(t *testing.T) {
		t.Parallel()
		noteID := "dup-" + uuid.MustNewV7().String()[:8]
		createNoteRequest := &libraryservicepb.CreateNoteRequest{
			Parent: author.Name,
			NoteId: noteID,
			Note:   &librarypb.Note{DisplayName: "Dup Note"},
		}
		_, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		require.NoError(t, err)
		_, err = libraryServiceClient.CreateNote(ctx, createNoteRequest)
		grpcrequire.Error(t, codes.AlreadyExists, err)
	})
}

func TestNoteCreate_UnderShelf(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	shelf := createTestShelf(t, organizationParent, "Note Shelf Parent", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		createNoteRequest := &libraryservicepb.CreateNoteRequest{
			Parent: shelf.Name,
			Note: &librarypb.Note{
				DisplayName: "Shelf Note",
				Content:     "Written under a shelf.",
			},
		}
		createdNote, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(createdNote.Name, shelf.Name+"/notes/"))
	})

	t.Run("GetMatchesCreateResponse", func(t *testing.T) {
		t.Parallel()
		createdNote := createTestNote(t, shelf.Name, "Shelf Note GetMatch")
		gotNote := getNote(t, createdNote.Name)
		grpcrequire.Equal(t, createdNote, gotNote)
	})

	t.Run("WithCustomID", func(t *testing.T) {
		t.Parallel()
		createNoteRequest := &libraryservicepb.CreateNoteRequest{
			Parent: shelf.Name,
			NoteId: "custom-shelf-note-id",
			Note:   &librarypb.Note{DisplayName: "Custom ID Shelf Note"},
		}
		createdNote, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		require.NoError(t, err)
		require.Equal(t, shelf.Name+"/notes/custom-shelf-note-id", createdNote.Name)
	})
}

func TestNoteCreate_UnderOrganization(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		createNoteRequest := &libraryservicepb.CreateNoteRequest{
			Parent: organizationParent,
			Note:   &librarypb.Note{DisplayName: "Organization Note"},
		}
		createdNote, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(createdNote.Name, organizationParent+"/notes/"))

		gotNote := getNote(t, createdNote.Name)
		grpcrequire.Equal(t, createdNote, gotNote)
	})

	t.Run("WithCustomID", func(t *testing.T) {
		t.Parallel()
		createNoteRequest := &libraryservicepb.CreateNoteRequest{
			Parent: organizationParent,
			NoteId: "custom-org-note-id",
			Note:   &librarypb.Note{DisplayName: "Custom ID Org Note"},
		}
		createdNote, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		require.NoError(t, err)
		require.Equal(t, organizationParent+"/notes/custom-org-note-id", createdNote.Name)
	})

	t.Run("UpdatePreservesOrganizationPatternName", func(t *testing.T) {
		t.Parallel()
		note := createTestNote(t, organizationParent, "Org Note Update")
		updated := updateNote(t, &librarypb.Note{Name: note.Name, Content: "changed"}, []string{"content"})
		require.Equal(t, note.Name, updated.Name)
	})

	t.Run("SoftDelete", func(t *testing.T) {
		t.Parallel()
		note := createTestNote(t, organizationParent, "Org Note Delete")
		deleteNoteRequest := &libraryservicepb.DeleteNoteRequest{Name: note.Name}
		deletedNote, err := libraryServiceClient.DeleteNote(ctx, deleteNoteRequest)
		require.NoError(t, err)
		require.NotNil(t, deletedNote.DeleteTime)
	})

	t.Run("OrganizationParentListsAllNotesInOrganization", func(t *testing.T) {
		t.Parallel()
		scopedOrganizationParent := getOrganizationParent()
		author := createTestAuthor(t, scopedOrganizationParent, "Org List Author")
		shelf := createTestShelf(t, scopedOrganizationParent, "Org List Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		orgNote := createTestNote(t, scopedOrganizationParent, "Org Scoped Note")
		authorNote := createTestNote(t, author.Name, "Author Scoped Note")
		shelfNote := createTestNote(t, shelf.Name, "Shelf Scoped Note")

		// The organization pattern carries no author/shelf discriminator, so
		// nothing is filtered on them: all notes in the organization are listed.
		results := listNotes(t, scopedOrganizationParent, "")
		nameSet := map[string]bool{}
		for _, note := range results {
			nameSet[note.Name] = true
		}
		require.True(t, nameSet[orgNote.Name])
		require.True(t, nameSet[authorNote.Name])
		require.True(t, nameSet[shelfNote.Name])
	})
}

func TestNoteCreate_InvalidParents(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Note Invalid Parent Author")
	shelf := createTestShelf(t, organizationParent, "Note Invalid Parent Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	book := createTestBook(t, shelf.Name, author.Name, "Note Invalid Parent Book")

	tests := []struct {
		name   string
		parent string
	}{
		{"Book", book.Name},
		{"Garbage", "not-a-resource-name"},
		{"NoteItself", author.Name + "/notes/some-note"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			createNoteRequest := &libraryservicepb.CreateNoteRequest{
				Parent: tc.parent,
				Note:   &librarypb.Note{DisplayName: "Bad Parent Note"},
			}
			_, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
			grpcrequire.Error(t, codes.InvalidArgument, err)
		})
	}

	t.Run("WildcardParent", func(t *testing.T) {
		t.Parallel()
		createNoteRequest := &libraryservicepb.CreateNoteRequest{
			Parent: organizationParent + "/authors/-",
			Note:   &librarypb.Note{DisplayName: "Wildcard Parent Note"},
		}
		_, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestNoteCreate_Protovalidation(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Note Protoval Author")

	t.Run("MissingParent", func(t *testing.T) {
		t.Parallel()
		createNoteRequest := &libraryservicepb.CreateNoteRequest{
			Note: &librarypb.Note{DisplayName: "No Parent"},
		}
		_, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("MissingNote", func(t *testing.T) {
		t.Parallel()
		createNoteRequest := &libraryservicepb.CreateNoteRequest{Parent: author.Name}
		_, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("MissingDisplayName", func(t *testing.T) {
		t.Parallel()
		createNoteRequest := &libraryservicepb.CreateNoteRequest{
			Parent: author.Name,
			Note:   &librarypb.Note{Content: "no display name"},
		}
		_, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("ContentTooLong", func(t *testing.T) {
		t.Parallel()
		createNoteRequest := &libraryservicepb.CreateNoteRequest{
			Parent: author.Name,
			Note: &librarypb.Note{
				DisplayName: "Long Content Note",
				Content:     strings.Repeat("x", 4097),
			},
		}
		_, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("InvalidNoteID", func(t *testing.T) {
		t.Parallel()
		createNoteRequest := &libraryservicepb.CreateNoteRequest{
			Parent: author.Name,
			NoteId: "INVALID_UPPERCASE",
			Note:   &librarypb.Note{DisplayName: "Bad ID Note"},
		}
		_, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("InvalidRequestID", func(t *testing.T) {
		t.Parallel()
		createNoteRequest := &libraryservicepb.CreateNoteRequest{
			Parent:    author.Name,
			RequestId: "not-a-uuid",
			Note:      &librarypb.Note{DisplayName: "Bad RequestID Note"},
		}
		_, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestNoteCreate_ValidateOnly(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Note ValidateOnly Author")

	noteID := "validate-only-" + uuid.MustNewV7().String()[:8]
	createNoteRequest := &libraryservicepb.CreateNoteRequest{
		Parent:       author.Name,
		NoteId:       noteID,
		ValidateOnly: true,
		Note:         &librarypb.Note{DisplayName: "ValidateOnly Note"},
	}
	previewNote, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
	require.NoError(t, err)
	require.Equal(t, author.Name+"/notes/"+noteID, previewNote.Name)

	getNoteRequest := &libraryservicepb.GetNoteRequest{Name: previewNote.Name}
	_, err = libraryServiceClient.GetNote(ctx, getNoteRequest)
	grpcrequire.Error(t, codes.NotFound, err)
}

func TestNoteCreate_RequestIdempotency(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Note Idempotent Author")
	shelf := createTestShelf(t, organizationParent, "Note Idempotent Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	t.Run("SameRequestID_UnderAuthor", func(t *testing.T) {
		t.Parallel()
		createNoteRequest := &libraryservicepb.CreateNoteRequest{
			Parent:    author.Name,
			RequestId: uuid.MustNewV7().String(),
			Note:      &librarypb.Note{DisplayName: "Idempotent Author Note"},
		}
		firstNote, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		require.NoError(t, err)
		secondNote, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		require.NoError(t, err)
		grpcrequire.Equal(t, firstNote, secondNote)
	})

	t.Run("SameRequestID_UnderShelf", func(t *testing.T) {
		t.Parallel()
		createNoteRequest := &libraryservicepb.CreateNoteRequest{
			Parent:    shelf.Name,
			RequestId: uuid.MustNewV7().String(),
			Note:      &librarypb.Note{DisplayName: "Idempotent Shelf Note"},
		}
		firstNote, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		require.NoError(t, err)
		secondNote, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		require.NoError(t, err)
		grpcrequire.Equal(t, firstNote, secondNote)
	})

	t.Run("SameRequestID_UnderOrganization", func(t *testing.T) {
		t.Parallel()
		createNoteRequest := &libraryservicepb.CreateNoteRequest{
			Parent:    organizationParent,
			RequestId: uuid.MustNewV7().String(),
			Note:      &librarypb.Note{DisplayName: "Idempotent Org Note"},
		}
		firstNote, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		require.NoError(t, err)
		secondNote, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		require.NoError(t, err)
		grpcrequire.Equal(t, firstNote, secondNote)
	})

	t.Run("DifferentRequestID_NotIdempotent", func(t *testing.T) {
		t.Parallel()
		createNoteRequest := &libraryservicepb.CreateNoteRequest{
			Parent:    author.Name,
			RequestId: uuid.MustNewV7().String(),
			Note:      &librarypb.Note{DisplayName: "Non Idempotent Note"},
		}
		firstNote, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		require.NoError(t, err)

		createNoteRequest.RequestId = uuid.MustNewV7().String()
		secondNote, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		require.NoError(t, err)
		require.NotEqual(t, firstNote.Name, secondNote.Name)
	})
}

// ===================== Get =====================

func TestNoteGet(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Note Get Author")
	shelf := createTestShelf(t, organizationParent, "Note Get Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	t.Run("UnderAuthor", func(t *testing.T) {
		t.Parallel()
		note := createTestNote(t, author.Name, "Get Author Note")
		gotNote := getNote(t, note.Name)
		grpcrequire.Equal(t, note, gotNote)
	})

	t.Run("UnderShelf", func(t *testing.T) {
		t.Parallel()
		note := createTestNote(t, shelf.Name, "Get Shelf Note")
		gotNote := getNote(t, note.Name)
		grpcrequire.Equal(t, note, gotNote)
	})

	t.Run("UnderOrganization", func(t *testing.T) {
		t.Parallel()
		note := createTestNote(t, organizationParent, "Get Org Note")
		gotNote := getNote(t, note.Name)
		grpcrequire.Equal(t, note, gotNote)
	})

	t.Run("NotFound_AuthorPattern", func(t *testing.T) {
		t.Parallel()
		getNoteRequest := &libraryservicepb.GetNoteRequest{Name: author.Name + "/notes/nonexistent-note"}
		_, err := libraryServiceClient.GetNote(ctx, getNoteRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("NotFound_ShelfPattern", func(t *testing.T) {
		t.Parallel()
		getNoteRequest := &libraryservicepb.GetNoteRequest{Name: shelf.Name + "/notes/nonexistent-note"}
		_, err := libraryServiceClient.GetNote(ctx, getNoteRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("NotFound_OrganizationPattern", func(t *testing.T) {
		t.Parallel()
		getNoteRequest := &libraryservicepb.GetNoteRequest{Name: organizationParent + "/notes/nonexistent-note"}
		_, err := libraryServiceClient.GetNote(ctx, getNoteRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("CrossParentLookupFails", func(t *testing.T) {
		t.Parallel()
		// A note created under an author must not resolve under a shelf or the
		// organization name.
		note := createTestNote(t, author.Name, "Cross Parent Note")
		noteID := note.Name[strings.LastIndex(note.Name, "/")+1:]

		getNoteRequest := &libraryservicepb.GetNoteRequest{Name: shelf.Name + "/notes/" + noteID}
		_, err := libraryServiceClient.GetNote(ctx, getNoteRequest)
		grpcrequire.Error(t, codes.NotFound, err)

		getNoteRequest = &libraryservicepb.GetNoteRequest{Name: organizationParent + "/notes/" + noteID}
		_, err = libraryServiceClient.GetNote(ctx, getNoteRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("Wildcard_Rejected", func(t *testing.T) {
		t.Parallel()
		getNoteRequest := &libraryservicepb.GetNoteRequest{Name: author.Name + "/notes/-"}
		_, err := libraryServiceClient.GetNote(ctx, getNoteRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("InvalidPatternName", func(t *testing.T) {
		t.Parallel()
		getNoteRequest := &libraryservicepb.GetNoteRequest{Name: organizationParent + "/books/x/notes/y"}
		_, err := libraryServiceClient.GetNote(ctx, getNoteRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_MissingName", func(t *testing.T) {
		t.Parallel()
		_, err := libraryServiceClient.GetNote(ctx, &libraryservicepb.GetNoteRequest{})
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

// ===================== Update =====================

func TestNoteUpdate(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Note Update Author")
	shelf := createTestShelf(t, organizationParent, "Note Update Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	t.Run("AllowedFields_UnderAuthor", func(t *testing.T) {
		t.Parallel()
		original := createTestNote(t, author.Name, "Update Author Note")
		updated := updateNote(t, &librarypb.Note{
			Name:        original.Name,
			DisplayName: "Updated Author Note",
			Content:     "Updated content.",
			Labels:      map[string]string{"env": "prod"},
		}, []string{"display_name", "content", "labels"})
		require.Equal(t, "Updated Author Note", updated.DisplayName)
		require.Equal(t, "Updated content.", updated.Content)
		require.Equal(t, "prod", updated.Labels["env"])

		gotNote := getNote(t, original.Name)
		grpcrequire.Equal(t, updated, gotNote)
	})

	t.Run("AllowedFields_UnderShelf", func(t *testing.T) {
		t.Parallel()
		original := createTestNote(t, shelf.Name, "Update Shelf Note")
		updated := updateNote(t, &librarypb.Note{
			Name:    original.Name,
			Content: "Updated shelf note content.",
		}, []string{"content"})
		require.Equal(t, "Updated shelf note content.", updated.Content)
		require.Equal(t, original.DisplayName, updated.DisplayName)

		gotNote := getNote(t, original.Name)
		grpcrequire.Equal(t, updated, gotNote)
	})

	t.Run("NamePreservedAcrossUpdate", func(t *testing.T) {
		t.Parallel()
		original := createTestNote(t, author.Name, "Name Preserved Note")
		updated := updateNote(t, &librarypb.Note{
			Name:    original.Name,
			Content: "changed",
		}, []string{"content"})
		// Name must round-trip through the author pattern, not flip to another pattern.
		require.Equal(t, original.Name, updated.Name)
		require.True(t, strings.HasPrefix(updated.Name, author.Name+"/notes/"))
	})

	t.Run("FieldPreservation", func(t *testing.T) {
		t.Parallel()
		original := createTestNote(t, author.Name, "Preserve Note")
		updated := updateNote(t, &librarypb.Note{
			Name:        original.Name,
			DisplayName: "Preserve Note Changed",
		}, []string{"display_name"})

		expected := proto.CloneOf(original)
		expected.DisplayName = "Preserve Note Changed"
		grpcrequire.Equal(t, expected, updated,
			protocmp.IgnoreFields((*librarypb.Note)(nil), "update_time", "etag"))
	})

	t.Run("EtagAndUpdateTimeChange", func(t *testing.T) {
		t.Parallel()
		original := createTestNote(t, author.Name, "Etag Change Note")
		updated := updateNote(t, &librarypb.Note{
			Name:    original.Name,
			Content: "etag test",
		}, []string{"content"})
		require.NotEqual(t, original.Etag, updated.Etag)
		require.True(t, !updated.UpdateTime.AsTime().Before(original.UpdateTime.AsTime()))
		require.Equal(t, original.CreateTime.AsTime(), updated.CreateTime.AsTime())
	})

	t.Run("EtagMatch", func(t *testing.T) {
		t.Parallel()
		original := createTestNote(t, shelf.Name, "Etag Match Note")
		updateNoteRequest := &libraryservicepb.UpdateNoteRequest{
			Note: &librarypb.Note{
				Name:    original.Name,
				Content: "with etag",
				Etag:    original.Etag,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"content"}},
		}
		_, err := libraryServiceClient.UpdateNote(ctx, updateNoteRequest)
		require.NoError(t, err)
	})

	t.Run("StaleEtag_Aborted", func(t *testing.T) {
		t.Parallel()
		original := createTestNote(t, shelf.Name, "Etag Stale Note")
		staleEtag := original.Etag
		updateNote(t, &librarypb.Note{Name: original.Name, Content: "advance"}, []string{"content"})

		updateNoteRequest := &libraryservicepb.UpdateNoteRequest{
			Note: &librarypb.Note{
				Name:    original.Name,
				Content: "stale",
				Etag:    staleEtag,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"content"}},
		}
		_, err := libraryServiceClient.UpdateNote(ctx, updateNoteRequest)
		grpcrequire.Error(t, codes.Aborted, err)
	})

	t.Run("ClearFields", func(t *testing.T) {
		t.Parallel()
		original := createTestNote(t, author.Name, "Clear Fields Note")
		updated := updateNote(t, &librarypb.Note{
			Name:    original.Name,
			Content: "",
			Labels:  nil,
		}, []string{"content", "labels"})
		require.Empty(t, updated.Content)
		require.Empty(t, updated.Labels)
	})

	t.Run("UnauthorizedFields", func(t *testing.T) {
		t.Parallel()
		original := createTestNote(t, author.Name, "Unauthorized Note")
		for _, path := range []string{"name", "create_time", "delete_time", "etag"} {
			updateNoteRequest := &libraryservicepb.UpdateNoteRequest{
				Note:       &librarypb.Note{Name: original.Name},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{path}},
			}
			_, err := libraryServiceClient.UpdateNote(ctx, updateNoteRequest)
			grpcrequire.Error(t, codes.InvalidArgument, err)
		}
	})

	t.Run("Protovalidation_MissingName", func(t *testing.T) {
		t.Parallel()
		updateNoteRequest := &libraryservicepb.UpdateNoteRequest{
			Note:       &librarypb.Note{Content: "no name"},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"content"}},
		}
		_, err := libraryServiceClient.UpdateNote(ctx, updateNoteRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_EmptyMask", func(t *testing.T) {
		t.Parallel()
		original := createTestNote(t, author.Name, "Empty Mask Note")
		updateNoteRequest := &libraryservicepb.UpdateNoteRequest{
			Note:       &librarypb.Note{Name: original.Name},
			UpdateMask: &fieldmaskpb.FieldMask{},
		}
		_, err := libraryServiceClient.UpdateNote(ctx, updateNoteRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()
		updateNoteRequest := &libraryservicepb.UpdateNoteRequest{
			Note: &librarypb.Note{
				Name:    author.Name + "/notes/nonexistent-update",
				Content: "ghost",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"content"}},
		}
		_, err := libraryServiceClient.UpdateNote(ctx, updateNoteRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("SoftDeleted_NotFound", func(t *testing.T) {
		t.Parallel()
		note := createTestNote(t, author.Name, "Update Deleted Note")
		deleteNoteRequest := &libraryservicepb.DeleteNoteRequest{Name: note.Name}
		_, err := libraryServiceClient.DeleteNote(ctx, deleteNoteRequest)
		require.NoError(t, err)

		updateNoteRequest := &libraryservicepb.UpdateNoteRequest{
			Note:       &librarypb.Note{Name: note.Name, Content: "should fail"},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"content"}},
		}
		_, err = libraryServiceClient.UpdateNote(ctx, updateNoteRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})
}

// ===================== Delete (soft) =====================

func TestNoteDelete(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Note Delete Author")
	shelf := createTestShelf(t, organizationParent, "Note Delete Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	t.Run("SoftDelete_UnderAuthor", func(t *testing.T) {
		t.Parallel()
		note := createTestNote(t, author.Name, "SoftDel Author Note")

		before := time.Now().UTC()
		deleteNoteRequest := &libraryservicepb.DeleteNoteRequest{Name: note.Name}
		deletedNote, err := libraryServiceClient.DeleteNote(ctx, deleteNoteRequest)
		after := time.Now().UTC()

		require.NoError(t, err)
		require.NotNil(t, deletedNote.DeleteTime)
		deleteTime := deletedNote.DeleteTime.AsTime()
		require.True(t, !deleteTime.Before(before))
		require.True(t, !deleteTime.After(after))

		gotNote := getNote(t, note.Name)
		require.NotNil(t, gotNote.DeleteTime)
		require.Equal(t, note.DisplayName, gotNote.DisplayName)
	})

	t.Run("SoftDelete_UnderShelf", func(t *testing.T) {
		t.Parallel()
		note := createTestNote(t, shelf.Name, "SoftDel Shelf Note")
		deleteNoteRequest := &libraryservicepb.DeleteNoteRequest{Name: note.Name}
		deletedNote, err := libraryServiceClient.DeleteNote(ctx, deleteNoteRequest)
		require.NoError(t, err)
		require.NotNil(t, deletedNote.DeleteTime)
	})

	t.Run("HiddenFromList_ShowDeletedReveals", func(t *testing.T) {
		t.Parallel()
		note := createTestNote(t, author.Name, "SoftDel Hidden Note 4471")
		deleteNoteRequest := &libraryservicepb.DeleteNoteRequest{Name: note.Name}
		_, err := libraryServiceClient.DeleteNote(ctx, deleteNoteRequest)
		require.NoError(t, err)

		results := listNotes(t, author.Name, `display_name = "SoftDel Hidden Note 4471"`)
		require.Empty(t, results)

		listNotesRequest := &libraryservicepb.ListNotesRequest{
			Parent:      author.Name,
			Filter:      `display_name = "SoftDel Hidden Note 4471"`,
			ShowDeleted: true,
		}
		listNotesResponse, err := libraryServiceClient.ListNotes(ctx, listNotesRequest)
		require.NoError(t, err)
		require.Len(t, listNotesResponse.Notes, 1)
		require.NotNil(t, listNotesResponse.Notes[0].DeleteTime)
	})

	t.Run("DoubleDelete_NotFound_ThenAllowMissing", func(t *testing.T) {
		t.Parallel()
		note := createTestNote(t, shelf.Name, "SoftDel Twice Note")
		deleteNoteRequest := &libraryservicepb.DeleteNoteRequest{Name: note.Name}
		_, err := libraryServiceClient.DeleteNote(ctx, deleteNoteRequest)
		require.NoError(t, err)
		_, err = libraryServiceClient.DeleteNote(ctx, deleteNoteRequest)
		grpcrequire.Error(t, codes.NotFound, err)

		deleteNoteRequest = &libraryservicepb.DeleteNoteRequest{Name: note.Name, AllowMissing: true}
		_, err = libraryServiceClient.DeleteNote(ctx, deleteNoteRequest)
		require.NoError(t, err)
	})

	t.Run("AllowMissing_NeverExisted_NotFound", func(t *testing.T) {
		t.Parallel()
		deleteNoteRequest := &libraryservicepb.DeleteNoteRequest{
			Name:         author.Name + "/notes/never-existed",
			AllowMissing: true,
		}
		_, err := libraryServiceClient.DeleteNote(ctx, deleteNoteRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("EtagMatch", func(t *testing.T) {
		t.Parallel()
		note := createTestNote(t, author.Name, "SoftDel Etag Note")
		deleteNoteRequest := &libraryservicepb.DeleteNoteRequest{Name: note.Name, Etag: note.Etag}
		_, err := libraryServiceClient.DeleteNote(ctx, deleteNoteRequest)
		require.NoError(t, err)
	})

	t.Run("EtagMismatch_Aborted", func(t *testing.T) {
		t.Parallel()
		note := createTestNote(t, author.Name, "SoftDel BadEtag Note")
		deleteNoteRequest := &libraryservicepb.DeleteNoteRequest{Name: note.Name, Etag: `"wrong-etag"`}
		_, err := libraryServiceClient.DeleteNote(ctx, deleteNoteRequest)
		grpcrequire.Error(t, codes.Aborted, err)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()
		deleteNoteRequest := &libraryservicepb.DeleteNoteRequest{Name: shelf.Name + "/notes/nonexistent-del"}
		_, err := libraryServiceClient.DeleteNote(ctx, deleteNoteRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})
}

// ===================== List =====================

func TestNoteList_ParentIsolation(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Note List Author")
	shelf := createTestShelf(t, organizationParent, "Note List Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	book := createTestBook(t, shelf.Name, author.Name, "Note List Book")

	authorNote := createTestNote(t, author.Name, "Isolation Author Note")
	shelfNote := createTestNote(t, shelf.Name, "Isolation Shelf Note")
	orgNote := createTestNote(t, organizationParent, "Isolation Org Note")

	t.Run("AuthorParentOnlySeesAuthorNotes", func(t *testing.T) {
		t.Parallel()
		results := listNotes(t, author.Name, "")
		require.Len(t, results, 1)
		require.Equal(t, authorNote.Name, results[0].Name)
	})

	t.Run("ShelfParentOnlySeesShelfNotes", func(t *testing.T) {
		t.Parallel()
		results := listNotes(t, shelf.Name, "")
		require.Len(t, results, 1)
		require.Equal(t, shelfNote.Name, results[0].Name)
	})

	t.Run("OtherAuthorSeesNothing", func(t *testing.T) {
		t.Parallel()
		otherAuthor := createTestAuthor(t, organizationParent, "Note List Other Author")
		results := listNotes(t, otherAuthor.Name, "")
		require.Empty(t, results)
	})

	t.Run("WildcardAuthorParent_SpansParentsWithinOrganization", func(t *testing.T) {
		t.Parallel()
		// The unmatched pattern's identifier is not filtered on, so a wildcard
		// author parent lists every note in the organization.
		results := listNotes(t, organizationParent+"/authors/-", "")
		nameSet := map[string]bool{}
		for _, note := range results {
			nameSet[note.Name] = true
		}
		require.True(t, nameSet[authorNote.Name])
		require.True(t, nameSet[shelfNote.Name])
	})

	t.Run("ReturnedNamesMatchParentPattern", func(t *testing.T) {
		t.Parallel()
		results := listNotes(t, author.Name, "")
		for _, note := range results {
			require.True(t, strings.HasPrefix(note.Name, author.Name+"/notes/"))
		}
		results = listNotes(t, shelf.Name, "")
		for _, note := range results {
			require.True(t, strings.HasPrefix(note.Name, shelf.Name+"/notes/"))
		}
	})

	t.Run("WildcardParent_OtherOrganizationExcluded", func(t *testing.T) {
		t.Parallel()
		otherOrganizationParent := getOrganizationParent()
		otherAuthor := createTestAuthor(t, otherOrganizationParent, "Note Other Org Author")
		otherNote := createTestNote(t, otherAuthor.Name, "Other Org Note")

		results := listNotes(t, organizationParent+"/authors/-", "")
		for _, note := range results {
			require.NotEqual(t, otherNote.Name, note.Name)
		}
	})

	t.Run("OrganizationNoteExcludedFromAuthorAndShelf", func(t *testing.T) {
		t.Parallel()
		authorResults := listNotes(t, author.Name, "")
		for _, note := range authorResults {
			require.NotEqual(t, orgNote.Name, note.Name)
		}
		shelfResults := listNotes(t, shelf.Name, "")
		for _, note := range shelfResults {
			require.NotEqual(t, orgNote.Name, note.Name)
		}
	})

	t.Run("InvalidParent", func(t *testing.T) {
		t.Parallel()
		listNotesRequest := &libraryservicepb.ListNotesRequest{Parent: book.Name}
		_, err := libraryServiceClient.ListNotes(ctx, listNotesRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_MissingParent", func(t *testing.T) {
		t.Parallel()
		_, err := libraryServiceClient.ListNotes(ctx, &libraryservicepb.ListNotesRequest{})
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestNoteList_FiltersAndOrdering(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Note Filter Author")

	createNote := func(displayName, content string, labels map[string]string) *librarypb.Note {
		t.Helper()
		createNoteRequest := &libraryservicepb.CreateNoteRequest{
			Parent: author.Name,
			Note:   &librarypb.Note{DisplayName: displayName, Content: content, Labels: labels},
		}
		note, err := libraryServiceClient.CreateNote(ctx, createNoteRequest)
		require.NoError(t, err)
		return note
	}

	noteA := createNote("AAA Filter Note", "alpha content", map[string]string{"env": "prod"})
	noteB := createNote("BBB Filter Note", "beta content", map[string]string{"env": "staging"})
	noteC := createNote("CCC Filter Note", "", nil)

	t.Run("FilterByDisplayName", func(t *testing.T) {
		t.Parallel()
		results := listNotes(t, author.Name, `display_name = "AAA Filter Note"`)
		require.Len(t, results, 1)
		require.Equal(t, noteA.Name, results[0].Name)
	})

	t.Run("FilterWildcard", func(t *testing.T) {
		t.Parallel()
		results := listNotes(t, author.Name, `display_name = "*Filter Note"`)
		require.Len(t, results, 3)
	})

	t.Run("FilterByContentWildcard", func(t *testing.T) {
		t.Parallel()
		results := listNotes(t, author.Name, `content = "alpha*"`)
		require.Len(t, results, 1)
		require.Equal(t, noteA.Name, results[0].Name)
	})

	t.Run("FilterContentPresence", func(t *testing.T) {
		t.Parallel()
		results := listNotes(t, author.Name, `NOT content:*`)
		require.Len(t, results, 1)
		require.Equal(t, noteC.Name, results[0].Name)
	})

	t.Run("FilterByLabelKeyValue", func(t *testing.T) {
		t.Parallel()
		results := listNotes(t, author.Name, `labels.env = "prod"`)
		require.Len(t, results, 1)
		require.Equal(t, noteA.Name, results[0].Name)
	})

	t.Run("FilterByLabelHasKey", func(t *testing.T) {
		t.Parallel()
		results := listNotes(t, author.Name, `labels:"env"`)
		require.Len(t, results, 2)
	})

	t.Run("FilterOR", func(t *testing.T) {
		t.Parallel()
		results := listNotes(t, author.Name, `display_name = "AAA Filter Note" OR display_name = "BBB Filter Note"`)
		require.Len(t, results, 2)
	})

	t.Run("OrderByDisplayNameAsc", func(t *testing.T) {
		t.Parallel()
		listNotesRequest := &libraryservicepb.ListNotesRequest{Parent: author.Name, OrderBy: "display_name asc"}
		listNotesResponse, err := libraryServiceClient.ListNotes(ctx, listNotesRequest)
		require.NoError(t, err)
		require.Len(t, listNotesResponse.Notes, 3)
		require.Equal(t, noteA.Name, listNotesResponse.Notes[0].Name)
		require.Equal(t, noteB.Name, listNotesResponse.Notes[1].Name)
		require.Equal(t, noteC.Name, listNotesResponse.Notes[2].Name)
	})

	t.Run("DefaultOrdering_CreateTimeDesc", func(t *testing.T) {
		t.Parallel()
		listNotesRequest := &libraryservicepb.ListNotesRequest{Parent: author.Name}
		listNotesResponse, err := libraryServiceClient.ListNotes(ctx, listNotesRequest)
		require.NoError(t, err)
		require.Len(t, listNotesResponse.Notes, 3)
		require.Equal(t, noteC.Name, listNotesResponse.Notes[0].Name)
	})

	t.Run("OrderByNotAllowed_Content", func(t *testing.T) {
		t.Parallel()
		listNotesRequest := &libraryservicepb.ListNotesRequest{Parent: author.Name, OrderBy: "content asc"}
		_, err := libraryServiceClient.ListNotes(ctx, listNotesRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("FilterInvalidSyntax", func(t *testing.T) {
		t.Parallel()
		listNotesRequest := &libraryservicepb.ListNotesRequest{Parent: author.Name, Filter: `display_name =`}
		_, err := libraryServiceClient.ListNotes(ctx, listNotesRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_PageSizeTooLarge", func(t *testing.T) {
		t.Parallel()
		listNotesRequest := &libraryservicepb.ListNotesRequest{Parent: author.Name, PageSize: 1001}
		_, err := libraryServiceClient.ListNotes(ctx, listNotesRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestNoteList_Pagination(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	shelf := createTestShelf(t, organizationParent, "Note Pagination Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	for i := range 5 {
		createTestNote(t, shelf.Name, fmt.Sprintf("Paginated Note %02d", i))
	}

	t.Run("ManualPagination", func(t *testing.T) {
		t.Parallel()
		var allNotes []*librarypb.Note
		pageToken := ""
		for {
			listNotesRequest := &libraryservicepb.ListNotesRequest{
				Parent:    shelf.Name,
				PageSize:  2,
				PageToken: pageToken,
			}
			listNotesResponse, err := libraryServiceClient.ListNotes(ctx, listNotesRequest)
			require.NoError(t, err)
			allNotes = append(allNotes, listNotesResponse.Notes...)
			if listNotesResponse.NextPageToken == "" {
				break
			}
			pageToken = listNotesResponse.NextPageToken
		}
		require.Len(t, allNotes, 5)
	})

	t.Run("AIPPaginate", func(t *testing.T) {
		t.Parallel()
		listNotesRequest := &libraryservicepb.ListNotesRequest{
			Parent:   shelf.Name,
			PageSize: 2,
			OrderBy:  "create_time asc",
		}
		notes, err := aip.Paginate[*librarypb.Note](ctx, listNotesRequest, libraryServiceClient.ListNotes)
		require.NoError(t, err)
		require.Len(t, notes, 5)
	})
}

// ===================== BatchGet =====================

func TestNoteBatchGet(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Note BatchGet Author")
	shelf := createTestShelf(t, organizationParent, "Note BatchGet Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	authorNote1 := createTestNote(t, author.Name, "Batch Author Note 1")
	authorNote2 := createTestNote(t, author.Name, "Batch Author Note 2")
	shelfNote := createTestNote(t, shelf.Name, "Batch Shelf Note")
	orgNote := createTestNote(t, organizationParent, "Batch Org Note")

	t.Run("UnderAuthorParent", func(t *testing.T) {
		t.Parallel()
		batchGetNotesRequest := &libraryservicepb.BatchGetNotesRequest{
			Parent: author.Name,
			Names:  []string{authorNote1.Name, authorNote2.Name},
		}
		batchGetNotesResponse, err := libraryServiceClient.BatchGetNotes(ctx, batchGetNotesRequest)
		require.NoError(t, err)
		require.Len(t, batchGetNotesResponse.Notes, 2)
	})

	t.Run("MixedParents_EmptyParent", func(t *testing.T) {
		t.Parallel()
		batchGetNotesRequest := &libraryservicepb.BatchGetNotesRequest{
			Names: []string{authorNote1.Name, shelfNote.Name, orgNote.Name},
		}
		batchGetNotesResponse, err := libraryServiceClient.BatchGetNotes(ctx, batchGetNotesRequest)
		require.NoError(t, err)
		require.Len(t, batchGetNotesResponse.Notes, 3)
		require.Equal(t, authorNote1.Name, batchGetNotesResponse.Notes[0].Name)
		require.Equal(t, shelfNote.Name, batchGetNotesResponse.Notes[1].Name)
		require.Equal(t, orgNote.Name, batchGetNotesResponse.Notes[2].Name)
	})

	t.Run("PreservesOrder", func(t *testing.T) {
		t.Parallel()
		batchGetNotesRequest := &libraryservicepb.BatchGetNotesRequest{
			Names: []string{shelfNote.Name, authorNote2.Name, authorNote1.Name},
		}
		batchGetNotesResponse, err := libraryServiceClient.BatchGetNotes(ctx, batchGetNotesRequest)
		require.NoError(t, err)
		require.Len(t, batchGetNotesResponse.Notes, 3)
		require.Equal(t, shelfNote.Name, batchGetNotesResponse.Notes[0].Name)
		require.Equal(t, authorNote2.Name, batchGetNotesResponse.Notes[1].Name)
		require.Equal(t, authorNote1.Name, batchGetNotesResponse.Notes[2].Name)
	})

	t.Run("MatchesIndividualGet", func(t *testing.T) {
		t.Parallel()
		batchGetNotesRequest := &libraryservicepb.BatchGetNotesRequest{
			Names: []string{authorNote1.Name, shelfNote.Name},
		}
		batchGetNotesResponse, err := libraryServiceClient.BatchGetNotes(ctx, batchGetNotesRequest)
		require.NoError(t, err)
		grpcrequire.Equal(t, getNote(t, authorNote1.Name), batchGetNotesResponse.Notes[0])
		grpcrequire.Equal(t, getNote(t, shelfNote.Name), batchGetNotesResponse.Notes[1])
	})

	t.Run("ParentMismatch", func(t *testing.T) {
		t.Parallel()
		batchGetNotesRequest := &libraryservicepb.BatchGetNotesRequest{
			Parent: author.Name,
			Names:  []string{shelfNote.Name},
		}
		_, err := libraryServiceClient.BatchGetNotes(ctx, batchGetNotesRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()
		batchGetNotesRequest := &libraryservicepb.BatchGetNotesRequest{
			Names: []string{authorNote1.Name, author.Name + "/notes/nonexistent-batch"},
		}
		_, err := libraryServiceClient.BatchGetNotes(ctx, batchGetNotesRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("SoftDeletedReturned", func(t *testing.T) {
		t.Parallel()
		deletedNote := createTestNote(t, author.Name, "Batch Deleted Note")
		deleteNoteRequest := &libraryservicepb.DeleteNoteRequest{Name: deletedNote.Name}
		_, err := libraryServiceClient.DeleteNote(ctx, deleteNoteRequest)
		require.NoError(t, err)

		batchGetNotesRequest := &libraryservicepb.BatchGetNotesRequest{
			Names: []string{deletedNote.Name},
		}
		batchGetNotesResponse, err := libraryServiceClient.BatchGetNotes(ctx, batchGetNotesRequest)
		require.NoError(t, err)
		require.Len(t, batchGetNotesResponse.Notes, 1)
		require.NotNil(t, batchGetNotesResponse.Notes[0].DeleteTime)
	})

	t.Run("WildcardName_Rejected", func(t *testing.T) {
		t.Parallel()
		batchGetNotesRequest := &libraryservicepb.BatchGetNotesRequest{
			Names: []string{author.Name + "/notes/-"},
		}
		_, err := libraryServiceClient.BatchGetNotes(ctx, batchGetNotesRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_EmptyNames", func(t *testing.T) {
		t.Parallel()
		batchGetNotesRequest := &libraryservicepb.BatchGetNotesRequest{Names: []string{}}
		_, err := libraryServiceClient.BatchGetNotes(ctx, batchGetNotesRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_DuplicateNames", func(t *testing.T) {
		t.Parallel()
		batchGetNotesRequest := &libraryservicepb.BatchGetNotesRequest{
			Names: []string{authorNote1.Name, authorNote1.Name},
		}
		_, err := libraryServiceClient.BatchGetNotes(ctx, batchGetNotesRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}
