package sat

import (
	"testing"

	"github.com/stretchr/testify/require"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

// The shelf's uninventoried joins filter its books on the shelf's own
// inventory_time: every book until the first inventory, then those created
// strictly after it.
func TestCorrelatedFilter_ShelfInventory(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "CorrelatedFilter Author")

	// inventory records an inventory at the given book's creation, which
	// inventories that book and every earlier one.
	inventory := func(t *testing.T, shelf *librarypb.Shelf, book *librarypb.Book) *librarypb.Shelf {
		t.Helper()
		return updateShelf(t, &librarypb.Shelf{Name: shelf.Name, InventoryTime: book.CreateTime}, []string{"inventory_time"})
	}

	t.Run("NoBooks", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "CorrelatedFilter Empty Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		for _, got := range []*librarypb.Shelf{shelf, getShelf(t, shelf.Name)} {
			require.Zero(t, got.UninventoriedBookCount)
			require.Empty(t, got.OldestUninventoriedBook)
		}
	})

	t.Run("NeverInventoried_EveryBook", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "CorrelatedFilter Fresh Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		first := createTestBookWithPageCount(t, shelf.Name, author.Name, "CorrelatedFilter Fresh Book One", 10)
		createTestBookWithPageCount(t, shelf.Name, author.Name, "CorrelatedFilter Fresh Book Two", 10)

		gotShelf := getShelf(t, shelf.Name)
		require.EqualValues(t, 2, gotShelf.UninventoriedBookCount)
		require.Equal(t, first.Name, gotShelf.OldestUninventoriedBook)
	})

	// The update's RETURNING subqueries correlate on the updated row, so the
	// response already reflects the new inventory_time.
	t.Run("InventoryCountsOnlyLaterBooks", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "CorrelatedFilter Inventory Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		first := createTestBookWithPageCount(t, shelf.Name, author.Name, "CorrelatedFilter Inventory Book One", 10)
		second := createTestBookWithPageCount(t, shelf.Name, author.Name, "CorrelatedFilter Inventory Book Two", 10)

		updated := inventory(t, shelf, first)
		require.EqualValues(t, 1, updated.UninventoriedBookCount)
		require.Equal(t, second.Name, updated.OldestUninventoriedBook)

		updated = inventory(t, shelf, second)
		require.Zero(t, updated.UninventoriedBookCount)
		require.Empty(t, updated.OldestUninventoriedBook)

		third := createTestBookWithPageCount(t, shelf.Name, author.Name, "CorrelatedFilter Inventory Book Three", 10)
		gotShelf := getShelf(t, shelf.Name)
		require.EqualValues(t, 1, gotShelf.UninventoriedBookCount)
		require.Equal(t, third.Name, gotShelf.OldestUninventoriedBook)
	})

	t.Run("ClearingInventoryCountsEveryBookAgain", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "CorrelatedFilter Clear Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		first := createTestBookWithPageCount(t, shelf.Name, author.Name, "CorrelatedFilter Clear Book One", 10)
		second := createTestBookWithPageCount(t, shelf.Name, author.Name, "CorrelatedFilter Clear Book Two", 10)
		require.Zero(t, inventory(t, shelf, second).UninventoriedBookCount)

		cleared := updateShelf(t, &librarypb.Shelf{Name: shelf.Name}, []string{"inventory_time"})
		require.EqualValues(t, 2, cleared.UninventoriedBookCount)
		require.Equal(t, first.Name, cleared.OldestUninventoriedBook)
	})

	// Each shelf's subquery reads its own row: one List resolves them all.
	t.Run("List", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "CorrelatedFilter List Author")
		inventoried := createTestShelf(t, organizationParent, "CorrelatedFilter List Inventoried Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		inventory(t, inventoried, createTestBookWithPageCount(t, inventoried.Name, author.Name, "CorrelatedFilter List Inventoried Book", 10))
		pending := createTestShelf(t, organizationParent, "CorrelatedFilter List Pending Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		createTestBookWithPageCount(t, pending.Name, author.Name, "CorrelatedFilter List Pending Book One", 10)
		createTestBookWithPageCount(t, pending.Name, author.Name, "CorrelatedFilter List Pending Book Two", 10)

		listShelves := func(t *testing.T, filter, orderBy string) []string {
			t.Helper()
			response, err := libraryServiceClient.ListShelves(ctx, &libraryservicepb.ListShelvesRequest{
				Parent:  organizationParent,
				Filter:  filter,
				OrderBy: orderBy,
			})
			require.NoError(t, err)
			names := make([]string, len(response.Shelves))
			for i, shelf := range response.Shelves {
				names[i] = shelf.Name
			}
			return names
		}
		require.Equal(t, []string{pending.Name}, listShelves(t, "uninventoried_book_count > 0", ""))
		require.Equal(t, []string{pending.Name, inventoried.Name}, listShelves(t, "", "uninventoried_book_count desc"))
	})
}
