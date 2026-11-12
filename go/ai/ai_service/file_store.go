package ai_service

import (
	"context"

	"google.golang.org/grpc/codes"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/grpc/status"
)

// FileStore keeps the content of image and document blocks out of the
// resources carrying them: a stored message or decision names a file, never
// holds its bytes. File names are opaque to the service, so a host backs them
// with files of its own.
type FileStore interface {
	// CreateFile stores content written under parent, the parent of a chat or
	// a decision, and returns the resource name of its file.
	CreateFile(ctx context.Context, parent, mediaType string, data []byte) (string, error)
	// GetFileURL returns the URL providers fetch a file named under parent
	// from. It must never change: every turn replays the history verbatim.
	GetFileURL(ctx context.Context, parent, file string) (string, error)
}

var hostFileStore FileStore

// SetFileStore sets the file store of the services built after it. A package
// variable because onyx builds a service from its manifest alone: a host
// calls it from its setup.
func SetFileStore(fileStore FileStore) {
	hostFileStore = fileStore
}

// storeFiles moves the inline content of image and document blocks into the
// file store: each block names its file from then on. Without a store,
// content stays inline.
func storeFiles(ctx context.Context, fileStore FileStore, parent string, blocks ...*aipb.Block) error {
	if fileStore == nil {
		return nil
	}
	for _, block := range blocks {
		switch content := block.GetContent().(type) {
		case *aipb.Block_Image:
			source, ok := content.Image.GetSource().(*aipb.Image_Data)
			if !ok {
				continue
			}
			file, err := fileStore.CreateFile(ctx, parent, content.Image.GetMediaType(), source.Data)
			if err != nil {
				return status.FromError(err, "storing image").Err()
			}
			content.Image.Source = &aipb.Image_File{File: file}

		case *aipb.Block_Document:
			source, ok := content.Document.GetSource().(*aipb.Document_Data)
			if !ok {
				continue
			}
			file, err := fileStore.CreateFile(ctx, parent, content.Document.GetMediaType(), source.Data)
			if err != nil {
				return status.FromError(err, "storing document").Err()
			}
			content.Document.Source = &aipb.Document_File{File: file}
		}
	}
	return nil
}

// resolveFiles replaces every file the blocks name by its URL, the form
// providers take. It rewrites the blocks it is given: a caller passes the
// provider-bound ones, and what is stored keeps naming its files.
func resolveFiles(ctx context.Context, fileStore FileStore, parent string, blocks ...*aipb.Block) error {
	for _, block := range blocks {
		switch content := block.GetContent().(type) {
		case *aipb.Block_Image:
			source, ok := content.Image.GetSource().(*aipb.Image_File)
			if !ok {
				continue
			}
			url, err := getFileURL(ctx, fileStore, parent, source.File)
			if err != nil {
				return err
			}
			content.Image.Source = &aipb.Image_Url{Url: url}

		case *aipb.Block_Document:
			source, ok := content.Document.GetSource().(*aipb.Document_File)
			if !ok {
				continue
			}
			url, err := getFileURL(ctx, fileStore, parent, source.File)
			if err != nil {
				return err
			}
			content.Document.Source = &aipb.Document_Url{Url: url}
		}
	}
	return nil
}

func getFileURL(ctx context.Context, fileStore FileStore, parent, file string) (string, error) {
	if fileStore == nil {
		return "", status.Errorf(codes.FailedPrecondition, "no file store to resolve %q", file).Err()
	}
	url, err := fileStore.GetFileURL(ctx, parent, file)
	if err != nil {
		return "", status.FromError(err, "resolving %q", file).Err()
	}
	return url, nil
}
