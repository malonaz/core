package ai_service

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	pb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/grpc/status"
)

const testFileParent = "organizations/o/users/u"

// fakeFileStore names files after their creation order and serves them from a fixed host.
type fakeFileStore struct {
	fileToData map[string][]byte
}

func newFakeFileStore() *fakeFileStore {
	return &fakeFileStore{fileToData: map[string][]byte{}}
}

func (f *fakeFileStore) CreateFile(_ context.Context, parent, mediaType string, data []byte) (string, error) {
	file := fmt.Sprintf("%s/files/%d", parent, len(f.fileToData))
	f.fileToData[file] = data
	return file, nil
}

func (f *fakeFileStore) GetFileURL(_ context.Context, _, file string) (string, error) {
	if _, ok := f.fileToData[file]; !ok {
		return "", status.Errorf(codes.NotFound, "file %q not found", file).Err()
	}
	return "https://files.test/" + file, nil
}

func TestStoreFiles(t *testing.T) {
	ctx := context.Background()

	t.Run("inline content moves into the store", func(t *testing.T) {
		fileStore := newFakeFileStore()
		imageBlock := ai.NewImageBlock(ai.NewImageFromData([]byte("png"), "image/png"))
		documentBlock := ai.NewDocumentBlock(ai.NewDocumentFromData([]byte("pdf"), "application/pdf"))
		require.NoError(t, storeFiles(ctx, fileStore, testFileParent, ai.NewTextBlock("hello"), imageBlock, documentBlock))

		image, document := imageBlock.GetImage(), documentBlock.GetDocument()
		require.Equal(t, []byte("png"), fileStore.fileToData[image.GetFile()])
		require.Equal(t, []byte("pdf"), fileStore.fileToData[document.GetFile()])
		require.Equal(t, "image/png", image.GetMediaType())
		require.Equal(t, "application/pdf", document.GetMediaType())
	})

	t.Run("url and file sources are left alone", func(t *testing.T) {
		fileStore := newFakeFileStore()
		urlBlock := ai.NewImageBlock(ai.NewImageFromURL("https://example.com/a.png"))
		fileBlock := ai.NewDocumentBlock(ai.NewDocumentFromFile("files/a", "application/pdf"))
		require.NoError(t, storeFiles(ctx, fileStore, testFileParent, urlBlock, fileBlock))
		require.Empty(t, fileStore.fileToData)
		require.Equal(t, "https://example.com/a.png", urlBlock.GetImage().GetUrl())
		require.Equal(t, "files/a", fileBlock.GetDocument().GetFile())
	})

	t.Run("without a store content stays inline", func(t *testing.T) {
		imageBlock := ai.NewImageBlock(ai.NewImageFromData([]byte("png"), "image/png"))
		require.NoError(t, storeFiles(ctx, nil, testFileParent, imageBlock))
		require.Equal(t, []byte("png"), imageBlock.GetImage().GetData())
	})
}

func TestResolveFiles(t *testing.T) {
	ctx := context.Background()

	t.Run("files become their urls", func(t *testing.T) {
		fileStore := newFakeFileStore()
		imageBlock := ai.NewImageBlock(ai.NewImageFromData([]byte("png"), "image/png"))
		documentBlock := ai.NewDocumentBlock(ai.NewDocumentFromData([]byte("pdf"), "application/pdf"))
		require.NoError(t, storeFiles(ctx, fileStore, testFileParent, imageBlock, documentBlock))
		imageFile, documentFile := imageBlock.GetImage().GetFile(), documentBlock.GetDocument().GetFile()

		require.NoError(t, resolveFiles(ctx, fileStore, testFileParent, ai.NewTextBlock("hello"), imageBlock, documentBlock))
		require.Equal(t, "https://files.test/"+imageFile, imageBlock.GetImage().GetUrl())
		require.Equal(t, "https://files.test/"+documentFile, documentBlock.GetDocument().GetUrl())
	})

	t.Run("a file the store refuses is an error", func(t *testing.T) {
		err := resolveFiles(ctx, newFakeFileStore(), testFileParent, ai.NewImageBlock(ai.NewImageFromFile("files/missing", "image/png")))
		require.True(t, status.HasCode(err, codes.NotFound), "got %v", err)
	})

	t.Run("a file cannot be resolved without a store", func(t *testing.T) {
		err := resolveFiles(ctx, nil, testFileParent, ai.NewImageBlock(ai.NewImageFromFile("files/a", "image/png")))
		require.True(t, status.HasCode(err, codes.FailedPrecondition), "got %v", err)
	})
}

func TestGenerateMessageWrapperSendStoresGeneratedContent(t *testing.T) {
	fileStore := newFakeFileStore()
	stream := &fakeStream{}
	wrapper := &generateMessageWrapper{
		AiService_StreamGenerateMessageServer: stream,
		messageAccumulator:                    ai.NewMessageAccumulator(),
		fileStore:                             fileStore,
		fileParent:                            testFileParent,
		toolCallIDToToolCall:                  map[string]*aipb.ToolCall{},
	}
	response := &pb.StreamGenerateMessageResponse{
		Content: &pb.StreamGenerateMessageResponse_Block{Block: ai.NewImageBlock(ai.NewImageFromData([]byte("png"), "image/png"))},
	}
	require.NoError(t, wrapper.Send(response))

	// Neither the stream nor the message to persist carries the bytes.
	require.Len(t, stream.responses, 1)
	streamedImage := stream.responses[0].GetBlock().GetImage()
	require.Equal(t, []byte("png"), fileStore.fileToData[streamedImage.GetFile()])
	accumulatedBlocks := wrapper.messageAccumulator.Message.GetBlocks()
	require.Len(t, accumulatedBlocks, 1)
	require.Equal(t, streamedImage.GetFile(), accumulatedBlocks[0].GetImage().GetFile())
}
