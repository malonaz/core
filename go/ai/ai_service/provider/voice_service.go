package provider

import (
	"context"
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc"
)

// Implements the voice service.
type VoiceService struct {
	voicesSorted   []*aipb.Voice
	voiceIdToVoice map[string]*aipb.Voice
}

func NewVoiceService() (*VoiceService, error) {
	return &VoiceService{
		voiceIdToVoice: map[string]*aipb.Voice{},
	}, nil
}

func (s *VoiceService) CreateVoice(ctx context.Context, request *aiservicepb.CreateVoiceRequest) (*aipb.Voice, error) {
	defer func() {
		slices.SortFunc(s.voicesSorted, func(a, b *aipb.Voice) int {
			if a.Name < b.Name {
				return -1
			}
			if a.Name > b.Name {
				return 1
			}
			return 0
		})
	}()

	voiceRn := &aipb.VoiceResourceName{Voice: request.VoiceId}
	if err := voiceRn.Validate(); err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "invalid voice_id: %v", err).Err()
	}
	request.Voice.Name = voiceRn.String()
	s.voiceIdToVoice[voiceRn.Voice] = request.Voice
	s.voicesSorted = append(s.voicesSorted, request.Voice)
	return request.Voice, nil
}

func (s *VoiceService) GetVoice(ctx context.Context, request *aiservicepb.GetVoiceRequest) (*aipb.Voice, error) {
	voiceRn := &aipb.VoiceResourceName{}
	if err := voiceRn.UnmarshalString(request.Name); err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "unmarshaling voice name: %v", err).Err()
	}
	voice, ok := s.voiceIdToVoice[voiceRn.Voice]
	if !ok {
		return nil, grpc.Errorf(codes.NotFound, "unknown voice %s ", voiceRn.Voice).Err()
	}
	return voice, nil
}

var listVoicesRequestParser = aip.MustNewPaginationRequestParser[*aiservicepb.ListVoicesRequest]()

func (s *VoiceService) ListVoices(ctx context.Context, request *aiservicepb.ListVoicesRequest) (*aiservicepb.ListVoicesResponse, error) {
	// Step 1: Parse the pagination request.
	parsed, err := listVoicesRequestParser.Parse(request)
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, err.Error()).Err()
	}
	offset := int(parsed.GetOffset())
	pageSize := int(request.GetPageSize())

	// Step 3: Determine which voices to paginate over.
	voicesToPage := make([]*aipb.Voice, 0, len(s.voicesSorted))
	for _, voice := range s.voicesSorted {
		voicesToPage = append(voicesToPage, proto.Clone(voice).(*aipb.Voice))
	}

	// Step 5: Apply pagination.
	response := &aiservicepb.ListVoicesResponse{}

	// Handle offset beyond available items.
	if offset >= len(voicesToPage) {
		return response, nil
	}

	// Calculate end index - fetch one extra item to determine if there's a next page.
	end := offset + pageSize + 1
	if end > len(voicesToPage) {
		end = len(voicesToPage)
	}

	// Slice the voices (including potential extra item).
	fetchedVoices := voicesToPage[offset:end]
	numFetched := len(fetchedVoices)

	// Return only pageSize items (not the extra one).
	if numFetched > pageSize {
		response.Voices = fetchedVoices[:pageSize]
	} else {
		response.Voices = fetchedVoices
	}

	// Pass the number of items actually fetched (including extra) to determine next page token.
	response.NextPageToken = parsed.GetNextPageToken(numFetched)

	return response, nil
}
