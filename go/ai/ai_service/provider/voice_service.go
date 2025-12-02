package provider

import (
	"context"

	"google.golang.org/grpc/codes"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/grpc"
)

// Implements the voice service.
type VoiceService struct {
	voiceIdToVoice map[string]*aipb.Voice
}

func NewVoiceService() (*VoiceService, error) {
	return &VoiceService{
		voiceIdToVoice: map[string]*aipb.Voice{},
	}, nil
}

func (s *VoiceService) CreateVoice(ctx context.Context, request *aiservicepb.CreateVoiceRequest) (*aipb.Voice, error) {
	voiceRn := &aipb.VoiceResourceName{Voice: request.VoiceId}
	if err := voiceRn.Validate(); err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "invalid voice_id: %v", err).Err()
	}
	request.Voice.Name = voiceRn.String()
	s.voiceIdToVoice[voiceRn.Voice] = request.Voice
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
