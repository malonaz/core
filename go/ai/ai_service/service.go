package ai_service

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"

	pb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	audiopb "github.com/malonaz/core/genproto/audio/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/ai/ai_service/provider"
	"github.com/malonaz/core/go/ai/ai_service/provider/anthropic"
	"github.com/malonaz/core/go/ai/ai_service/provider/cartesia"
	"github.com/malonaz/core/go/ai/ai_service/provider/elevenlabs"
	"github.com/malonaz/core/go/ai/ai_service/provider/openai"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/grpcinproc"
)

type Opts struct {
	OpenAIApiKey     string `long:"openai-api-key"     env:"OPENAI_API_KEY" description:"Open AI api key"`
	GroqApiKey       string `long:"gro-api-key"     env:"GROQ_API_KEY" description:"Groq api key"`
	ElevenlabsApiKey string `long:"elevenlabs-api-key"     env:"ELEVENLABS_API_KEY" description:"Elevenlabs api key"`
	AnthropicApiKey  string `long:"anthropic-api-key"     env:"ANTHROPIC_API_KEY" description:"Anthropic api key"`
	CartesiaApiKey   string `long:"cartesia-api-key"     env:"CARTESIA_API_KEY" description:"Cartesia api key"`
}

type runtime struct {
	opts           *Opts
	cartesiaClient *cartesia.Client
}

func newRuntime(opts *Opts) (*runtime, error) {
	runtime := &runtime{opts: opts}
	var providers []provider.Provider
	if opts.OpenAIApiKey != "" {
		providers = append(providers, openai.NewClient(opts.OpenAIApiKey))
	}
	if opts.GroqApiKey != "" {
		providers = append(providers, openai.NewGroqClient(opts.GroqApiKey))
	}
	if opts.ElevenlabsApiKey != "" {
		providers = append(providers, elevenlabs.NewClient(opts.ElevenlabsApiKey))
	}
	if opts.AnthropicApiKey != "" {
		providers = append(providers, anthropic.NewClient(opts.AnthropicApiKey))
	}
	if opts.CartesiaApiKey != "" {
		runtime.cartesiaClient = cartesia.NewClient(opts.CartesiaApiKey)
		providers = append(providers, runtime.cartesiaClient)
	}

	if err := provider.RegisterProviders(providers...); err != nil {
		return nil, err
	}

	return runtime, nil
}

func (s *Service) start(ctx context.Context) (func(), error) {
	if err := s.cartesiaClient.Start(ctx); err != nil {
		return nil, fmt.Errorf("starting cartesia client: %v", err)
	}
	return func() {
		s.cartesiaClient.Stop()
	}, nil
}

// textToText forwards the request to the appropriate registered client.
func (s *Service) TextToText(ctx context.Context, request *pb.TextToTextRequest) (*pb.TextToTextResponse, error) {
	modelConfig, err := provider.GetModelConfig(request.Model)
	if err != nil {
		return nil, err
	}
	if modelConfig.ModelType != aipb.ModelType_MODEL_TYPE_TTT {
		return nil, fmt.Errorf("model %s is not of type TTT", request.Model)
	}
	if request.Configuration.GetReasoningEffort() != aipb.ReasoningEffort_REASONING_EFFORT_UNSPECIFIED && !modelConfig.GetTtt().GetReasoning() {
		return nil, fmt.Errorf("model %s does not support reasoning", request.Model)
	}
	if len(request.Tools) > 0 && !modelConfig.GetTtt().GetToolCall() {
		return nil, fmt.Errorf("model %s does not support tool calling", request.Model)
	}

	client, ok := provider.GetTextToTextClient(request.Model)
	if !ok {
		return nil, fmt.Errorf("no TextToText client registered for provider %s and model %s", modelConfig.Provider, request.Model)
	}
	response, err := client.TextToText(ctx, request)
	if err != nil {
		return nil, err
	}
	if request.GetConfiguration().GetExtractJsonObject() {
		jsonContent, err := ai.ExtractJSONObject(response.Message.Content)
		if err != nil {
			return nil, grpc.Errorf(codes.Internal, "extracting json object: %v", err).WithErrorInfo(
				"JSON_EXTRACTION_FAILED",
				"ai_service",
				map[string]string{"original_content": response.Message.Content},
			).Err()
		}
		response.Message.Content = jsonContent
	}
	return response, nil
}

// SpeechToText forwards the request to the appropriate registered client.
func (s *Service) SpeechToText(ctx context.Context, request *pb.SpeechToTextRequest) (*pb.SpeechToTextResponse, error) {
	modelConfig, err := provider.GetModelConfig(request.Model)
	if err != nil {
		return nil, err
	}
	if modelConfig.ModelType != aipb.ModelType_MODEL_TYPE_STT {
		return nil, fmt.Errorf("model %s is not of type STT", request.Model)
	}
	client, ok := provider.GetSpeechToTextClient(request.Model)
	if !ok {
		return nil, fmt.Errorf("no SpeechToText client registered for provider %s and model %s", modelConfig.Provider, request.Model)
	}
	return client.SpeechToText(ctx, request)
}

// TextToSpeechStream implements the gRPC streaming method - direct pass-through
func (s *Service) TextToSpeechStream(request *pb.TextToSpeechStreamRequest, srv pb.Ai_TextToSpeechStreamServer) error {
	modelConfig, err := provider.GetModelConfig(request.Model)
	if err != nil {
		return err
	}
	if modelConfig.ModelType != aipb.ModelType_MODEL_TYPE_TTS {
		return fmt.Errorf("model %s is not of type TTS", request.Model)
	}

	client, ok := provider.GetTextToSpeechClient(request.Model)
	if !ok {
		return fmt.Errorf("no TextToSpeech client registered for provider %s and model %s", modelConfig.Provider, request.Model)
	}

	// Direct pass-through - provider implements exact gRPC interface
	return client.TextToSpeechStream(request, srv)
}

// TextToSpeech collects all streamed audio chunks into a single response
func (s *Service) TextToSpeech(ctx context.Context, request *pb.TextToSpeechRequest) (*pb.TextToSpeechResponse, error) {
	modelConfig, err := provider.GetModelConfig(request.Model)
	if err != nil {
		return nil, err
	}
	if modelConfig.ModelType != aipb.ModelType_MODEL_TYPE_TTS {
		return nil, fmt.Errorf("model %s is not of type TTS", request.Model)
	}
	client, ok := provider.GetTextToSpeechClient(request.Model)
	if !ok {
		return nil, fmt.Errorf("no TextToSpeech client registered for provider %s and model %s", modelConfig.Provider, request.Model)
	}

	// Convert to streaming request
	streamRequest := &pb.TextToSpeechStreamRequest{
		Model: request.Model,
		Text:  request.Text,
		Voice: request.Voice,
	}

	// Create a local streaming client using grpcinproc
	serverStreamClient := grpcinproc.NewServerStreamAsClient[
		pb.TextToSpeechStreamRequest,
		pb.TextToSpeechStreamResponse,
		pb.Ai_TextToSpeechStreamServer,
	](client.TextToSpeechStream)

	stream, err := serverStreamClient(ctx, streamRequest)
	if err != nil {
		return nil, err
	}

	// Collect all chunks into a single response
	response := &pb.TextToSpeechResponse{
		AudioChunk: &audiopb.Chunk{},
	}

	for {
		event, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		switch content := event.GetContent().(type) {
		case *pb.TextToSpeechStreamResponse_AudioFormat:
			response.AudioFormat = content.AudioFormat

		case *pb.TextToSpeechStreamResponse_AudioChunk:
			if response.AudioFormat == nil {
				return nil, fmt.Errorf("received audio chunk before receiving audio format")
			}
			response.AudioChunk.Data = append(response.AudioChunk.Data, content.AudioChunk.Data...)

		case *pb.TextToSpeechStreamResponse_ModelUsage:
			response.ModelUsage = content.ModelUsage

		case *pb.TextToSpeechStreamResponse_GenerationMetrics:
			content.GenerationMetrics.Ttfb = nil
			response.GenerationMetrics = content.GenerationMetrics
		}
	}

	return response, nil
}
