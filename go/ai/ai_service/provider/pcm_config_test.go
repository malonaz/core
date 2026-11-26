package provider_test

import (
	"errors"
	"testing"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	audiopb "github.com/malonaz/core/genproto/audio/v1"
)

func TestPCMOnlyConfigs(t *testing.T) {
	mp3 := &audiopb.Format{Encoding: audiopb.Encoding_ENCODING_MP3}
	pcm := &audiopb.Format{SampleRate: 16000, Channels: 1, BitsPerSample: 16}
	streamConfiguration := func(format *audiopb.Format) *aiservicepb.SpeechToTextStreamConfiguration {
		return &aiservicepb.SpeechToTextStreamConfiguration{Model: "providers/deepgram/models/flux-general-en", AudioFormat: format}
	}
	ttsConfig := func(format *audiopb.Format) *aipb.TtsModelConfig {
		return &aipb.TtsModelConfig{AudioFormat: format, SupportedSampleRates: []int32{16000}}
	}
	tests := []struct {
		name    string
		message proto.Message
		ruleID  string
	}{
		{"stream pcm", streamConfiguration(pcm), ""},
		{"stream mp3", streamConfiguration(mp3), "speech_to_text_stream_configuration.audio_format_pcm"},
		{"stream unset format", streamConfiguration(nil), "required"},
		{"tts pcm", ttsConfig(pcm), ""},
		{"tts mp3", ttsConfig(mp3), "tts_model_config.audio_format_pcm"},
		{"tts unset format", ttsConfig(nil), "required"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := protovalidate.Validate(test.message)
			if test.ruleID == "" {
				require.NoError(t, err)
				return
			}
			var validationError *protovalidate.ValidationError
			require.True(t, errors.As(err, &validationError))
			require.Len(t, validationError.Violations, 1)
			require.Equal(t, test.ruleID, validationError.Violations[0].Proto.GetRuleId())
		})
	}
}
