package audio_test

import (
	"errors"
	"testing"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/require"

	audiopb "github.com/malonaz/core/genproto/audio/v1"
)

func TestFormatValidation(t *testing.T) {
	tests := []struct {
		name   string
		format *audiopb.Format
		ruleID string
	}{
		{"pcm", &audiopb.Format{SampleRate: 16000, Channels: 1, BitsPerSample: 16}, ""},
		{"pcm without rate", &audiopb.Format{Channels: 1, BitsPerSample: 16}, "format.pcm_parameters"},
		{"pcm with three channels", &audiopb.Format{SampleRate: 16000, Channels: 3, BitsPerSample: 16}, "format.pcm_parameters"},
		{"mp3 without pcm parameters", &audiopb.Format{Encoding: audiopb.Encoding_ENCODING_MP3}, ""},
		{"mp3 with pcm parameters", &audiopb.Format{Encoding: audiopb.Encoding_ENCODING_MP3, SampleRate: 8000, Channels: 1, BitsPerSample: 16}, ""},
		{"undefined encoding", &audiopb.Format{Encoding: 99}, "enum.defined_only"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := protovalidate.Validate(test.format)
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
