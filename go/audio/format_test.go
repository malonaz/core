package audio_test

import (
	"testing"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/require"

	audiopb "github.com/malonaz/core/genproto/audio/v1"
)

func TestFormatValidation(t *testing.T) {
	tests := []struct {
		name    string
		format  *audiopb.Format
		wantErr bool
	}{
		{"pcm", &audiopb.Format{SampleRate: 16000, Channels: 1, BitsPerSample: 16}, false},
		{"pcm without rate", &audiopb.Format{Channels: 1, BitsPerSample: 16}, true},
		{"pcm with three channels", &audiopb.Format{SampleRate: 16000, Channels: 3, BitsPerSample: 16}, true},
		{"mp3 without pcm parameters", &audiopb.Format{Encoding: audiopb.Encoding_ENCODING_MP3}, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := protovalidate.Validate(test.format)
			require.Equal(t, test.wantErr, err != nil)
		})
	}
}
