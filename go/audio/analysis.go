package audio

import (
	"encoding/binary"
	"fmt"
	"time"

	audiopb "github.com/malonaz/core/genproto/audio/v1"
)

func CalculateWAVDuration(bytes []byte) (time.Duration, error) {
	// WAV files need at least 44 bytes for the header
	if len(bytes) < 44 {
		return 0, fmt.Errorf("invalid WAV file: too short")
	}

	// Check if it's a valid WAV file
	if string(bytes[0:4]) != "RIFF" || string(bytes[8:12]) != "WAVE" {
		return 0, fmt.Errorf("not a valid WAV file")
	}

	// Extract audio format info from header
	// Byte 22-23: Number of Channels
	// Byte 24-27: Sample Rate (Hz)
	// Byte 34-35: Bits Per Sample
	// Byte 40-43: Data Size (bytes)

	channels := int32(binary.LittleEndian.Uint16(bytes[22:24]))
	sampleRate := int32(binary.LittleEndian.Uint32(bytes[24:28]))
	bitsPerSample := int32(binary.LittleEndian.Uint16(bytes[34:36]))
	dataSize := int(binary.LittleEndian.Uint32(bytes[40:44]))

	audioFormat := &audiopb.Format{
		SampleRate:    sampleRate,
		Channels:      channels,
		BitsPerSample: bitsPerSample,
	}
	return CalculatePCMDuration(audioFormat, dataSize)
}

// CalculatePCMDuration calculates the duration of audio data given its parameters.
// dataSize is the total size of audio data in bytes.
// sampleRate is the number of samples per second (Hz).
// channels is the number of audio channels (1 for mono, 2 for stereo).
// bitsPerSample is the number of bits per sample (e.g., 16 for PCM16).
func CalculatePCMDuration(audioFormat *audiopb.Format, dataSize int) (time.Duration, error) {
	if audioFormat.SampleRate == 0 {
		return 0, fmt.Errorf("invalid sample rate: cannot be zero")
	}
	if audioFormat.Channels == 0 {
		return 0, fmt.Errorf("invalid channels: cannot be zero")
	}
	if audioFormat.BitsPerSample == 0 {
		return 0, fmt.Errorf("invalid bits per sample: cannot be zero")
	}

	// Calculate byte rate: (sample rate * channels * bits per sample) / 8
	byteRate := (audioFormat.SampleRate * audioFormat.Channels * audioFormat.BitsPerSample) / 8

	// Calculate duration in seconds
	durationSeconds := float64(dataSize) / float64(byteRate)

	// Convert to time.Duration
	duration := time.Duration(durationSeconds * float64(time.Second))

	return duration, nil
}
