// Package audio provides utilities for converting between PCM and μ-law audio formats.
package audio

import (
	"encoding/binary"
	"fmt"

	"github.com/AlexxIT/go2rtc/pkg/pcm"

	audiopb "github.com/malonaz/core/genproto/audio/v1"
)

// WAVToPCM16 extracts raw 16-bit PCM audio data from a WAV file.
// Returns the PCM data, sample rate, number of channels, and any error.
// The function validates the WAV header and ensures it contains 16-bit PCM audio.
func WAVToPCM16(wavData []byte) (pcmData []byte, sampleRate int32, numChannels int16, err error) {
	if len(wavData) < 44 {
		return nil, 0, 0, fmt.Errorf("invalid WAV file: too short (minimum 44 bytes required)")
	}

	// Validate RIFF header
	if string(wavData[0:4]) != "RIFF" {
		return nil, 0, 0, fmt.Errorf("invalid WAV file: missing RIFF header")
	}

	// Validate WAVE format
	if string(wavData[8:12]) != "WAVE" {
		return nil, 0, 0, fmt.Errorf("invalid WAV file: missing WAVE format")
	}

	// Validate fmt subchunk
	if string(wavData[12:16]) != "fmt " {
		return nil, 0, 0, fmt.Errorf("invalid WAV file: missing fmt subchunk")
	}

	// Read audio format (should be 1 for PCM)
	audioFormat := binary.LittleEndian.Uint16(wavData[20:22])
	if audioFormat != 1 {
		return nil, 0, 0, fmt.Errorf("unsupported audio format: %d (only PCM format 1 is supported)", audioFormat)
	}

	// Read number of channels
	numChannels = int16(binary.LittleEndian.Uint16(wavData[22:24]))

	// Read sample rate
	sampleRate = int32(binary.LittleEndian.Uint32(wavData[24:28]))

	// Read bits per sample
	bitsPerSample := binary.LittleEndian.Uint16(wavData[34:36])
	if bitsPerSample != 16 {
		return nil, 0, 0, fmt.Errorf("unsupported bits per sample: %d (only 16-bit PCM is supported)", bitsPerSample)
	}

	// Validate data subchunk
	if string(wavData[36:40]) != "data" {
		return nil, 0, 0, fmt.Errorf("invalid WAV file: missing data subchunk")
	}

	// Read data size
	dataSize := binary.LittleEndian.Uint32(wavData[40:44])

	// Validate data size
	if len(wavData) < 44+int(dataSize) {
		return nil, 0, 0, fmt.Errorf("invalid WAV file: data size mismatch")
	}

	// Extract PCM data
	pcmData = wavData[44 : 44+dataSize]

	return pcmData, sampleRate, numChannels, nil
}

// PCMToWAV converts raw PCM audio to WAV format
// pcmData: PCM samples in the format specified by audioFormat
// audioFormat: specifies sample rate, number of channels, and bits per sample
func PCMToWAV(pcmData []byte, audioFormat *audiopb.Format) []byte {
	dataSize := uint32(len(pcmData))
	byteRate := uint32(audioFormat.SampleRate) * uint32(audioFormat.Channels) * uint32(audioFormat.BitsPerSample/8)
	blockAlign := int16(audioFormat.Channels) * int16(audioFormat.BitsPerSample/8)

	header := make([]byte, 44)

	// RIFF header
	copy(header[0:4], "RIFF")
	binary.LittleEndian.PutUint32(header[4:8], dataSize+36) // File size - 8
	copy(header[8:12], "WAVE")

	// fmt subchunk
	copy(header[12:16], "fmt ")
	binary.LittleEndian.PutUint32(header[16:20], 16) // Subchunk size
	binary.LittleEndian.PutUint16(header[20:22], 1)  // Audio format (1 = PCM)
	binary.LittleEndian.PutUint16(header[22:24], uint16(audioFormat.Channels))
	binary.LittleEndian.PutUint32(header[24:28], uint32(audioFormat.SampleRate))
	binary.LittleEndian.PutUint32(header[28:32], byteRate)
	binary.LittleEndian.PutUint16(header[32:34], uint16(blockAlign))
	binary.LittleEndian.PutUint16(header[34:36], uint16(audioFormat.BitsPerSample))

	// data subchunk
	copy(header[36:40], "data")
	binary.LittleEndian.PutUint32(header[40:44], dataSize)

	// Combine header + PCM data
	return append(header, pcmData...)
}

// MulawToPCM converts μ-law encoded audio to 16-bit PCM (little-endian).
// Input: μ-law bytes (8-bit samples)
// Output: PCM bytes (16-bit samples, little-endian)
func Mulaw8ToPCM16(mulaw []byte) []byte {
	pcmBytes := make([]byte, len(mulaw)*2)

	for i, mulawByte := range mulaw {
		// Convert μ-law byte to PCM16
		pcm16 := pcm.PCMUtoPCM(mulawByte)

		// Write as little-endian 16-bit integer
		binary.LittleEndian.PutUint16(pcmBytes[i*2:], uint16(pcm16))
	}

	return pcmBytes
}

// PCMToMulaw converts 16-bit PCM to μ-law encoded audio.
// Input: PCM bytes (16-bit samples, little-endian)
// Output: μ-law bytes (8-bit samples)
func PCM16ToMulaw8(pcmBytes []byte) []byte {
	// PCM is 16-bit, so we have half as many samples
	sampleCount := len(pcmBytes) / 2
	mulawBytes := make([]byte, sampleCount)

	for i := 0; i < sampleCount; i++ {
		// Read little-endian 16-bit integer
		pcm16 := int16(binary.LittleEndian.Uint16(pcmBytes[i*2:]))

		// Convert PCM16 to μ-law byte
		mulawBytes[i] = pcm.PCMtoPCMU(pcm16)
	}

	return mulawBytes
}

// ResamplePCM16 resamples 16-bit PCM audio from one sample rate to another
// using simple linear interpolation (suitable for downsampling)
func ResamplePCM16(input []byte, fromRate, toRate int32) []byte {
	if fromRate == toRate {
		return input
	}

	// Convert bytes to int16 samples
	numInputSamples := len(input) / 2
	inputSamples := make([]int16, numInputSamples)
	for i := 0; i < numInputSamples; i++ {
		inputSamples[i] = int16(input[i*2]) | int16(input[i*2+1])<<8
	}

	// Calculate output size
	ratio := float64(toRate) / float64(fromRate)
	numOutputSamples := int(float64(numInputSamples) * ratio)
	outputSamples := make([]int16, numOutputSamples)

	// Simple decimation for downsampling (taking every Nth sample)
	// For 24kHz -> 8kHz, we take every 3rd sample
	step := float64(fromRate) / float64(toRate)
	for i := 0; i < numOutputSamples; i++ {
		srcIndex := int(float64(i) * step)
		if srcIndex < numInputSamples {
			outputSamples[i] = inputSamples[srcIndex]
		}
	}

	// Convert back to bytes
	output := make([]byte, numOutputSamples*2)
	for i, sample := range outputSamples {
		output[i*2] = byte(sample)
		output[i*2+1] = byte(sample >> 8)
	}

	return output
}
