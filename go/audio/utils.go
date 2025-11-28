package audio

import (
	"encoding/binary"
	"math"
	"time"
)

func convertPCMBytesToFloat32(data []byte, bitsPerSample int32) []float32 {
	bytesPerSample := int(bitsPerSample) / 8
	sampleCount := len(data) / bytesPerSample
	float32Data := make([]float32, sampleCount)

	for i := 0; i < sampleCount; i++ {
		offset := i * bytesPerSample
		if offset+1 >= len(data) {
			break
		}

		// Read 16-bit little-endian sample
		sample := int16(binary.LittleEndian.Uint16(data[offset : offset+2]))

		// Convert to float32 normalized to [-1.0, 1.0]
		float32Data[i] = float32(sample) / 32768.0
	}

	return float32Data
}

// Helper method to calculate RMS
func calculateRMS(data []float32) float32 {
	if len(data) == 0 {
		return 0
	}

	var sumSquares float64
	for _, sample := range data {
		sumSquares += float64(sample * sample)
	}

	return float32(math.Sqrt(sumSquares / float64(len(data))))
}

// durationToBytes converts an audio duration to bytes using saturated arithmetic.
func durationToBytes(d time.Duration, sampleRate, channels, bitsPerSample int32) int {
	if d <= 0 || sampleRate <= 0 || channels <= 0 || bitsPerSample <= 0 {
		return 0
	}
	bytesPerSample := bitsPerSample / 8
	if bytesPerSample <= 0 {
		bytesPerSample = 2
	}
	// totalBytes = duration(ns) * sampleRate * channels * bytesPerSample / 1e9
	nsec := d.Nanoseconds()
	total := (nsec * int64(sampleRate) * int64(channels) * int64(bytesPerSample)) / 1_000_000_000

	const maxInt = int(^uint(0) >> 1)
	if total < 0 {
		return 0
	}
	if total > int64(maxInt) {
		return maxInt
	}
	return int(total)
}
