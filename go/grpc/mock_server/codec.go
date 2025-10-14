package mockserver

// Codec is an instance of this codec.
type Codec struct{}

type frame struct {
	payload []byte
}

// Marshal returns the given byte-array wrapped inside a struct as is.
func (m Codec) Marshal(v any) ([]byte, error) {
	return v.(*frame).payload, nil
}

// Unmarshal returns a frame struct with the byte array wrapped within it.
func (m Codec) Unmarshal(b []byte, v any) error {
	dst, ok := v.(*frame)
	if !ok {
		return nil
	}
	dst.payload = b
	return nil
}

// String returns a printable name for this codec.
func (m Codec) String() string {
	return "bytes"
}
