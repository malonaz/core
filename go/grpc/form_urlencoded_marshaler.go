package grpc

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/grpc-ecosystem/grpc-gateway/v2/utilities"
	"github.com/huandu/xstrings"
	"google.golang.org/protobuf/proto"
)

type urlEncodedMarshaler struct {
	runtime.Marshaler
}

// ContentType means the content type of the response
func (u urlEncodedMarshaler) ContentType(_ any) string {
	return "application/json"
}

func (u urlEncodedMarshaler) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

// NewDecoder indicates how to decode the request
func (u urlEncodedMarshaler) NewDecoder(r io.Reader) runtime.Decoder {
	return runtime.DecoderFunc(func(p any) error {
		msg, ok := p.(proto.Message)
		if !ok {
			return fmt.Errorf("not proto message")
		}

		formData, err := ioutil.ReadAll(r)
		if err != nil {
			return err
		}

		values, err := url.ParseQuery(string(formData))
		if err != nil {
			return err
		}

		// Convert PascalCase keys to snake_case
		normalizedValues := url.Values{}
		for key, vals := range values {
			normalizedValues[xstrings.ToSnakeCase(key)] = vals
		}

		filter := &utilities.DoubleArray{}
		if err := runtime.PopulateQueryParameters(msg, normalizedValues, filter); err != nil {
			return err
		}
		return nil
	})
}
