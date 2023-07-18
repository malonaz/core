package jsonnet

import (
	"encoding/json"
	"reflect"
)

// JSONParse is syntactic sugar for generating a parse function for a JSON config.
func JSONParse(payload config) ParseFn {
	payloadType := reflect.ValueOf(payload).Elem().Type()
	return func(bytes []byte) (config, error) {
		payload := reflect.New(payloadType).Interface()
		err := json.Unmarshal(bytes, payload)
		return payload.(config), err
	}
}
