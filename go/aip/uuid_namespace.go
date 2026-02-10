package aip

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/uuid"
)

// GetUUIDNamespace extracts the UUID namespace from a proto message's options.
// Returns an error if the namespace is not defined or is empty.
func GetUUIDNamespace(msg proto.Message) (uuid.UUID, error) {
	namespace := pbutil.Must(pbutil.GetMessageOption[string](msg, aippb.E_UuidNamespace))
	parsedUUID, err := uuid.Parse(namespace)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("parsing namespace for %T: %w", msg, err)
	}

	return parsedUUID, nil
}

// MustGetUUIDNamespace extracts the UUID namespace from a proto message's options.
// Panics if the namespace is not defined or is empty.
func MustGetUUIDNamespace(msg proto.Message) uuid.UUID {
	namespace, err := GetUUIDNamespace(msg)
	if err != nil {
		panic(err)
	}
	return namespace
}
