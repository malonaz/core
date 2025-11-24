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
	namespace := pbutil.MustGetMessageOption(msg, aippb.E_UuidNamespace)
	if namespace == nil {
		return uuid.UUID{}, fmt.Errorf("%T does not define a uuid_namespace", msg)
	}

	namespaceStr, ok := namespace.(string)
	if !ok {
		return uuid.UUID{}, fmt.Errorf("uuid_namespace for %T is not a string", msg)
	}

	if namespaceStr == "" {
		return uuid.UUID{}, fmt.Errorf("uuid_namespace for %T is empty", msg)
	}

	parsedUUID, err := uuid.Parse(namespaceStr)
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
