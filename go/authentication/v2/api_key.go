package authentication

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"strings"

	"google.golang.org/grpc/codes"

	"github.com/malonaz/core/go/grpc/status"
)

// apiKeyAuthenticator proves service accounts by their API keys.
type apiKeyAuthenticator struct {
	// header is lowercase, as gRPC metadata keys are.
	header string
	// Keyed by the key's digest, so a lookup's timing says nothing of the keys.
	keyDigestToAccount map[[sha256.Size]byte]*serviceAccount
}

// newAPIKeyAuthenticator parses `service_account_id:api_key` pairs, as v1 did.
func newAPIKeyAuthenticator(ctx context.Context, header, apiKeys string, serviceAccountIDToAccount map[string]*serviceAccount) (*apiKeyAuthenticator, error) {
	authenticator := &apiKeyAuthenticator{
		header:             strings.ToLower(header),
		keyDigestToAccount: map[[sha256.Size]byte]*serviceAccount{},
	}
	if apiKeys == "" {
		return authenticator, nil
	}
	for pair := range strings.SplitSeq(apiKeys, ",") {
		serviceAccountID, apiKey, ok := strings.Cut(pair, ":")
		serviceAccountID, apiKey = strings.TrimSpace(serviceAccountID), strings.TrimSpace(apiKey)
		if !ok || serviceAccountID == "" || apiKey == "" {
			return nil, fmt.Errorf("invalid api key pair %q: expected service_account_id:api_key", serviceAccountID+":...")
		}
		account, ok := serviceAccountIDToAccount[serviceAccountID]
		if !ok {
			// Removing an account from the configuration revokes it; its key leaves the secret store later.
			slog.WarnContext(ctx, "skipping the api key of a service account not configured", "service_account_id", serviceAccountID)
			continue
		}
		digest := sha256.Sum256([]byte(apiKey))
		if _, ok := authenticator.keyDigestToAccount[digest]; ok {
			return nil, fmt.Errorf("duplicate api key for service account %q", serviceAccountID)
		}
		authenticator.keyDigestToAccount[digest] = account
	}
	return authenticator, nil
}

func (a *apiKeyAuthenticator) authenticate(apiKey string) (*serviceAccount, error) {
	account, ok := a.keyDigestToAccount[sha256.Sum256([]byte(apiKey))]
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "invalid api key").Err()
	}
	return account, nil
}
