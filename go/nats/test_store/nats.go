// Package teststore provides a buffered store of the messages a system under test publishes to
// NATS, so that tests can assert on side effects the RPCs triggering them do not wait for.
package teststore

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	corenats "github.com/malonaz/core/go/nats"

	aippb "github.com/malonaz/core/genproto/aip/v1"
)

// defaultWaitTimeout is how long Require blocks before giving up.
const defaultWaitTimeout = 10 * time.Second

// waitPollInterval is how often Require re-scans the buffer. Messages arrive on the
// subscription's own goroutine, so polling keeps the store free of condition variables.
const waitPollInterval = 10 * time.Millisecond

// Nats buffers every message published to the subjects it subscribes to, keyed by the subject
// it arrived on. Nothing else identifies a message: publishes carry no message ID or headers,
// so the subject is what a test selects on, and for AIP resource events it already encodes the
// resource ID and the verb, as in user_v1_organization.{organization}.created.
//
// Subscriptions are plain NATS rather than JetStream consumers: the store never acknowledges
// anything, so it cannot interfere with the consumers under test. It only sees messages
// published after it was created.
type Nats struct {
	subscriptions []*nats.Subscription

	mutex    sync.Mutex
	messages []*NatsMessage
}

// NatsMessage is a single message the store buffered.
type NatsMessage struct {
	Subject string
	Payload []byte
}

// Unmarshal decodes the payload into the given message.
func (m *NatsMessage) Unmarshal(message proto.Message) error {
	return proto.Unmarshal(m.Payload, message)
}

// NewNats subscribes to the given subjects and starts buffering everything published to them.
// Subjects may contain the NATS wildcards `*` and `>`.
func NewNats(client *corenats.Client, subjects ...string) (*Nats, error) {
	store := &Nats{}
	for _, subject := range subjects {
		subscription, err := client.Subscribe(subject, store.buffer)
		if err != nil {
			store.Close()
			return nil, fmt.Errorf("subscribing to %q: %w", subject, err)
		}
		store.subscriptions = append(store.subscriptions, subscription)
	}
	return store, nil
}

// Close unsubscribes from every subject. Buffered messages are left in place.
func (n *Nats) Close() {
	for _, subscription := range n.subscriptions {
		subscription.Unsubscribe()
	}
	n.subscriptions = nil
}

// Messages returns a snapshot of everything buffered on subjects matching the given pattern,
// which may contain the NATS wildcards `*` and `>`.
func (n *Nats) Messages(subject string) []*NatsMessage {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	messages := []*NatsMessage{}
	for _, message := range n.messages {
		if subjectMatches(subject, message.Subject) {
			messages = append(messages, message)
		}
	}
	return messages
}

// Reset drops every buffered message.
func (n *Nats) Reset() {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.messages = nil
}

// Require blocks for up to 10s for a message on a subject matching the given pattern. See
// RequireWithTimeout.
func (n *Nats) Require(t *testing.T, subject string) *aippb.ResourceEvent {
	t.Helper()
	return n.RequireWithTimeout(t, subject, defaultWaitTimeout)
}

// RequireWithTimeout blocks until a message is buffered on a subject matching the given
// pattern, consumes it, decodes it as an AIP resource event and returns it. The pattern may
// contain the NATS wildcards `*` and `>`; where several messages match, the oldest is taken. It
// fails the test if nothing arrives within the timeout, leaving the comparison to the caller.
func (n *Nats) RequireWithTimeout(t *testing.T, subject string, timeout time.Duration) *aippb.ResourceEvent {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var resourceEvent aippb.ResourceEvent
		if buffered, ok := n.consume(subject); ok {
			if err := buffered.Unmarshal(&resourceEvent); err != nil {
				t.Fatalf("decoding the message on %q as aip.ResourceEvent: %v", buffered.Subject, err)
			}
			return &resourceEvent
		}
		if time.Now().After(deadline) {
			t.Fatalf("no message on %q within %s; %s", subject, timeout, n.describe())
			return nil
		}
		time.Sleep(waitPollInterval)
	}
}

// buffer records a message. It runs on the subscription's goroutine.
func (n *Nats) buffer(message *nats.Msg) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.messages = append(n.messages, &NatsMessage{Subject: message.Subject, Payload: message.Data})
}

// consume removes and returns the oldest message on a subject matching the given pattern.
func (n *Nats) consume(subject string) (*NatsMessage, bool) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	for index, message := range n.messages {
		if !subjectMatches(subject, message.Subject) {
			continue
		}
		n.messages = append(n.messages[:index], n.messages[index+1:]...)
		return message, true
	}
	return nil, false
}

// subjectMatches reports whether a subject matches a NATS subject pattern, in which `*` matches
// a single token and a trailing `>` matches every remaining token.
func subjectMatches(pattern, subject string) bool {
	patternTokens := strings.Split(pattern, ".")
	subjectTokens := strings.Split(subject, ".")
	for index, patternToken := range patternTokens {
		if patternToken == ">" {
			return index < len(subjectTokens)
		}
		if index >= len(subjectTokens) {
			return false
		}
		if patternToken != "*" && patternToken != subjectTokens[index] {
			return false
		}
	}
	return len(patternTokens) == len(subjectTokens)
}

// describe lists the subjects that were buffered, for a timeout error.
func (n *Nats) describe() string {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	if len(n.messages) == 0 {
		return "nothing was buffered at all"
	}
	subjects := make([]string, 0, len(n.messages))
	for _, message := range n.messages {
		subjects = append(subjects, message.Subject)
	}
	return "buffered: " + strings.Join(subjects, ", ")
}
