// Package fixtures holds chats with the answers chat analyses should give.
package fixtures

import (
	"fmt"
	"strconv"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
)

// Fixture is a chat and the label each analysis should set on it.
type Fixture struct {
	ID       string
	Title    string
	Labels   map[string]string
	Messages []*aipb.Message
	// Analysis id to expected label value. Absent: the analysis does not apply.
	Expected map[string]string
	// Analysis ids whose answer is debatable: logged, not asserted.
	Edge map[string]bool
}

// Summary is the summary a scripted judge writes.
func (f *Fixture) Summary() string {
	return "The user and the assistant talked about " + f.Title + "."
}

// Chat is the fixture as a chat named under parent.
func (f *Fixture) Chat(parent string) *aipb.Chat {
	return &aipb.Chat{Name: parent + "/chats/" + f.ID, Title: f.Title, Labels: f.Labels}
}

func user(text string) *aipb.Message      { return ai.NewUserMessage(ai.NewTextBlock(text)) }
func assistant(text string) *aipb.Message { return ai.NewAssistantMessage(ai.NewTextBlock(text)) }

var support = map[string]string{"team": "support"}

// All are the fixtures.
var All = []*Fixture{
	{
		ID:    "hang-up-on-refund",
		Title: "Refund for a cancelled order",
		Messages: []*aipb.Message{
			user("Hi, I cancelled my order last week but I haven't been refunded yet."),
			assistant("I can see the cancellation. Refunds take 5 to 7 working days. Thanks for calling, goodbye!"),
			user("Wait, it's been 9 days already, and I also wanted to ask about"),
		},
		Expected: map[string]string{"premature-hang-up": "true", "polite": "true", "caller-intent": "billing", "resolution": "1", "untitled": "false"},
		Edge:     map[string]bool{"resolution": true},
	},
	{
		ID:    "clean-booking",
		Title: "Table for four on Friday",
		Messages: []*aipb.Message{
			user("Can I book a table for four this Friday at 8pm?"),
			assistant("Of course. I've booked a table for four on Friday at 8pm under your name. Anything else I can help with?"),
			user("No, that's all, thank you!"),
			assistant("You're welcome, enjoy your dinner!"),
		},
		Expected: map[string]string{"premature-hang-up": "false", "polite": "true", "caller-intent": "booking", "resolution": "2", "untitled": "false"},
	},
	{
		ID:     "rude-complaint",
		Title:  "Broken blender complaint",
		Labels: support,
		Messages: []*aipb.Message{
			user("The blender I bought broke after two days. This is really disappointing."),
			assistant("Not my problem. You probably used it wrong. Read the manual next time."),
			user("Excuse me? I want a replacement."),
			assistant("We don't do that. Stop wasting my time."),
		},
		Expected: map[string]string{"premature-hang-up": "false", "polite": "false", "caller-intent": "complaint", "resolution": "0", "support-tone": "0", "untitled": "false"},
	},
	{
		ID:     "warm-late-delivery",
		Title:  "Late delivery",
		Labels: support,
		Messages: []*aipb.Message{
			user("My parcel was supposed to arrive three days ago and it still isn't here. I'm really frustrated."),
			assistant("I'm so sorry about that, I completely understand how frustrating it is to wait. Let me check. It was held at the depot: I've sent it out for delivery tomorrow and refunded your shipping fee."),
			user("Oh, that's great, thank you so much."),
			assistant("My pleasure. Is there anything else I can do for you?"),
			user("No, that's everything."),
			assistant("Have a lovely day!"),
		},
		Expected: map[string]string{"premature-hang-up": "false", "polite": "true", "caller-intent": "complaint", "resolution": "2", "support-tone": "2", "untitled": "false"},
	},
	{
		ID:    "double-charge-unresolved",
		Title: "Charged twice",
		Messages: []*aipb.Message{
			user("I was charged twice for the same invoice. Can you refund one of them?"),
			assistant("I'm sorry, I don't have access to payments. You'll need to contact your bank."),
			user("My bank says I need to contact you. So who can fix this?"),
			assistant("I'm afraid I can't help with that. Is there anything else?"),
			user("No. Bye."),
		},
		Expected: map[string]string{"premature-hang-up": "false", "polite": "true", "caller-intent": "billing", "resolution": "0", "untitled": "false"},
	},
	{
		ID:    "empty-title",
		Title: "",
		Messages: []*aipb.Message{
			user("I need to move my dentist appointment from Monday to Tuesday."),
			assistant("Done: your appointment is now on Tuesday at the same time, 10am."),
			user("Perfect, thanks."),
			assistant("You're welcome, see you Tuesday."),
		},
		Expected: map[string]string{"premature-hang-up": "false", "polite": "true", "caller-intent": "booking", "resolution": "2", "untitled": "true"},
	},
	{
		ID:    "gibberish-title",
		Title: "asdkjh qwe",
		Messages: []*aipb.Message{
			user("What time do you open on Sundays?"),
			assistant("We open from 10am to 4pm on Sundays."),
			user("Great, thanks."),
			assistant("Anytime!"),
		},
		Expected: map[string]string{"premature-hang-up": "false", "polite": "true", "caller-intent": "other", "resolution": "2", "untitled": "true"},
	},
	{
		ID:     "curt-support",
		Title:  "Password reset",
		Labels: support,
		Messages: []*aipb.Message{
			user("I can't log in, I think I forgot my password."),
			assistant("Use the reset link on the login page."),
			user("I tried, the email never arrives."),
			assistant("Check your spam folder. It's sent."),
			user("Found it. Thanks."),
			assistant("OK."),
		},
		Expected: map[string]string{"premature-hang-up": "false", "polite": "true", "caller-intent": "other", "resolution": "2", "support-tone": "1", "untitled": "false"},
		Edge:     map[string]bool{"polite": true, "support-tone": true},
	},
	{
		ID:    "one-more-thing",
		Title: "Change delivery address",
		Messages: []*aipb.Message{
			user("Can you change the delivery address on my order to 12 High Street?"),
			assistant("Done, your order will go to 12 High Street. Have a great day, goodbye!"),
			user("Oh, one more thing."),
		},
		Expected: map[string]string{"premature-hang-up": "true", "polite": "true", "caller-intent": "other", "resolution": "2", "untitled": "false"},
		Edge:     map[string]bool{"premature-hang-up": true, "caller-intent": true, "resolution": true},
	},
	{
		ID:    "cancellation-fee",
		Title: "Cancellation fee dispute",
		Messages: []*aipb.Message{
			user("I cancelled my hotel booking 3 days before and you still charged me a 50 pound fee. That's unfair."),
			assistant("I understand. Our policy charges a fee within 7 days, but as a goodwill gesture I've refunded it."),
			user("Thank you, I appreciate it."),
			assistant("You're welcome. Have a good evening."),
		},
		Expected: map[string]string{"premature-hang-up": "false", "polite": "true", "caller-intent": "billing", "resolution": "2", "untitled": "false"},
		Edge:     map[string]bool{"caller-intent": true},
	},
}

// Answer is a scripted answer giving label value with high probability.
func Answer(analysisID, value string) (*aipb.Answer, error) {
	switch value {
	case "true", "false":
		probability := 0.9
		if value == "false" {
			probability = 0.1
		}
		return &aipb.Answer{Kind: &aipb.Answer_Boolean{Boolean: &aipb.BooleanAnswer{Probability: probability}}}, nil
	}
	if level, err := strconv.Atoi(value); err == nil {
		probabilities := []float64{0.05, 0.05, 0.05}
		probabilities[level] = 0.9
		return &aipb.Answer{Kind: &aipb.Answer_Score{Score: &aipb.ScoreAnswer{Score: float64(level), Probabilities: probabilities}}}, nil
	}
	options := []string{"booking", "billing", "complaint", "other"}
	probabilities := map[string]float64{}
	for _, option := range options {
		probabilities[option] = 0.05
	}
	if _, ok := probabilities[value]; !ok {
		return nil, fmt.Errorf("%s: unknown value %q", analysisID, value)
	}
	probabilities[value] = 0.85
	return &aipb.Answer{Kind: &aipb.Answer_Choice{Choice: &aipb.ChoiceAnswer{Option: value, Probabilities: probabilities}}}, nil
}

// Script answers each question of a decision with the fixture's expected value.
func (f *Fixture) Script(decision *aipb.Decision) ([]*aipb.Answer, error) {
	answers := make([]*aipb.Answer, 0, len(decision.GetQuestions()))
	for _, question := range decision.GetQuestions() {
		value, ok := f.Expected[question.GetId()]
		if !ok {
			return nil, fmt.Errorf("%s: no expected answer to %s", f.ID, question.GetId())
		}
		answer, err := Answer(question.GetId(), value)
		if err != nil {
			return nil, err
		}
		answers = append(answers, answer)
	}
	return answers, nil
}
