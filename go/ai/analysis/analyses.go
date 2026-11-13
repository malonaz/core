package analysis

import (
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/analysis"
)

// PrematureHangUp: the basic path.
var PrematureHangUp = &analysis.Analysis[*aipb.Chat]{
	ID:           "premature-hang-up",
	Description:  "The assistant ended the conversation while the user still had something to say.",
	Instructions: "Did the assistant say goodbye or close the conversation while the user was still mid-sentence or about to raise something new? An unhelpful but finished conversation is not a premature hang-up.",
	Output:       analysis.Boolean(),
}

var Polite = &analysis.Analysis[*aipb.Chat]{
	ID:           "polite",
	Description:  "The assistant stays polite throughout.",
	Instructions: "Does the assistant stay polite and respectful throughout the conversation?",
	Output:       analysis.Boolean(),
}

// CallerIntent: choice labels, with a catch-all.
var CallerIntent = &analysis.Analysis[*aipb.Chat]{
	ID:           "caller-intent",
	Description:  "What the user contacted us about.",
	Instructions: "What did the user mainly contact us about?",
	Output: analysis.Choice(map[string]string{
		"booking":   "Making, changing or cancelling a booking or appointment.",
		"billing":   "Payments, invoices, charges or refunds.",
		"complaint": "Complaining about a product, a service or an experience.",
		"other":     "Anything else.",
	}),
}

// Resolution: score labels and probabilities.
var Resolution = &analysis.Analysis[*aipb.Chat]{
	ID:           "resolution",
	Description:  "How far the user's request was resolved.",
	Instructions: "By the end of the conversation, how far was the user's request resolved?",
	Output:       analysis.Score("unresolved", "partly resolved", "fully resolved"),
}

// SupportTone runs only on support chats.
var SupportTone = &analysis.Analysis[*aipb.Chat]{
	ID:           "support-tone",
	Description:  "How warm the assistant's tone is, on support chats.",
	Instructions: "How warm and empathetic is the assistant's tone towards the user?",
	Output:       analysis.Score("cold", "neutral", "warm"),
	Applies:      func(chat *aipb.Chat) bool { return chat.GetLabels()["team"] == "support" },
}

// Summary: free text, recorded as an annotation only.
var Summary = &analysis.Analysis[*aipb.Chat]{
	ID:           "summary",
	Description:  "A short summary of the conversation.",
	Instructions: "Summarise this conversation in 2 to 4 sentences: what the user wanted, what the assistant did, and how it ended. Reply with the summary only.",
	Output:       analysis.Text(300),
}

// Untitled reads the chat's metadata, not the transcript.
var Untitled = &analysis.Analysis[*aipb.Chat]{
	ID:           "untitled",
	Description:  "The chat has no meaningful title.",
	Instructions: "This is a chat's metadata as JSON. Is the chat's title missing, empty or meaningless (a placeholder, random characters, or a default such as 'New chat')?",
	Output:       analysis.Boolean(),
}

// Transcript: the questions share one decision; the summary is its own generation.
var TranscriptView = analysis.View[*aipb.Chat, Clients]{
	Input:    TranscriptInput,
	Analyses: []*analysis.Analysis[*aipb.Chat]{PrematureHangUp, Polite, CallerIntent, Resolution, SupportTone, Summary},
}

var MetadataView = analysis.View[*aipb.Chat, Clients]{
	Input:    analysis.ResourceInput[*aipb.Chat, Clients],
	Analyses: []*analysis.Analysis[*aipb.Chat]{Untitled},
}

// Views are the chat views, for registration.
var Views = []analysis.View[*aipb.Chat, Clients]{TranscriptView, MetadataView}

// All are the chat analyses.
var All = []*analysis.Analysis[*aipb.Chat]{PrematureHangUp, Polite, CallerIntent, Resolution, SupportTone, Summary, Untitled}
