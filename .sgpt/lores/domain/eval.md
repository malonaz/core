---
title: 'eval: canonical evals, the judge, and recording verdicts on resources'
description: 'How chats are evaluated: evals declared as proto file options (malonaz.ai.eval.v1.eval), eval.Must, the Judge (one forced tool call, judge chat beside the judged one), and how a verdict is recorded on the resource as a filterable label plus an annotation of the same key (eval.malonaz.com/{eval}). Traps: linking, id reuse, judge chats.'
labels:
    repo: core
    tags: ai,eval,labels,annotations
    topic: eval
---

# eval

There is no eval service, database or job. An eval is a proto declaration, the judge is
a Go library over `AiServiceClient`, and its output lives **on the judged resource**.

## Declaring an eval

The owning package declares evals as file options, like experiment definitions:

```proto
import "malonaz/ai/eval/v1/eval.proto";

option (malonaz.ai.eval.v1.eval) = {
  id: "premature-hang-up"
  criterion: "The assistant never ends the call while the caller still has something to say."
};
```

`criterion` describes what a **passing** conversation does. Code takes the eval with
`eval.Must("premature-hang-up")` in a package-level var. It panics when no linked proto
file declares the id, or more than one does. Must scans `protoregistry.GlobalFiles`, so
the declaring Go proto package has to be linked: import it.

## Judging and recording

```go
judge := eval.NewJudge(aiServiceClient, model)
chat, err := judge.EvaluateChat(ctx, chatName, prematureHangUp, other)  // Evaluate + Record + UpdateChat
evaluations, err := judge.Evaluate(ctx, chatName, evals...)             // judge only
err = eval.Record(resource, evaluations...)                             // any Labellable + Annotatable
mask := eval.UpdateMask(evaluations...)                                 // only the keys Record wrote
```

- **One generation per call.** All evals go into one forced `record_verdicts` tool
  call. Each eval id is a required property holding `{rationale, verdict}`, so the judge
  can neither skip an eval nor invent one.
- **The judge sees** the chat's live messages (failed and superseded turns excluded),
  rendered with the system prompt and tool calls/results, without thoughts. Criteria are
  often about following instructions.
- **Judge chats** are real ai chats, created under the judged chat's user and titled
  with the judged chat's name (`ListChats` with `title = "<chat name>"` finds them). They
  carry no labels.
- **Recorded under `eval.malonaz.com/{eval}` twice:**
  - the **label** holds the verdict (`pass` | `fail` | `not-applicable`, the `Verdict`
    enum's external values), so `labels."eval.malonaz.com/premature-hang-up" = "fail"`
    finds the failures and presence `:*` finds every resource the eval ran on;
  - the **annotation** holds the `Evaluation` (rationale, judge model, evaluate_time),
    read with `evalpb.Annotations.Eval(id).Get(resource)`.
- `UpdateMask` holds map-key paths (``labels.`k` ``, ``annotations.`k` ``). The generated
  Update sets just those keys, so concurrent evals and other tooling don't clobber
  each other.

## Traps

- **Never reuse an id** for a different question: past labels would silently change
  meaning. Retire the id and pick a new one.
- An id is at most 63 characters, the length limit of a label key's name segment.
- Re-evaluating overwrites: the label and annotation hold the **latest** evaluation only.
- `not-applicable` exists so out-of-scope conversations (robocalls for a hang-up eval)
  stay out of the fail rate. Write criteria so the judge can tell.

## Key files
- malonaz/ai/eval/v1/{eval,evaluation,labels,annotations}.proto
- go/ai/eval/{eval,judge,record,transcript}.go
- go/test/ai/ai_service/sat/eval_test.go: the record and label-filter contract against a
  real ai-service.
