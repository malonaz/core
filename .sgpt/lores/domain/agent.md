---
title: 'agent: the AgentService (go/ai/agent_service)'
description: 'The server-side agent framework: definitions and agents (malonaz.ai.agent.v1), turns run by leased replicas over Postgres, queued messages, awaited calls (ApproveToolCall with always, ResolveToolCall total), the tool catalogue (ToolSet/Tool, derived, trimmed to what each caller could call as targets answer TestMethods, the toolbox to what the owner could) and specs granting tools by resource name (ToolGrant), sub-agents and their reports, schedules woken by the scheduler dispatcher, StreamAgent across replicas, tool targets, and whose identity each call carries (the service''s own, the owner''s exchanged token on generations and tool calls, models gated by the host''s generation rule).'
labels:
    repo: core
    topic: agent, ai
---

# The AgentService

`go/ai/agent_service` serves `malonaz.ai.agent.agent_service.v1.AgentService` and runs every agent's turns itself,
on whichever replica holds the agent's lease. A host (onikisu) serves it and routes its tool targets.

## Resources

- `Definition` (`organizations/{o}/definitions/{d}`): a reusable `AgentSpec` (instructions, model, reasoning effort,
  tool grants, sub-agent depth). An agent created from one snapshots its spec; only the agent's `spec.tools` change
  afterwards (UpdateAgent, or an always approval).
- `Agent` (`organizations/{o}/users/{u}/agents/{a}`): its owner is in its name; it stores no principal. It holds a
  chat (`malonaz.ai.v1`, its transcript), `state`, `latest_turn` (outcome, error, awaited tool calls),
  `queued_messages`, and `parent_agent`/`root_agent` for sub-agent trees.

## Turns and leases

- A turn generates on the agent's chat (`AiService.StreamGenerateMessage`), runs the tool calls concurrently,
  and repeats until the model answers without calling tools. Queued messages are delivered at its start and
  between tool rounds; messages queued meanwhile run another turn.
- Replicas claim RUNNING agents without a live lease (`runClaimLoop`, up to `--max-concurrent-turns`), renew
  their leases (`runHeartbeatLoop`) and cancel the turns whose lease they lost. A replica that dies leaves its
  leases to expire; another replica takes the turns over and replays them from the chat: tool requests carry
  request ids derived from the call id, reports from the turn's last message.
- A turn pauses AWAITING on calls only a user can settle, each naming its `tool` (resource name): a gated method
  (`REASON_APPROVAL`) and input genui components (`REASON_RESULT`). Two orthogonal verbs settle them:
  `ApproveToolCall` (the service runs it; only for approval) and `ResolveToolCall` (the caller supplies the outcome, a
  result or an error status, for any awaited call: a refusal is a status, "I ran it myself" a result). Same outcome
  again is a no-op, another FAILED_PRECONDITION. An input's non-error result must carry its InputResponse.
- A reply (`MessageAgent` to an awaiting agent) or `CancelAgent` is the service's own resolve: every unsettled call
  gets a CANCELLED status. Results carry `approved-by` or `resolved-by`, with `actor`.
- `ApproveToolCall.always`: in the same transaction, the agent's own `spec.tools` gets (or refines) a grant of that
  one tool with `APPROVAL_NOT_REQUIRED`, and its other calls awaiting approval for the tool are approved. Scope: this
  agent and the ad-hoc sub-agents it launches afterwards (they inherit its grants); never the definition (org-wide)
  nor the set. Undone by `UpdateAgent` on `spec.tools` (validated as at creation).
- Who sent, settled or cancelled is recorded as the caller's principal and actor (annotations), never the owner.

## Built-in tools

Every agent holds `create_agents` (sub-agents from a definition or ad hoc, at most `max_sub_agent_depth` deep; each
sub-agent's turn ends by sending its report to its parent as a message that wakes it), `message_agent`,
`list_agents`, `update_labels`, and `schedule` / `list_schedules` / `cancel_schedule` (scheduler schedules and
one-offs under the owner, labelled with the agent). `WakeAgent` runs a schedule's tick: it queues its message
or launches its sub-agent, and runs only when the caller is `--scheduler-dispatcher-account`.

## Streaming

`StreamAgent` sends snapshots and, if asked, the transcript's live activity. Activity comes from the replica
running the turn: this one's hub, or the lease holder's `StreamAgent`, proxied (a forwarded call is told by its
caller being the service's own account). A poll every `--stream-poll-interval` re-reads the agents for changes
made elsewhere.

## Tools and identity

- The catalogue: `ToolSet` (`toolSets/{id}`: a routed service by full name, or `genui`) and `Tool`
  (`toolSets/{id}/tools/{method or component}`), derived from `--tool-target pkg.Service=endpoint` and each target's
  reflection (read as the service, cached five minutes), never stored. `ListToolSets`/`GetToolSet`; `ListTools`
  (parent `toolSets/-` across sets)/`GetTool`. Facts: `read_only` (NO_SIDE_EFFECTS, every component),
  `resolved_by_client` (input components). An unreadable set carries `error`, is left out of `toolSets/-` and fails
  its own list (UNAVAILABLE). Comments travel only when the target serves reflection from a descriptor set built with
  source info (`cmd/workspace-server`); the Go registry's descriptors (genui) have none. Built-ins are not listed.
- Reach: each caller sees the tools they could call. Per routed set the service asks its target
  `AuthorizationService.TestMethods` (core's `lores/authentication/overview`) carrying the caller's session; tools not
  returned are left out (GetTool NOT_FOUND), and a readable set left with none is hidden (GetToolSet and its ListTools
  NOT_FOUND, absent from ListToolSets). A target that cannot say (serves no AuthorizationService: UNIMPLEMENTED, or
  fails, logged) trims nothing. Each turn asks again as the owner (`toolbox.reachable`, once per set a turn, on the
  turn's owner token): a granted set offers only the returned methods (none: no discovery tool), a lone tool grant
  not returned is left out. Spec validation stays offline: an unreachable grant is accepted and trimmed at turn time.
  Trimming saves context and failed calls; every call is still the target's decision.
- A spec grants by resource name (`AgentSpec.ToolGrant`: oneof `tool_set` | `tool`, plus `approval`). A set grant
  reaches the model as a discovery tool set (services) or one tool per component (genui); a lone tool grant as one
  tool; a tool grant inside a granted set only refines its approval (the tool's grant, else its set's, else
  NO_SIDE_EFFECTS runs and anything else awaits). Validation checks the set is configured and components exist
  (no network: a wrong method fails the first turn); genui grants take no approval.
- Ad-hoc sub-agents get the parent's grants, or a subset: a held grant or a tool of a held set, approvals inherited
  and only tightened by the model; a requested set keeps the parent's refinements inside it.
- The service's own calls (creating chats, writing and listing their messages, scheduler, replica to replica) go out
  as its account (`asService`).
- Schemas are read as the service: they are the same for every owner (cached five minutes per endpoint).
- Generations and tool calls are made as the agent's owner: the turn exchanges a token of `--owner-token-issuer`
  (`TokenService.ExchangeToken`, subject the owner, actor the agent's name), reused until a minute before it
  expires, and calls with it as the only credential (`asOwner`, `owner_token.go`). The owner's own grants decide
  each call; a refused exchange fails a tool call (its result), and a generation's turn. The host serves the
  TokenService and decides who exchanges for whom (core's `lores/authentication/overview`, Exchange). The token goes
  to every tool target.
- Which models an owner's agents use is the host's rule on `AiService.StreamGenerateMessage` (a condition on
  `request.model`), decided per generation: a refused model fails the turn with the AiService's PERMISSION_DENIED
  ("generating message with {model} as the agent's owner: ..."). Nothing checks the model earlier: CreateAgent,
  CreateDefinition and `create_agents` accept it, and a sub-agent on a refused model fails its first turn, whose
  report ("My turn failed: ...") tells its parent, free to launch it again on another model.

## Deletion and cancellation

`CancelAgent` ends the running turn wherever it runs. `DeleteAgent` soft-deletes, cancels the turn and the agent's
schedules (force takes its live descendants in one transaction, restored together by `UndeleteAgent`); chats stay.

## Known gaps (2026-11 audit, not re-verified)

A replica that lost its lease is not fenced and may run a turn twice; a message queued during a failed or cancelled
turn is stranded; no caps on tool rounds, fan-out or schedules; nothing compacts a chat that outgrows the model's
context; the workspace server (`cmd/workspace-server`) is unauthenticated.
