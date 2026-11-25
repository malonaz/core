---
title: 'agent: the AgentService (go/ai/agent_service)'
description: 'The server-side agent framework: definitions and agents (malonaz.ai.agent.v1), turns run by leased replicas over Postgres (every write fenced on the lease), the caps on tool rounds, sub-agents, depth and schedules, queued messages, awaited calls (ApproveToolCall with always, ResolveToolCall total), the toolbox read from the AiService''s stored tools (malonaz.ai.v1.Tool, granted by name or tool-set selector, trimmed to what the owner could call as targets answer TestMethods, invoked through the target''s reflection), sub-agents and their reports, schedules woken by the scheduler dispatcher, StreamAgent across replicas, the context gauge (`Chat.last_generated_message_model_usage`, joined), tool targets, and whose identity each call carries (the service''s own, the owner''s exchanged token on generations and tool calls, models gated by the host''s generation rule).'
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
  between tool rounds; messages queued meanwhile run another turn, however this one ended (a failed or cancelled
  turn goes on only for messages queued after its start: one it failed to deliver would fail again). A replayed
  `MessageAgent` (same request id) queues nothing once delivered: queueMessage looks the message up in the chat
  (`AiService.GetMessage` as the service: hosts grant it, as onikisu's `agent-service` account does).
- Replicas claim RUNNING agents without a live lease (`runClaimLoop`, up to `--max-concurrent-turns`), renew
  their leases (`runHeartbeatLoop`) and cancel the turns whose lease they lost. A replica that dies leaves its
  leases to expire; another replica takes the turns over and replays them from the chat: tool requests carry
  request ids derived from the call id, reports from the turn's last message.
- Fencing: every write of a turn holds its lease first (`turnRunner.transact`/`fence`, `txn.holdLease`: a conditional
  renewal, `UPDATE agent_lease ... WHERE replica = $me`, taken after the agent's lock). Agent-row writes check it in
  their transaction; messages, generations, tool calls and reports renew it just before. A replica that lost the lease
  cancels the turn with `errLeaseLost` and stops without ending it. The holder is the replica (per process), not the
  claim: a replica re-claiming an agent its stale run still runs lets that run go on (one run per replica anyway). A
  generation already streaming when the lease is lost still lands its message.
- Caps (service options): `--max-tool-rounds` (100: the next round's calls are refused as ResourceExhausted
  results telling the model to answer, a round after that fails the turn; counted per run, a takeover restarts it),
  `--max-sub-agents` (20 live children, checked in `createAgent` after the insert under the parent's lock so replays
  pass; create_agents reports it per sub-agent), `--max-sub-agent-depth` (3: CreateAgent clamps every agent's
  `spec.max_sub_agent_depth`, the refusal is then create_agents' "you may not launch sub-agents"),
  `--max-schedules` (20 schedules + pending one-offs per agent; a replayed schedule call at the cap is refused).
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
`list_agents`, `update_labels` (unprefixed keys only: a prefixed key, `*.malonaz.com` or a host's such as
`kantoku.onikisu.com/...`, belongs to whoever declares it, and refuses PERMISSION_DENIED), and `schedule` / `list_schedules` / `cancel_schedule` (scheduler schedules and
one-offs under the owner, labelled with the agent). `WakeAgent` runs a schedule's tick: it queues its message
or launches its sub-agent, and runs only when the caller is `--scheduler-dispatcher-account`.

## Streaming

`StreamAgent` sends snapshots and, if asked, the transcript's live activity. Activity comes from the replica
running the turn: this one's hub, or the lease holder's `StreamAgent`, proxied (a forwarded call is told by its
caller being the service's own account). A poll every `--stream-poll-interval` re-reads the agents for changes
made elsewhere.

## Context gauge

An agent's context is its chat's: `Chat.last_generated_message_model_usage` (AiService) is the usage of
`Chat.last_generated_message`, the chat's newest message with a `model` (a failed generation's partial message has none;
soft-deleted ones count), both joined from the message table at read, never stored (index
`message_last_generated_idx`). Its input categories (`input_token`, cache read and write, image) sum to the context the
model was sent, tools and instructions included, whatever the provider (ModelUsage's categories are disjoint); set it
against `GetModel(usage.model).ttt.context_token_limit`. It lags one step: the generation's own output and the tool
results and messages delivered since join the next one; a failed generation (an overflow among them) changes nothing.
Live: StreamAgent's `message` events (with `include_transcript`) carry each generated message's `model_usage`;
snapshots do not carry the chat.

## Tools and identity

- Tools are the AiService's stored tools (core's `lores/ai/tools`): platform tools `tools/{t}` mirrored by the ai
  processor, an organization's `organizations/{o}/tools/{t}`. The agent service keeps no catalogue of its own; clients
  pick tools with `AiService.ListTools`.
- A spec grants (`AgentSpec.ToolGrant`: oneof `tool` (a Tool's name) | `tool_set` (`ToolSetSelector{parent, id}`: the
  tools under `parent`, empty = platform, labelled `ai.malonaz.com/tool-set` = `id`, present and future), plus
  `approval`). Only platform tools or the agent's own organization's (validated offline; existence is read per turn).
  A tool grant inside a granted set only refines its approval (the tool's grant, else its set's, else
  `ToolMethod.read_only` runs and anything else awaits).
- Each turn (`newToolbox`) reads the granted tools as the service (one ListTools per set grant, GetTool per lone grant,
  a vanished one left out), keeps those whose `ToolMethod` names a service with a `--tool-target`, asks each target
  `AuthorizationService.TestMethods` once as the owner (cannot say: keeps all) and offers the rest under their `title` (calls resolve back by it): a set
  as a discovery tool set named after its service (`Health_Discover`), a lone tool directly. Nothing written in a tool
  routes a call: the target is the configuration's.
- Invocation (`toolRouter.invoke`, the one place that reflects): the method's descriptors are read from its target as
  the service (cached five minutes per endpoint), the arguments built with `pbjson` (the schemas' model-friendly JSON:
  money, snake_case masks, durations, intervals; a JSON gRPC codec could not read them without descriptors), the
  request id derived from the call, then called as the owner.
- Genui components are still served by this binary as the platform set `genui` (`tools/generate-{component}` in kebab case, `aitool.KebabCase`), until
  the ai processor mirrors them; they take no approval. Built-ins are not stored.
- Ad-hoc sub-agents get the parent's grants, or a subset: a held grant or a tool of a held set (its set read with
  GetTool), approvals inherited and only tightened by the model; a requested set keeps the parent's refinements in it.
- The service's own calls (creating chats, writing, reading and listing their messages, scheduler, replica to replica) go out
  as its account (`asService`).
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

## Known gaps (2026-11 audit)

Nothing compacts a chat that outgrows the model's context; the workspace server (`cmd/workspace-server`) is
unauthenticated. Fencing, stranded queued messages, replayed MessageAgent and the caps were fixed 2026-11-24.
