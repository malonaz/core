---
title: 'agent: the AgentService (go/ai/agent_service)'
description: 'The server-side agent framework: definitions and agents (malonaz.ai.agent.v1), turns run by leased replicas over Postgres, queued messages, awaited calls (approval, input components), sub-agents and their reports, schedules woken by the scheduler dispatcher, StreamAgent across replicas, tool targets, and whose identity each call carries (the service''s own, the owner''s exchanged token on tool calls).'
labels:
    repo: core
    topic: agent, ai
---

# The AgentService

`go/ai/agent_service` serves `malonaz.ai.agent.agent_service.v1.AgentService` and runs every agent's turns itself,
on whichever replica holds the agent's lease. A host (onikisu) serves it and routes its tool targets.

## Resources

- `Definition` (`organizations/{o}/definitions/{d}`): a reusable `AgentSpec` (instructions, model, reasoning effort,
  tools, sub-agent depth). An agent created from one snapshots its spec.
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
- A turn pauses AWAITING on calls only a user can settle: a gated method (`ApproveToolCall`, or `ResolveToolCall`
  with an error result to deny) and input genui components (`ResolveToolCall` with the answer). A method awaits
  approval unless its tool says otherwise or the method is `NO_SIDE_EFFECTS`. Replying with a message denies them.
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

- A spec's tools are gRPC methods or whole services (a tool set with a discovery tool), resolved per turn by
  reflection on their target: `--tool-target pkg.Service=endpoint`. A spec naming a service without one is refused.
  Genui components need no target.
- The service's own calls (chats, scheduler, replica to replica) go out as its account (`asService`).
- Schemas are read as the service: they are the same for every owner (cached five minutes per endpoint).
- Tool calls are made as the agent's owner: the turn exchanges a token of `--owner-token-issuer` for them
  (`TokenService.ExchangeToken`, subject the owner, actor the agent's name), reused until a minute before it
  expires, and calls with it as the only credential (`asOwner`, `owner_token.go`). The owner's own grants decide
  each call; a refused exchange fails the call, not the turn. The host serves the TokenService and decides who
  exchanges for whom (core's `lores/authentication/overview`, Exchange). The token goes to every tool target.

## Deletion and cancellation

`CancelAgent` ends the running turn wherever it runs. `DeleteAgent` soft-deletes, cancels the turn and the agent's
schedules (force takes its live descendants in one transaction, restored together by `UndeleteAgent`); chats stay.

## Known gaps (2026-11 audit, not re-verified)

A replica that lost its lease is not fenced and may run a turn twice; a message queued during a failed or cancelled
turn is stranded; no caps on tool rounds, fan-out or schedules; nothing compacts a chat that outgrows the model's
context; the workspace server (`cmd/workspace-server`) is unauthenticated.
