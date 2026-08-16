---
title: 'agent: Agent/Task/Memory ontology, durable runners, wake model'
description: 'Agent domain design: Agent (durable actor), Task (terminal work unit, the queue), Memory (lore-shaped long-term memory), chat compaction, wake-by-append model. Postgres SKIP LOCKED queue; streaming/runner deferred.'
labels:
    repo: core
    status: in-progress
    tags: idea,ai
    topic: agent
---

Agent is a server-side resource (agent.v1) that moves the agentic loop out of the browser. An Agent owns a backing AI Chat (malonaz.ai.v1) holding its whole conversation, so any surface renders an agent's transcript with ordinary ListMessages. Design: the loop that lives in the client today (app/core/agent/loop.ts) becomes a durable state machine on the resource — every park point is a state transition, every resumption is a re-claim.

## Resource shape
Pattern organizations/{organization}/users/{user}/agents/{agent}. Immutable inputs: task (self-contained brief), model, tools (AgentTools: service tool sets + allow_sub_agents/max_sub_agents/max_depth). OUTPUT_ONLY projections maintained by the runner: state, chat, report, error_message, root_agent, metadata (turn_count, pending_approvals, awaited_agents), runner. parent_agent references the spawner (empty = root/orchestrator); root_agent makes the whole tree one ListAgents filter and gives CancelAgent its cascade.

AgentState: PENDING (awaiting pickup) -> RUNNING -> {AWAITING_APPROVAL, AWAITING_AGENTS} -> SUCCEEDED | FAILED | CANCELLED.

Service: standard Create/Get/List/Delete plus custom CancelAgent, ResolveApproval, and the streaming StreamAgent. CreateAgent only persists a PENDING row — it never blocks on execution.

## Execution policy (ported from the browser loop)
Per turn: StreamGenerateMessage on the backing chat; RPC tools declared NO_SIDE_EFFECTS auto-execute concurrently. Anything side-effecting parks the agent in AWAITING_APPROVAL with the calls in metadata.pending_approvals; ResolveApproval executes or declines each, and the last resolution resumes the turn. A turn always resolves into a single tool message in original block order — the model never sees a dangling tool call. No tool calls -> final text becomes report, state SUCCEEDED.

## Orchestration via sub-agents
The runner injects a built-in Agent_Spawn tool (task, model?, tools? clamped to a subset of the spawner's own; depth/fan-out enforced by max_depth/max_sub_agents) which is just CreateAgent with parent_agent = self. The spawn call does not resolve inline: like an interactive genui component, it defers the turn — the orchestrator parks in AWAITING_AGENTS with metadata.awaited_agents mapping spawn tool-call id -> child agent name. Several children may be spawned in one turn (free parallelism). Each child's terminal transition writes its report (or error_message) as that spawn call's tool result and, when every awaited child is terminal, resumes the parent. Take SELECT ... FOR UPDATE on the parent row so two children finishing at once can't double-resume it.

## Durable queue without NATS
The agent table is the work queue — no broker, no outbox. Runners claim with `SELECT name FROM agent.agent WHERE state = 'PENDING' AND delete_time IS NULL ORDER BY create_time LIMIT 1 FOR UPDATE SKIP LOCKED`; SKIP LOCKED gives competing consumers for free (no visibility timeouts, no acks). Claiming flips state to RUNNING and stamps runner{id, address, lease_expire_time}; the runner heartbeats the lease each turn and a reaper re-queues RUNNING agents whose lease expired. Crash recovery is free because the pending turn is already persisted (dangling tool calls in the chat, held results in metadata) — the client-side restorePendingTurn logic moved server-side.

Latency: enqueue and every resuming transition NOTIFY agent_work in the same transaction as the insert/update; runners LISTEN and treat a notification purely as a hint to run the claim query, never as the work itself (NOTIFY is fire-and-forget, lost on disconnect). A 5-10s fallback poll stays the source of truth. Because enqueue and state change share one transaction, the publish-after-commit hazard NATS had disappears. Trade-off: no external subscribers (add an outbox table if ever needed) and no pub/sub fan-out for live UIs.

## Streaming: the row is the service registry
Queue dispatch alone would lose token-level streaming, so streaming is a property of who drives the loop, not of the queue.
- Interactive turns: StreamAgent (server-streaming, AgentEvent = oneof {block, message, agent}) claims the agent with the same SKIP LOCKED + lease mechanics and runs the loop in-process, teeing chunks to the caller. Disconnect does not cancel: the lease lapses and a background runner takes over.
- Reattach: StreamAgent on an already-claimed agent proxies to the owning replica's internal attach endpoint, dialing agent.runner.address (plain replica-to-replica gRPC; the row already names the owner).
- Background / sub-agents: per-message granularity via ListMessages polling or a server-side polling watch — the right granularity for progress chips.

Rule: the address is a hint, the lease is the truth, polling is the floor. Every attach degrades one tier down (proxy -> poll) without surfacing an error. Failure matrix: task killed but row stale -> dial fails, reaper re-queues on lease expiry; IP recycled by a new task -> attach carries agent name + runner.id, the new runner answers NotFound; lease expired but process alive (GC pause, netsplit) -> proxy checks lease_expire_time before dialing and treats it as unowned; runner released mid-attach -> internal stream ends with the terminal state, client switches to GetAgent.

## Runner self-addressing on ECS
Runners need their own reachable address to register in Agent.runner. Requires awsvpc network mode (always the case on Fargate): each task gets its own ENI and private IP, so a fixed listen port works with no port conflicts. The task reads its own IP from the metadata endpoint ECS injects, ECS_CONTAINER_METADATA_URI_V4 + "/task" -> Containers[].Networks[].IPv4Addresses[0], then advertises ip:port. Boot order: resolve address -> start internal gRPC server -> start claiming; each claim writes runner{id, address, lease}. runner.id is a fresh UUID per boot so a recycled IP is never mistaken for the same runner.

Security group: the runner SG allows the internal port only from the API/gateway SG. Private task IPs are not browser-reachable, which is exactly why proxying through StreamAgent is load-bearing rather than optional. Do not use Service Connect / Cloud Map DNS for this — those resolve to any healthy task, while attach needs one specific task. EC2 + bridge mode works but is strictly worse: containers share the host IP and the container port is NAT-mapped to a random host port, so you must advertise hostIP:hostPort read from Containers[].Ports[].HostPort, and one host running many runners multiplies the stale-address surface.

## Key files
- app/core/agent/loop.ts — the browser loop this design ports server-side (turn deferral, resolvePendingTurn, restorePendingTurn).
- app/core/agent/executor.ts — NO_SIDE_EFFECTS classification driving the approval policy.
- app/core/agent/useAgentChat.ts — shrinks to CreateAgent + StreamAgent + ResolveApproval.
- cmd/shared/container_service/ecs.tf — ECS/Fargate service module (awsvpc, security groups).

## Implementation status / revisions (2026-08)
- Landed: agent.v1 protos, agent.agent table + SKIP LOCKED claim index, model/store/rpc codegen, go/agent/agent_service scaffold (CancelAgent/ResolveApproval stubbed), cmd/agent-service.
- AgentTools was folded into AgentMetadata (tool_sets, allow_sub_agents, max_sub_agents, max_depth alongside runner bookkeeping); metadata is no longer OUTPUT_ONLY.
- Agent.runner and StreamAgent/AgentEvent are deferred: no streaming for now, so no runner self-addressing either. Attach/proxy design above stays as future reference.
- Open design: split a long-lived AgentDefinition (persona: instructions, model, tools, optional input/output json schemas) from the Agent run. A definition with schemas is exposable as a malonaz.ai.v1.Tool; sub-agent spawn becomes invoking a definition with args.

## Settled ontology (2026-08-16 discussion)
Three primitives:
- Agent `organizations/{org}/users/{user}/agents/{agent}`: durable actor, never terminal (IDLE/ACTIVE/SUSPENDED, heals via lease reaper). Persona: display_name, instructions, model, tool config. `chat` = CURRENT chat; conversation is backed by N chats over its lifetime.
- Task `.../agents/{agent}/tasks/{task}`: terminal work unit (PENDING->...->SUCCEEDED/FAILED/CANCELLED); what was scaffolded as agent.v1.Agent, to be renamed. Nesting = assignment: the task executes with that agent's persona/tools in a FRESH chat. Tree via parent_task/root_task; the table is the queue. Tasks never compact.
- Memory `.../agents/{agent}/memories/{memory}`: lore-shaped (title/description as the search surface, content as google.protobuf.Value, labels over enums — see ~/sgpt/sgpt/v1/lore.proto), plus source_chat/source_task provenance; embeddings later.

Unifying rule: everything an agent hears is a message appended to its chat, and every append is a wake. Flows:
1. User->Agent: message append -> wake -> turns -> reply -> idle.
2. Agent delegates: Task_Create tool call -> turn defers (awaited_tasks) -> idle; task terminal report written as that tool call's result -> append wakes agent -> resume exact turn (the AWAITING_AGENTS machinery).
3. Task->subtask: same, parent parked AWAITING_TASKS.
4. User->Task directly under an agent: report via task.report to creator; optional courtesy message into agent chat.
5. Agent->Agent: chat message = flow 1; delegating work = task under the other agent (flow 4, creator awaits like flow 2).

Compaction (agent chats only): lazily at wake-time, never mid-turn (pending turns must resolve in their own chat). Over threshold + no pending turn -> extraction turn on old chat emits Memory_Create/updates (revise/merge, not append-only) -> new chat seeded with persona + top-N memories + last K messages verbatim -> atomic agent.chat swap, old chat sealed. Memory retrieval: seed-time top-N for v1; Memory_Search tool later.

Wake triggers v1: user/agent message appends + task terminal transitions. Later: schedules ("wake me at 9am"), watched events.

Implemented (2026-08-16): full domain landed and SAT-tested.
- Protos: agent.v1.{Agent,Task,Memory} + AgentService (Agent CRUD+SendAgentMessage, Task CRUD+CancelTask+ResolveApproval, Memory CRUD). AgentState gained PROCESSING=4 as the claim marker (ACTIVE = queued wake).
- Storage: one agent schema/db, tables agent.{agent,task,memory}; task_claim_idx/agent_claim_idx partial indexes; update_time doubles as runner heartbeat, reaper re-queues stale RUNNING/PROCESSING rows.
- Runner (go/agent/agent_service): pollLoop workers claim via FOR UPDATE SKIP LOCKED (queue.go); task loop (task_loop.go) seeds persona+goal chat, executes built-in tools (create_task/create_memory), parks AWAITING_TASKS, final text -> report; terminal.go writes reports back as awaited tool results under row locks and re-queues/wakes the awaiter; agent loop (agent_loop.go) defers turns on create_task, idles via locked last-processed-message recheck; compaction.go rolls chats over (extraction turns -> memories, digest+summary+tail seed, atomic swap).
- Custom RPCs (agent.go): SendAgentMessage (first-contact chat creation, append-then-wake), CancelTask (recursive-CTE subtree cascade + awaiter notification), ResolveApproval (FailedPrecondition/Unimplemented: no external tool executor server-side yet). CreateAgent/CreateTask wrap codegen to initialize OUTPUT_ONLY lifecycle fields.
- SAT: go/test/agent/agent_service/sat, both services against one db, scripted mock provider drives the full loop (memory -> delegation -> task terminal report -> resume -> idle).
Deferred: tool_sets/external tool execution + approvals, streaming/attach, wake schedules, Memory_Search, NOTIFY hints (polling only).
