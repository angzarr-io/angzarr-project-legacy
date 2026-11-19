# The bus interface: subscriptions served from a stream store

**Status:** proposed (the owner's design, for review). The trait is `angzarr_bus::Bus` in angzarr core's `crates/angzarr-bus`, its conformance suite `angzarr_bus::contract` (feature `contract`); the first implementation is angzarr-embedded's `Bus`. The distributed coordinator's implementation is §9's sketch, not built. The handler contracts it serves are unchanged: `ProjectorService.Project` (docs/design/projectors.md) and `PolicyService.React`/`Translate` (docs/design/policies.md).

A bus serves **subscriptions**: each is a named handler, the streams it
reads, and how much of them it may have in flight. The bus reads every
owed event from the store, hands it to one of the subscription's
instances, and records the subscription's position once the instance
answers. There is ONE interface, so the in-process bus over the event
store and the distributed bus stand in for each other: an application
written against the trait runs on either.

What this document fixes, for every implementation: the two handler
kinds (§1), the subscription (§2), positions (§3), delivery (§4), the
atomic commit of a policy's outputs (§5), failures (§6), purged streams
(§7), and how a policy on a bus relates to the angzarr-embedded kernel's
inline policies (§8).

## 1. Two handler kinds

- **A projector subscription** serves a sink. Its answer is "handled" or
  a failure, nothing else: the bus routes nothing. Its instances may have
  side effects, recording through a runtime (angzarr-embedded's
  `Kernel::handle`) among them, each with its own identity: a runner that
  records one fact per run of a work item, each fact's external id
  derived from its cause, so a redelivery records nothing twice. The bus
  neither sees nor routes what a sink records.
- **A policy subscription** serves a policy (`PolicyService`: React, and
  Translate for its refused commands). Its answer is outputs, commands or
  facts addressed to target streams, and the bus routes them: it
  addresses, stamps and checks each one, derives its command id, delivers
  it, and commits the policy's position with them (§5). Routing is done in
  one place per transport: angzarr-embedded's kernel in process (its
  `check_outputs` and `ids::policy_command_id`, the one copy), the
  coordinator's `Deliver` distributed (docs/design/distributed-transport.md
  §2).

Policies move work across domains; anything that does work and reports
what it found is a projector that records facts. A policy is never a
worker pool.

The handler types are the implementation's (the trait's associated types
`Projector` and `Policy`): angzarr core cannot name angzarr-router's
`ProjectorHandler` and `PolicyComponent`, which depend on it.
angzarr-embedded serves exactly those (a projector boxed as
`DynProjectorHandler`; a policy as a `BusPolicy`, its `PolicyComponent`
with its output domain), and so does a coordinator, whose components are those traits
over gRPC (the host's remote projector and policy). An application is
generic over a bus whose handler types are those.

## 2. The subscription

```rust
pub struct Subscription<P, Q> { /* name, scope, credit, handler */ }
pub enum Handler<P, Q> { Projector(P), Policy(Q) }
pub struct Scope   { /* one or more domains */ }
pub struct Credit  { /* instances, per stream */ }

pub trait Bus {
    type Projector;
    type Policy;
    type Refused: Diagnostic;
    type Error: Diagnostic;

    fn subscribe(
        &mut self,
        subscription: Subscription<Self::Projector, Self::Policy>,
    ) -> Result<(), Self::Refused>;

    fn serve<M: FnMut(&Served) -> bool + MaybeSend>(
        &mut self,
        more: M,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend;
}
```

- **The name** keys the positions and, for a policy, derives every
  command id. Two subscriptions on one bus never share a name; a policy's
  name is its `PolicyDescriptor` name.
- **Scope: every stream of a set of domains**, at least one, every
  edition's streams as main's. A stream that appears later is served from
  its start. A policy's scope is its one input domain.
- **Credit: instances × per stream.** At most `instances` items of the
  subscription are in its handler at once, and at most `per stream` of
  one stream. Per stream is 1 by default, so a stream's events are
  handled one at a time, in order; a projector whose events are
  independent work items may ask for up to `MAX_PER_STREAM` (64), and is
  then handed a stream's events out of order. **A policy's per-stream
  credit is 1**, always: its position is committed with its outputs
  (§5), so it moves in order. Policies scale by streams, not by events of
  one stream.
- **Acknowledgement** is the handler's answer: the bus records the
  position only after it (projectors.md §4: "never mark as done until
  business logic responds with an affirmative").
- **Serving.** `serve` runs every subscription until `more`, asked with
  what was served since it was last asked each time the bus is idle
  (nothing in flight, nothing owed but what failed), says no, or until
  the bus meets an error it cannot hold (a store failure). Dropping the
  future is a crash: nothing is lost (§4).

## 3. Positions: a mark and a window

A subscription's position on one stream is a **mark** and the **handled
set above it** (`angzarr_bus::Window`): every sequence below the mark is
handled, the mark is not, and of the `MAX_PER_STREAM` sequences from the
mark on, the set says which are. Instances finish in any order; the mark
moves past every handled sequence at it. The set is one 64-bit word.

Stored as two positions of the store's `PositionStore` (a
`PositionKey`'s next sequence), in one write: the mark under the
subscription's name, the set under the name followed by `+handled`
(`angzarr_bus::handled_key`). A subscription with one event of a stream
in flight never has a set, and a policy's position is its mark alone,
the format of an inline policy's (§8).

## 4. Delivery

- **At least once.** The position is written after the handler answers.
  A crash between the answer and the write hands the item again; one
  before the answer, likewise. A re-served subscription is handed
  exactly what its stored position does not cover: nothing handled is
  skipped, and with the window nothing recorded as handled is handed
  again.
- **Per stream, never across streams.** Nothing orders two streams'
  events (projectors.md §2, decision 14.96).
- **Read when handed.** The bus keeps no event: it reads each from the
  store when an instance is free for it, so credit, not memory, bounds
  what is in flight.
- **Several subscriptions on one bus** each have their own positions and
  instances; one's failure or slowness holds none of the others.
- **Wake source.** The implementation's. In process: the store's
  `CommitNotify`, armed before each read, so no commit after the arming
  is missed, with the `Watch` change token polled on the caller's timer
  as the fallback (another process's commits). Distributed: the stream
  subscription the coordinator follows (§9).
- **A framework meta-event** (an edition's fork) is never handed; its
  sequence counts as handled.

## 5. A policy's outputs, committed with its position

The position never moves past a trigger unless every output of it is
durable, and an output is never delivered twice:

- **In process** (angzarr-embedded): each output is a step of its own in
  its target stream, through the target's five steps, and the policy's
  position past the trigger is written in the same store write as the
  last output's events (or alone, when the policy answers none). A crash
  between two outputs leaves the position at the trigger; the re-served
  trigger re-derives the same command ids, and the outputs that landed
  are duplicates at their targets, deduplicated by id.
- **Distributed** (policies.md §5): the stamped outputs are written to
  the transient outbox in the same transaction as the checkpoint, then
  dispatched (`Deliver`) and cleared; the receiver deduplicates by the
  command id. A crash after the commit re-sends the stored outputs, and
  the policy is not run again for that trigger.

Either way the ids are `ids::policy_command_id(trigger, name, index)`,
commands and facts in one index space, so a re-run, a redelivery or a
re-sent outbox entry lands nothing twice. A refusal's fact (Translate) is
committed the same way, with the refused command's id.

## 6. Failures

- **A projector's failure is classed by its status** (command-handling.md
  §3), as every framework retry loop's is: a retryable one (UNAVAILABLE,
  DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, ABORTED) is retried with backoff
  and jitter up to the retry cap, then dead-lettered; any other
  (INTERNAL, UNKNOWN, a business status) is dead-lettered at once.
  Dead-lettered, its letter kept, the event is passed and the position
  moves past it: the letter is the record to replay. A failure not yet
  dead-lettered (retries off, the letter not kept, an upcast that failed)
  holds its event, handed again at the next poll; a stream with one item
  in flight is held there, the others go on. Every failure is reported in
  `Served`'s failures. In process the kernel's inline-projector retry
  loop does this (`Kernel::project_book`), so a projector fails alike
  inline and on the bus.
- **A policy's failure is its router's.** In process the kernel's rules
  hold unchanged (command-handling.md, policies.md §4): a policy's own
  error, a handler's bug in a step, or a retryable failure past its cap,
  is dead-lettered, and the position moves past the trigger
  (`OnPolicyBug::MovePast`, the default) or stays (`OnPolicyBug::Stay`);
  a refused answer (`check_outputs`), a malformed reaction, a chain too
  deep (`ChainTooDeep`) or a store failure holds the position. The bus
  reads where the position ended: moved past, the trigger is done;
  held, it is handed again at the next poll. Every one is reported in
  `Served`'s failures. Distributed: the same rules, with on_policy_bug,
  by the policy coordinator.
- **Dead letters** are the router's: the kernel keeps each in the store
  with the write that moves the position past it, as for an inline
  policy.

## 7. Purged streams

A stream the subscription holds a position on and the store no longer
lists (decision 14.68: a purge never touches positions):

- **a projector** is handed it once as gone (its cover, no page), and,
  handled, the position is put back to 0;
- **a policy** is never called for it (one event per call, decision 46,
  Q3): its position is put back to 0, as `Kernel::forget_gone` does.

A stream whose head is below the mark (purged and written again) is
served again from its start.

## 8. Inline policies and the bus

angzarr-embedded runs a policy one of two ways, never both:

- **Inline**: registered in the `Registry` (`Registry::policy` and its
  twins). It runs inside `Kernel::handle`'s chain, once per written
  event, and `Kernel::resume` runs what it owes after a crash.
- **On a bus**: a policy subscription
  (`Subscription::policy(instances, BusPolicy::new(policy))`) of
  angzarr-embedded's bus, which is the trait's implementation and has no
  API of its own beside it (`Bus::new(Host::new(store, kernel, clock,
  wait, every))`). It is
  not in the registry, so no chain runs it and `Kernel::resume` and
  `Kernel::forget_gone` never touch it: the bus's positions are its
  resume, and the bus forgets its purged streams (§7). The hosted bus
  refuses to subscribe a policy whose name the kernel's registry runs
  inline (`Misconfigured::ComponentRegistered`), and one whose source is a
  remote domain (`Misconfigured::RemoteSource`), as registering it is
  refused; so is a projector subscription whose scope holds a remote
  domain.

A policy step on the bus is an inline step's: the kernel runs the one
policy on the one trigger (upcast first), routes its outputs as a chain
does (the same `react`, `check_outputs`, ids, retries, Translate, dead
letters, `OnPolicyBug` and depth limit), and then settles what those
outputs wrote: the targets' inline policies and the inline projectors
run, exactly as after `Kernel::handle`. Its sampled instant is the bus's
clock's at the step.

**Moving a policy between inline and the bus keeps its place**: both
keep the position under the policy's name, as the next sequence on each
source stream, so a policy registered inline yesterday and served by a
bus today is handed only what it has yet to handle, and its re-derived
ids deduplicate anything in between.

Nothing about an inline policy changes: `Kernel::handle`, the chain, its
failures and `Kernel::resume` are as they were.

## 9. The distributed implementation

The coordinator implements the same trait:

- **Projector subscriptions**: the coordinator follows each scope's
  domains (`EventStreamService.Subscribe`: catch-up, then live) as its
  wake source and stream listing, hands each owed event to one of the
  subscription's instances (`ProjectorService.Project`) within its credit,
  and records the window in its `PositionStore` after the affirmative
  answer. The window is the coordinator's, never the client's cursor.
- **Policy subscriptions**: `PolicyService.React` per owed trigger, the
  outputs checked, addressed and stamped (the same rules and ids as
  angzarr-embedded's kernel), written to the outbox with the checkpoint in
  one transaction, relayed by `Deliver`, Translate on a business refusal
  (policies.md §3-5).
- **Gone streams** from its `ListStreams` and `Processed` as in §7; dead
  letters to its `DeadLetterStore`.

One copy of the output rules: `check_outputs` and the stamping are
angzarr-embedded's today; for the coordinator to share them they move to
angzarr-router (beside `PolicyComponent`), a change of its own.

## 10. angzarr-embedded's earlier bus, audited against the distributed design

angzarr-embedded's first `Bus` (projectors only, before this interface)
was designed in process first. Each of its points against the
distributed design, and what became of it; the trait (§2) is the
definition, and the bus was reworked to it:

| Point | The earlier bus | Verdict | Now |
|---|---|---|---|
| Positions | a mark under the name and the handled set under `name+handled`, two `PositionStore` positions in one write, read back through `Processed` | compatible | kept, as `angzarr_bus::Window`: the coordinator keeps the same two positions in its `PositionStore` (its `Processed` is the same record, decision 14.68); a policy's is the mark alone, the format of the coordinator's checkpoint |
| A policy's outputs | none: projectors only | needed change | policy subscriptions; their commit is §5, and its remaining difference from the outbox is divergence 1 (§12) |
| Ack and credit | the handler's `Ok`, written after; instances and per stream | compatible | kept: projectors.md §4's "never mark as done until business logic responds" |
| Ordering | per stream, never across | compatible | kept (decision 14.96, queries-and-subscriptions.md §5) |
| At least once, dedup | re-handed until written; a projector skips by (stream, sequence) | compatible | kept; a policy's dedup is its derived ids (§5) |
| Failure | every error held and re-handed at each poll, forever: no status class, no retry cap, no dead letter, so one bug stalled its stream | needed change | classed by status, retried to the cap, dead-lettered and passed (§6), the kernel's inline-projector loop reused |
| Purged streams | handed once as gone, forgotten | compatible | kept; a policy forgets without a call (§7) |
| Wake | `CommitNotify`, the `Watch` poll | unavoidable divergence | the transport's (§12, 5) |
| Naming and identity | the projector's name keyed its positions; a name was refused twice on one bus, not against the registry | needed change | the name is the component's registered name (the coordinator's projector and policy names); a policy is refused if the registry runs it inline, and any subscription of a remote source is refused |
| API | its own `Subscription`, `Served` and `follow`/`follow_commits`, served with a catalog and no kernel | needed change | removed: `Bus::new(Host)` implements the trait and has nothing beside it, so an application written to it is written to the trait |

## 11. The Go predecessor (evented)

angzarr's Go predecessor (github.com/benjaminabbitt/evented) ran sagas
and projectors over a transport. What it was, read from its source:

- **Interfaces.** Async delivery was one method,
  `EventTransporter.Handle(ctx, *EventBook) error`
  (transport/async/eventTransporter.go:8-10); synchronous delivery one
  per kind, `SyncSagaTransporter.HandleSync(ctx, *EventBook)
  (*SynchronousProcessingResponse, error)`
  (transport/sync/saga/syncSagaTransporter.go:9-11) and
  `SyncProjectorTransporter.HandleSync(ctx, *EventBook) (*Projection,
  error)` (transport/sync/projector/syncProjectionTransporter.go:9-11).
  The command handler held them in a `transport.Holder`: Go channels of
  books for the async sinks, and the sync clients
  (applications/command/command-handler/framework/transport/transportHolder.go:9-16).
  The services were `Saga`/`SagaCoordinator` and
  `Projector`/`ProjectorCoordinator`, each `Handle` and `HandleSync`
  (proto/evented/evented.proto:81-93, 119-130).
- **Delivery: pushed by the writer.** After its write the command
  handler called every sync projector and saga with the pages marked
  `synchronous` (framework/server.go:74-106, 108-144; the retries at :58, :112, :132), then sent the book
  down every async channel to an AMQP fanout exchange
  (transport/async/amqp/sender/amqp.go:23-40). A saga's or projector's
  coordinator received from its queue
  (transport/async/amqp/receiver/amqp.go:30-58,
  applications/event/saga/amqp/amqp.go:30-46).
- **A saga's outputs** were books its business logic answered
  (`SynchronousProcessingResponse.books`), which the coordinator sent to
  the other domain's `BusinessCoordinator.Record`
  (support/coordinator/sagaCoordinator.go:41-47): facts recorded at the
  target, the first configured target only
  (applications/event/saga/grpc/coordinator.go:24), with no derived id.
- **Acknowledgement and retry.** The receiver acked on success and nacked
  to requeue on failure (applications/event/saga/amqp/amqp.go:33-44),
  but consumed with auto-ack on (receiver/amqp.go:117-125), so the
  broker had already dropped the message. Every gRPC call retried with
  an unbounded exponential backoff (transport/sync/saga/grpcSagaClient.go:15-21,
  framework/server.go:58-61); there was no dead letter.
- **Positions.** `Processed` kept the last sequence received per root,
  one collection per deployed coordinator
  (repository/processed/processed.go:22-57). On a delivery whose sequence
  was past the last + 1, the coordinator read the gap from
  `EventQuery.GetEvents` and handed it first (`RepairSequencing`,
  support/coordinator/coordinatorBase.go:18-48), then marked the book
  processed (:50-58). A saga's position was marked before its outputs
  were routed (sagaCoordinator.go:34 then :41), and a routing error was
  logged and dropped (:43-45).
- **In-memory and networked transports were interchangeable** by the
  narrow interface: a channel, a no-op sender
  (transport/async/amqp/sender/noop.go) and AMQP behind
  `EventTransporter`; a gRPC client behind each `Sync*Transporter`.

What we keep:

- **One narrow interface per handler kind**, the same in process and over
  the wire: `ProjectorService.Project` and `PolicyService.React`/`Translate`
  are the Go `Projector`/`Saga` services' successors, and the trait's
  associated handler types are them (§1).
- **The store is the truth, a delivery a hint.** `RepairSequencing` read
  the gap from the store whenever a pushed book arrived past the
  position; the bus goes further and reads every event from the store
  when it is handed (§4), so a lost or reordered push loses nothing.
- **A position per (handler, stream), the next sequence**, as `Processed`
  kept it; ours names the handler (one store holds every subscription's).
- **A saga answering facts for its target**: angzarr's policy fact arm
  (policies.md §2).

What we do differently, and why:

- **The position moves with the outputs, never before them** (§5):
  evented marked a saga's position before routing its outputs and
  dropped a failed route, so a crash or a refusal lost them. This answers
  the question divergence 1 (§12) leaves open: the outputs must be
  durable with the checkpoint, which the outbox does in one transaction
  and angzarr-embedded's last-step write does by re-deriving; evented's
  loss is the case against anything weaker.
- **Ids are derived** (`policy_command_id`), so a redelivery lands
  nothing twice; evented's facts had none, and a requeue recorded them
  again.
- **Acknowledge only after the answer, and bound the retries**: evented's
  auto-ack lost failed messages and its unbounded backoff stalled a
  stream on a bug forever; the bus writes the position after the answer
  and dead-letters at the cap (§6).
- **One event of a stream at a time for a policy** (§2): evented's
  coordinators handled a root's books in sequence (the gap repair
  assumed it), which a saga's outputs need; the bus keeps that order for
  policies and lets only projectors of independent work items widen it.
- **Pull, not push**: the writer does not call its subscribers
  (evented's command handler called every sync saga and projector and
  waited); the bus serves subscriptions from the store, woken by commits,
  and SIMPLE's synchronous projection is the command coordinator's
  (`HandleSync`), not the bus's.
- **Every output goes to its policy's one output domain**, addressed by
  the kernel or coordinator; evented sent every saga book to its first
  configured target.

## 12. Divergences of angzarr-embedded from the distributed design

The rule: angzarr-embedded and the distributed system diverge only where
they must, so an application prototyped on angzarr-embedded moves to the
distributed system unchanged. The distributed design (§9,
distributed-transport.md, policies.md, queries-and-subscriptions.md,
process-managers.md) is the reference, not angzarr-embedded's existing
behaviour. The trait and the conformance suite (§13) are written to it:
every check holds of both. Where angzarr-embedded's `Bus` differs today:

1. **A policy's outputs are not held in an outbox.** Distributed, the
   stamped outputs are written to the outbox with the checkpoint in one
   transaction and the policy is never run again for a stored trigger
   (policies.md §5). angzarr-embedded writes each output as its own step,
   the position with the last, and a crash between two runs the policy
   again, the outputs that landed deduplicated by their ids (§5). Not
   necessary: angzarr-ports' `Write` already carries outbox entries with
   positions, and `Outbox` is the relay's side, so the kernel can write a
   served policy's stamped outputs and its position as one write and relay
   them (deliver, clear, Translate a refusal into a new entry in the write
   that clears its command's). Open, for the owner: it changes the
   kernel's policy step (and, to converge fully, the inline chain's).
   Until then, a policy on angzarr-embedded must re-derive the same
   outputs for a trigger (deterministic, as `Policy::NAME`'s docs ask),
   where distributed it need not.
2. **Translate and retries follow from 1**: the kernel calls Translate
   and retries a step within the policy's step; distributed, the outbox
   relay does, after the commit. Closed with 1.
3. **A served policy's step settles the inline chain downstream** (its
   targets' inline policies and the inline projectors run in the same
   call). Necessary only while inline policies exist; they are
   angzarr-embedded's alone (the atomic in-process chain, a deliberate
   divergence tagged in angzarr-embedded-conformance's `tags.test.rs`). An
   application with every policy on the bus has none.
4. **`Kernel::resume` and `Kernel::forget_gone`** exist for inline
   policies alone; the bus, in both, is its own resume. Necessary for the
   same reason as 3.
5. **The wake source**: the store's `CommitNotify` and `Watch` in process,
   the coordinator's stream subscription distributed. Necessary: it is the
   transport.
6. **A held failure is handed again at the next poll**, the host's poll
   interval its backoff, where the coordinator's relay re-delivers on its
   own schedule. Both retry within the cap first (§6); what is left is
   only a failure the cap has not ruled on (retries off, a store that
   failed to keep the letter). Unavoidable while the bus is a poll loop
   over the store; harmless, since both re-deliver until the cap.
7. **One event per Project call.** Distributed, Project is handed a run
   of one stream's events (projectors.md §1); angzarr-embedded's bus hands
   runs of one, so its instances finish out of order within the window. A
   run of one is a run, so a projector written for either is correct on
   both; the coordinator may batch a stream's run where its credit is 1.

## 13. Conformance

`angzarr_bus::contract` is a test kit any implementation runs: a
`Harness` gives the suite a bus over durable state it can drop and
rebuild, a store to append to and purge, and adapters from the suite's
scripted sink and policy to the implementation's handler types; each
check is a `pub async fn` over a harness, `bus_contract!` stamps a test
per check. The checks:

- a projector's instances finish out of order, and the window records it;
- a crash hands again exactly what the stored position does not cover,
  skipping nothing;
- a re-served policy delivers nothing twice: its outputs' ids are
  `policy_command_id`'s, deduplicated at their targets;
- several subscriptions on one bus are each served all their scope's
  events, and only those;
- a stream's events are handed one at a time, in order, by default;
  credit bounds what is in flight, per subscription and per stream;
- a policy's position moves only with its outputs (a failed commit lands
  neither);
- a projector's retryable failure is handed again, and its bug
  dead-lettered and passed, its stream going on;
- a policy's bug is dead-lettered (the position past it, not run again)
  or held (run again);
- a purged stream is handed to a projector once as gone and forgotten by
  a policy;
- a commit wakes the bus with no poll.
