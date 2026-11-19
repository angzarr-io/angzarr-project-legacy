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
pub enum Handler<P, Q> { Projector { scope: Scope, projector: P }, Policy(Q) }
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
- **Two layers.** `Bus` is the application's interface. Under it a
  `Transport` carries each subscription's deliveries and takes their
  settlements back (§14); `Hub` is the one generic bus over any transport,
  its handlers reached through a `Dispatch` (the kernel in
  angzarr-embedded, the component clients in a coordinator):

  ```rust
  pub trait Transport {
      type Token: Clone; type Error;
      type Feed<'t>: Feed<Token = Self::Token, Error = Self::Error>;
      fn open(&self, opening: Opening<'_>) -> impl Future<Output = Result<Self::Feed<'_>, Self::Error>>;
  }
  pub trait Feed {
      fn next(&self) -> impl Future<Output = Result<Next<Self::Token>, Self::Error>>;  // Delivery | Idle
      fn settle(&self, token: Self::Token, settle: Settle) -> impl Future<Output = Result<(), Self::Error>>;
  }
  pub struct Delivery<T> { token: T, stream: Cover, sequence: u64, attempt: u32, key: OrderingKey, body: Body }
  pub enum Body { Event(Box<EventPage>), Reference, Gone }
  pub enum Settle { Handled, Held, DeadLettered }
  pub struct Hub<'h, T: Transport, S /* event and position store */, D: Dispatch>;  // impl Bus
  ```
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

- **Through the outbox, in both** (policies.md §5): the stamped outputs
  are written to the transactional outbox in the same write as the
  policy's position past the trigger, then delivered (distributed,
  `Deliver`; angzarr-embedded, each a step of its own in its target,
  through the target's five steps) and cleared; the receiver
  deduplicates by the command id. A crash after the write leaves the
  entries pending: they are relayed when the policy is next served
  (angzarr-embedded: before its first delivery, `Dispatch::opened`), and
  the policy is not run again for that trigger. A failed write lands
  neither the outputs nor the position.
- **A relayed step's failure** is the kernel's or coordinator's, as for any
  reaction: a refusal goes to Translate, a bug or a cap is dead-lettered,
  and the entry is cleared; a failure that would be run again (a store's,
  a retryable one with retries off) leaves it pending.

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
| A policy's outputs | none: projectors only | needed change | policy subscriptions, committed through the outbox as the coordinator's are (§5) |
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

Point by point, which side has the better idea (evented, angzarr-embedded's
earlier `Bus`, or the distributed design as written), and why:

| Point | Better | Why |
|---|---|---|
| A request/response handler contract | the earlier `Bus`, with the distributed design | evented had one: `Handle(EventBook) -> Empty` and `HandleSync(EventBook) -> SynchronousProcessingResponse`/`Projection` on every `Saga` and `Projector` (proto/evented/evented.proto:88-93, 126-130), and its AMQP coordinator acked on `Handle`'s answer (applications/event/saga/amqp/amqp.go:33-44). But the answer acknowledged nothing durable: auto-ack had already dropped the message, and the position was marked before a saga's outputs were routed. The earlier `Bus` makes the per-event answer the ack (the position written after it, nothing before), which is projectors.md §4's rule; keep it |
| Credit | the earlier `Bus` | instances × per stream bounds what is in flight per subscription and per stream; evented had none (a channel, a queue's prefetch), and the distributed design names none. Proposal 1 below |
| The event source | the earlier `Bus` | nothing is stored twice: each event is read from the store at the position when an instance is free for it, the bus holding only slots and positions, woken by `CommitNotify` with the `Watch` token as the fallback. evented carried whole books through channels and AMQP; the distributed design carries books in a `BusEnvelope`, with a claim check for large payloads (distributed-transport.md §4), a second copy of every event and a second failure path (`PAYLOAD_UNAVAILABLE`). Proposal 2 below |
| Gaps and redelivery | the earlier `Bus` | evented repaired a gap by reading the store when a pushed book arrived past its position (`RepairSequencing`); reading from the store always makes a gap impossible rather than repaired |
| Positions | the earlier `Bus`, with the distributed design | the distributed design keeps a checkpoint per (handler, stream) in `Processed` (decision 14.68), as evented did per root; the window above the mark is the earlier `Bus`'s, and costs the coordinator one more position per stream |
| A policy's commit | the distributed design | the outbox written with the checkpoint (policies.md §5) is stronger than angzarr-embedded's last-step write: it needs no re-derivation. evented's mark-before-route lost outputs. angzarr-embedded now commits through the outbox too (§5) |
| Retry and dead letters | the distributed design | status classes, a capped backoff, then a dead letter (command-handling.md §3); evented retried without bound, and the earlier `Bus` held a bug forever. The bus now does the distributed design's (§6) |
| Interchangeable transports | evented and the distributed design alike | one narrow interface per handler kind, its in-process and gRPC implementations behind it; the trait keeps that (§1) |

The owner's rulings on the two proposals:

1. **Credit in the distributed design: approved, made.** The projector and
   policy coordinators serve each subscription at a credit of instances ×
   per stream, one event of a stream at a time by default
   (projectors.md §4, distributed-transport.md §2 and §3).
2. **Notifications, not events, on the distributed message bus: not
   approved.** The bus envelope keeps carrying events, with claim checks
   for large payloads (distributed-transport.md §4); a transport may still
   carry references (§14), and every transport falls back to the store for
   missing predecessors.

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
  the outbox: the outputs are durable with the checkpoint, written in
  one transaction, in angzarr-embedded as in the coordinator; evented's
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

1. **A refusal's fact is not its own outbox entry.** Distributed, the
   relay writes Translate's fact as a new outbox entry in the transaction
   that clears its refused command's (policies.md §4); angzarr-embedded's
   relay delivers the fact as a step at once, then clears the command's
   entry. A crash between them re-relays the command, deduplicated at the
   target, refused again, and its fact deduplicated at the source by the
   refused command's id: nothing lands twice. Small and closable (a second
   write), not closed here.
2. **(closed)** A policy's outputs and position commit through the
   outbox in both (§5).
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
- a commit wakes the bus with no poll;
- over the suite's scripted transport: out-of-order deliveries served in
  stream order from the store, the late originals not handled twice; a
  policy's triggers likewise; a held delivery settled held and delivered
  again, one attempt more.

The transport's own checks are §14's.

## 14. Brokers: handled or failed, and what a delivery must carry

Every delivery is acknowledged, handled (dequeued) or failed, with no
fire-and-forget path, and the interface maps onto real brokers (Kafka,
RabbitMQ, JetStream and the like) using only the data it carries; a
broker may carry the event or only a reference to it, resolved against
the event store.

**The transport layer** (the owner's decision). Under `Bus` (§2) a
`Transport` opens one feed per subscription (its name, scope and credit:
the consumer group, queue or durable consumer); a feed's `next` yields a
`Delivery` within the credit, or `Idle` when nothing more is owed, and
its `settle` takes each delivery back, once, after its handler answered:
`Handled` (dequeue: Kafka commit, AMQP `basic.ack`, JetStream `ack`),
`Held` (redeliver, its attempt one more: the retry topic, `basic.nack`
with requeue, `nak`) or `DeadLettered` (dequeue after the bus kept the
letter). `Hub` is the one generic bus over any transport; what it does
above it is the same for every transport:

- **positions** are its own, a window per (subscription, stream) in the
  event store's `PositionStore`, written after the answer: a transport's
  cursor (an offset, an ack floor) is only its own;
- **dedup by the window**: a delivery the window covers (a redelivery, a
  late original) is settled handled without its handler;
- **order from the store**: a delivery beyond its stream's window (per
  stream 1: any sequence past the mark) first has every unhandled
  predecessor read from the event store and handed, in order; the late
  originals, when they come, are covered and settled without their
  handler. So a transport's order never decides correctness: the store is
  the source of order, whatever the broker delivers;
- **references resolved** by reading the event store at the delivery's
  stream and sequence: the event store is part of the bus's context (the
  `Hub`'s store; a coordinator's `EventQueryService`), not an
  implementation detail;
- **credit**: at most a subscription's instances in its handler at once,
  as its transport honours the same credit.

angzarr-embedded's bus is the `Hub` over `StoreTransport`, the event
store as a transport: each owed event delivered as a reference at the
subscription's stored position, nothing carried or kept twice, woken by
the store's `CommitNotify` with its `Watch` token polled as the fallback.
A Kafka or AMQP adapter is another `Transport` under the same `Hub`; none
is built yet, and the requirements on them are below.

**What a delivery must carry**, for any broker to ack, nack or redeliver
it with nothing else:

| Field | Why |
|---|---|
| a delivery token | the broker's handle (Kafka: topic, partition, offset; AMQP: channel and delivery tag; JetStream: the message's reply subject; SQS: the receipt handle; Pub/Sub: the ack id; in process: the claim's slot) |
| the stream: domain, root, edition | the position's key, the dead letter's place, the ordering key |
| the sequence | the position; dedup by (stream, sequence) |
| the attempt | the retry cap without broker support (Kafka has none; AMQP has `redelivered` only, or `x-death` counts) |
| the ordering key | derived from the stream (domain, root, edition), never chosen by the producer: the partition (Kafka), the consistent-hash routing key or single-active-consumer queue (AMQP), the subject token (JetStream), the message group (SQS FIFO), the ordering key (Pub/Sub) |
| the body: the event, or a reference | a reference is (stream, sequence), the claim check's case (`BusEnvelope.claim_checks`), resolved by reading the event store at it |

**The mappings.**

| | Kafka | RabbitMQ (AMQP 0-9-1) | NATS JetStream | in-process store-backed bus | distributed coordinator |
|---|---|---|---|---|---|
| subscription | consumer group, named by the subscription | a queue per subscription, bound to the domain's exchange | a durable consumer per subscription | a pool, by name | a subscription, by the component's name |
| handled (dequeue) | commit the partition's offset up to the lowest unhandled message | `basic.ack` of the delivery tag | `ack` | the position written after the answer | the checkpoint written after the answer |
| failed, held | no per-message nack: hold in memory, pause the partition or seek back; or publish to a retry topic | `basic.nack` with requeue | `nak` (with a delay) | held, handed at the next poll | the relay re-delivers |
| redelivery | from the committed offset after a crash or rebalance | requeued; `redelivered` set | after `ack_wait` | from the stored window | from the checkpoint |
| dead letter | a dead-letter topic, then commit | a dead-letter exchange (`x-dead-letter-exchange`), or ack after writing the letter | `term`, after writing the letter; `max_deliver` | the letter kept in the store, the position past it | `DeadLetterStore`, the position past it |
| credit (instances) | `max.poll.records`, pause/resume per partition | `basic.qos` prefetch per consumer | `max_ack_pending` | instances | instances |
| per-stream order | per partition, the stream its key: holds, but a partition holds many streams | not across consumers, unless single-active-consumer or a consistent-hash exchange on the stream key | per subject, with `max_ack_pending` per subject 1 or ordered consumers | per stream | per stream |
| position | the offset per partition, contiguous; the window above it is in memory only | none in the broker: the checkpoint is the consumer's | the consumer's ack floor | mark and window per stream, stored | mark and window per stream, in `Processed` |
| a reference body | the record holds (stream, sequence) | the message holds (stream, sequence) | likewise | always: the event is read at the position | the claim check |

(SQS and Pub/Sub fit as JetStream does: delete or ack as handled, the
visibility timeout or ack deadline as redelivery, a redrive policy or
dead-letter topic, FIFO message groups or ordering keys for per-stream
order.)

**What brokers cannot express, and the design around it:**

1. **Kafka has no per-message nack, and its offset is per partition.**
   *Required of a Kafka transport:* a **retry topic**. A delivery settled
   held is published to the subscription's retry topic, keyed by its
   stream's ordering key, and its source offset committed, so the
   partition goes on; the retry topic is consumed by the same feed, its
   attempt counted in the record. Per-stream order is kept by routing
   (every record of a stream, retried or not, keyed by its stream) and,
   whatever arrives first, by the bus's order from the store: a later
   event of a stream whose earlier one is retrying is beyond the stream's
   window, so the bus reads and hands the earlier one from the store
   first (or waits for it in flight). The source offset is committed up to
   the lowest unsettled record; a dead-lettered record is committed after
   the bus keeps its letter (a dead-letter topic is optional, the store's
   letter the record). Credit is `max.poll.records` with pause and resume
   per partition.
2. **AMQP orders nothing across consumers.** No subscription is refused
   for it: the bus's order from the store makes the broker's order
   irrelevant to correctness (above). Single-active-consumer or a
   consistent-hash exchange keyed by the ordering key is still advised,
   as it saves the predecessor reads and the late originals' redundant
   deliveries. `basic.qos` prefetch is the credit; `basic.nack` with
   requeue is held; a dead-letter exchange is optional, the store's
   letter the record.
3. **Brokers count attempts loosely.** The attempt is carried in the
   delivery (a Kafka retry record's header, AMQP's `x-death` count,
   JetStream's delivery count), so the retry cap (§6) holds on every
   broker.
4. **A broker that carries the event** (the distributed `BusEnvelope`,
   its claim checks resolved before the bus sees the book) delivers
   `Body::Event`; one that carries only (stream, sequence) delivers
   `Body::Reference`, and the bus reads the event at it. Both are the
   same interface. (The proposal to carry notifications only on the
   distributed message bus was not approved: its envelopes still carry
   events, and any transport still falls back to the store for missing
   predecessors.)

**The conformance suite** checks both layers:

- *the transport* (`angzarr_bus::contract::transport`,
  `transport_contract!`, for any broker adapter): a delivery carries its
  stream, sequence, attempt, ordering key, and an event equal to the
  store's or a reference the store resolves; an unsettled delivery is
  delivered again after reopening; a handled delivery whose position is
  recorded is not; a held one is delivered again, its attempt one more;
  credit bounds what is unsettled, of all streams and of one; a purged
  stream is delivered gone;
- *the bus* (`bus_contract!`, §13), over the implementation's own
  transport, and over the suite's scripted transport, which delivers in a
  scripted order whatever the credit: out-of-order deliveries served in
  stream order, their predecessors read from the store, the late
  originals settled handled without their handler; a policy's triggers
  likewise; every delivery settled, a held one delivered again one
  attempt more; and every earlier check.

angzarr core runs both against the generic bus over its in-memory store
(a reference dispatch), and angzarr-embedded runs the bus checks against
its own.
