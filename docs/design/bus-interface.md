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

- **A projector's failure is held.** Its error (or an upcast that
  failed) is reported in `Served`'s failures, and the item is handed
  again at the next poll; a stream with one item in flight is held there,
  the others go on.
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
  (`Subscription::policy(instances, BusPolicy::new(policy))`) of a bus
  hosted on the kernel (`Bus::hosted(Host::new(store, kernel, clock,
  wait, every))`, angzarr-embedded's implementation of the trait). It is
  not in the registry, so no chain runs it and `Kernel::resume` and
  `Kernel::forget_gone` never touch it: the bus's positions are its
  resume, and the bus forgets its purged streams (§7). The hosted bus
  refuses to subscribe a policy whose name the kernel's registry runs
  inline (`Misconfigured::ComponentRegistered`), and one whose source is a
  remote domain (`Misconfigured::RemoteSource`), as registering it is
  refused. An unhosted bus (`Bus::follow`) serves its projectors alone.

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

## 10. Divergences of angzarr-embedded from the distributed design

The rule: angzarr-embedded and the distributed system diverge only where
they must, so an application prototyped on angzarr-embedded moves to the
distributed system unchanged. The distributed design (§9,
distributed-transport.md, policies.md, queries-and-subscriptions.md,
process-managers.md) is the reference, not angzarr-embedded's existing
behaviour. The trait and the conformance suite (§11) are written to it:
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
6. **One event per Project call.** Distributed, Project is handed a run
   of one stream's events (projectors.md §1); angzarr-embedded's bus hands
   runs of one, so its instances finish out of order within the window. A
   run of one is a run, so a projector written for either is correct on
   both; the coordinator may batch a stream's run where its credit is 1.

## 11. Conformance

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
- a held failure is handed again, and a policy's bug is dead-lettered or
  held;
- a purged stream is handed to a projector once as gone and forgotten by
  a policy;
- a commit wakes the bus with no poll.
