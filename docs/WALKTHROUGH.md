# Walkthrough

This is the argument for the design. The code is the source of truth; the numbers are only the ones in `results/benchmarks.json` and `results/simulation.json`.

## What this is

A key-value store with Raft underneath, small enough to read in one sitting and strict enough to defend. The cluster is static, 1 to 5 nodes. The replicated log is the database. HTTP is the client and peer transport. There is no consensus library and no storage engine.

Raft is the right algorithm here because the safety argument is published and checkable: election restriction, the leader-completeness property, and "only commit entries from the current term." Paxos is equivalent and harder to explain at a whiteboard. A single-leader primary with async replication would be faster and would lose acknowledged writes.

## One thread per replica

`RaftNode` is single-threaded. `NodeLoop` is the only caller in the real server: client requests, ticks, and RPC replies are runnables on one queue. The simulator calls the same object directly on the simulated thread of that node.

That removes locks from the protocol. The bug you would otherwise debug is a reply applied in the middle of `propose`. The transport is required to deliver a reply later, on the sender's thread, not on the stack that sent the request. The in-process benchmark does this with `submit` onto the destination loop. The HTTP transport does it with an async client and `submit` of the parsed reply.

The cost is queueing. Latency in the benchmark includes it.

## Time and roles

Roles are follower, pre-candidate, candidate, and leader.

A follower that hears nothing for an election timeout becomes a pre-candidate and asks for pre-votes in `term + 1` without incrementing its own term. If a majority grants them, it starts a real election and bumps the term. A partitioned node therefore cannot force the healthy majority to step up its term on every timeout. That is the disruption problem pre-vote is for.

Granting a pre-vote is not the same as the node's own election timer. Before any leader has been heard, any up-to-date pre-vote is granted, so the shortest timer wins the first election. After that, a node grants only when the leader has been silent for `grantSilenceMs`. That window can be shorter than this node's own timeout, so a slow node can vote for a faster one without starting a rival campaign. The heartbeat must be shorter than both the election window and the grant silence, or a live leader looks dead.

`checkQuorum` steps a leader down when it has not heard from a majority within an election timeout. A leader on the wrong side of a partition stops committing instead of acknowledging writes it cannot replicate.

The simulator gives node `n1` a short fixed timer and the others longer ones, so tests elect deterministically. The benchmark and the real server use a spread (`RaftConfig.sim` in the bench, `RaftConfig.standard` in `Main`) so split votes are unlikely. The invariant is one leader per term, not one leader at every wall-clock instant. Two leaders in the same term abort the simulator.

## Replication

`AppendEntries` carries a previous index and term. The follower refuses a hole, and on a term mismatch it returns the first index of the conflicting term so the leader can jump `nextIndex` back instead of walking one entry at a time. A success moves `matchIndex` forward with `max`. A failure is ignored unless its stamp is the latest probe, so a reordered reject cannot rewind a peer that has already caught up.

Only a current-term entry replicated to a majority advances `commitIndex`. An old-term entry that happens to be on a majority is not committed by count alone. That is Figure 8 in the Raft paper: a leader can be left with an uncommitted suffix that a later leader overwrites. `RaftSimTest.uncommittedEntryIsReplacedByTheNewMajority` crashes the majority before a put can commit, lets the surviving minority elect (they have not heard a leader, so they will grant), commits a different value, and checks that the orphan value is gone when the old majority returns.

On becoming leader the node appends a no-op in the new term. Until that no-op commits, a read is not served. The no-op is what pulls any committed prefix from the previous term into "committed in this term," which is the condition the read path waits for.

At most one append or snapshot is in flight to each peer. Further client entries sit in the leader log and leave in the next batch (up to `maxAppendEntries`, 32). A heartbeat may decide the outstanding RPC was lost and send another; a late reply for the old stamp does not launch a second send. Without that limit, every `propose` resent the unacknowledged prefix, and under a pipeline the follower inbox filled with duplicate batches. Three nodes got slower than five, because five had more followers and one of them answered before the queue was hopeless. After the limit, the same probe showed each entry sent about once per follower, and 3 nodes were faster than 5.

## Durability

`FileLogStore` keeps three files.

`meta.bin` is the current term and `votedFor`, with a CRC, written to a temp file and renamed. A crash cannot leave a half-updated vote.

`wal.log` is length-prefixed frames with a CRC32. On open, a torn tail that fails the CRC or the length check is truncated. The prefix that checksums is kept.

`snapshot.bin` is written, and only then is the WAL rewritten to the suffix after the snapshot. The other order can crash between deleting the prefix and finishing the snapshot, which drops committed data. `MemoryLogStore` is the same interface without disk. A simulated crash keeps the store object, which is the stand-in for "the disk survived."

The state machine snapshot is the map plus the client session cache, so idempotency survives compaction. Strings go through `DataOutput.writeUTF`, which caps a string at 64KB. That is a deliberate limit, not a general document store.

## Linearizable reads

A lease would let the leader answer reads from memory until the lease expires. It is faster, and it is wrong if clocks jump or a lease overlaps a new leader. This code does not use one.

A read on the leader waits until an entry from the current term is committed (the no-op, if nothing else). It records `commitIndex` as the read index, then sends a fresh append and waits until a majority of peers have answered that round. Then it waits until `lastApplied` has reached the read index and reads the map. The heartbeat round trip is the proof that this node is still the leader. A newer leader would have a higher term, and the replies would step this one down before the read returned.

Reads that do not return OK are recorded as FAIL and left out of the checker. They did not observe a value.

## Clients

Each command has `clientId` and `seq`. The state machine remembers the latest sequence per client and the result. A retry of the same pair returns the saved result and does not append again. The client keeps one sequence per call and follows `leaderId`. One `KvClient` is not safe for concurrent callers; the HTTP benchmark builds one per thread.

In the simulator, a call that is still inside `propose` when the timeout fires is INFO, not FAIL. NOT_LEADER and NO_QUORUM return before the leader appends, so a call whose every response was one of those, with nothing left in flight, is FAIL. UNKNOWN, a timeout with no reply, or a timeout while an append might already be in the log is INFO: the write may have committed. Marking that case FAIL is a real bug. A seed in the chaos campaign read back a value whose write had been labeled FAIL, and the checker correctly rejected the history. The label was fixed; the protocol was not asked to forget a committed entry.

## The checker

Different keys do not interact, so a history of a map is linearizable if each key is (Herlihy and Wing, P-compositionality). Each key is a register, searched with Wing and Gong. An OK write sets the register. An OK read must see exactly the current value. An INFO write may be applied or skipped. FAIL operations are omitted. The search memoizes failures only. A cycle in the visiting set returns false. More than 96 operations on one key, or more than 2,000,000 search steps, fails the check instead of returning a guessed answer.

The campaign is 20 seeds, 5 nodes, 3 clients, 5 keys, delay 2–12 ms, 5% loss, up to 60 operations and 12 seconds of virtual time, then a heal, a restart, convergence, and a final read of every key. The recorded result is 0 violations and 0 convergence failures, 148 ms of wall time. Virtual time stops when a client call returns, so a fast commit does not burn the rest of its timeout. That is why a seed can fit 60 operations into the budget.

## Membership, batching, and what was left out

Membership is static. Joint consensus is a second protocol (configuration entries, overlapping quorums) and it is the right next piece, but it is not required to show election, replication, compaction, or linearizability. Adding it without a test for the joint phase would be a claim, not a feature.

Client commands are not batched into one log entry. Batching raises throughput and muddies the latency story. The measured cost of a put is one round of replication of one entry, plus queueing behind the other in-flight calls.

Snapshots replace a prefix; they do not stream incrementally. A lagging follower that is behind `lastIncludedIndex` gets `InstallSnapshot`. A snapshot that is not newer is answered with the snapshot index, not the follower's longer log, so the leader does not treat a divergent tail as matched. The follower discards its suffix when a newer snapshot arrives. The leader resends what comes after.

## The benchmark, read carefully

The in-process rows are not the simulator. The simulator's clock is virtual and cannot produce ops/sec. The benchmark starts real `NodeLoop` threads, waits for a leader, throws away 1 second, and counts successes for 4 seconds. ops/sec is successes divided by the measured elapsed time, which includes draining the last in-flight calls, so it can be slightly over 4 seconds. The 5-node memory put row is the one that shows that (`elapsedNanos` in the JSON).

The HTTP row is a different transport: JDK `HttpServer`, `java.net.http`, HTTP/1.1, four client threads, TCP_NODELAY. The JDK server only disables Nagle when `sun.net.httpserver.nodelay` is set. Without it, small responses sat near 40 ms. That row is still much slower than in-process, which is the parsing, the extra sockets, and the thread hops, not the Raft state machine.

fsync rows call `FileDescriptor.sync`. On this ext4 volume a sync is a few hundred microseconds, so a 5-node fsync cluster is close to a 1-node fsync cluster. On a disk that actually flushes, the gap would be larger. The table does not claim anything about a different disk.

## Tests that matter

- Election and a committed put visible on a quorum.
- A partitioned leader cannot commit; the other side elects and commits.
- A crashed leader's committed data is still there after restart.
- The Figure 8 case above.
- A follower that was down across a snapshot catches up with `InstallSnapshot`.
- A repeated client sequence applies once.
- Delete removes the key.
- The file store truncates a torn WAL and does not drop the prefix when a snapshot is saved.
- The checker accepts a legal overlap, rejects a stale read, and lets an INFO write disappear.
- Six chaos seeds inside the unit suite, and the 20-seed campaign on disk.
- An HTTP cluster puts, redirects to the leader, deletes, and restarts from disk.
