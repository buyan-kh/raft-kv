package raftkv.raft;

import raftkv.kv.KvStateMachine;
import raftkv.persist.LogStore;
import raftkv.raft.Types.AppendEntries;
import raftkv.raft.Types.AppendEntriesReply;
import raftkv.raft.Types.ClientResult;
import raftkv.raft.Types.Clock;
import raftkv.raft.Types.Command;
import raftkv.raft.Types.InstallSnapshot;
import raftkv.raft.Types.InstallSnapshotReply;
import raftkv.raft.Types.LogEntry;
import raftkv.raft.Types.Observer;
import raftkv.raft.Types.RaftConfig;
import raftkv.raft.Types.RaftView;
import raftkv.raft.Types.RandomSource;
import raftkv.raft.Types.RequestVote;
import raftkv.raft.Types.RequestVoteReply;
import raftkv.raft.Types.Role;
import raftkv.raft.Types.Status;
import raftkv.raft.Types.Transport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Single-threaded Raft replica. Callers serialize {@code onTick} and every {@code handle*}
 * method; transports must not deliver a reply on the same stack that sent the request.
 *
 * <p>Safety-critical choices, each called out where it happens:
 * <ul>
 *   <li>a vote is persisted before it is granted</li>
 *   <li>only a current-term entry replicated to a majority advances {@code commitIndex}</li>
 *   <li>a new leader appends a no-op so it learns the real commit index before serving reads</li>
 *   <li>linearizable reads wait for a fresh majority heartbeat, not a clock lease</li>
 *   <li>pre-vote keeps a partitioned node from bumping the term</li>
 * </ul>
 */
public final class RaftNode {
    private final String id;
    private final List<String> peerIds;
    private final List<String> others;
    private final LogStore store;
    private final KvStateMachine stateMachine;
    private final Transport transport;
    private final RaftConfig config;
    private final Clock clock;
    private final RandomSource random;
    private final Observer observer;
    private final RaftLog log = new RaftLog();
    private final Map<Integer, Consumer<ClientResult>> pending = new HashMap<>();
    private final List<PendingRead> pendingReads = new ArrayList<>();
    private final Map<String, Integer> nextIndex = new HashMap<>();
    private final Map<String, Integer> matchIndex = new HashMap<>();
    private final Map<String, Long> lastContact = new HashMap<>();
    private final Map<String, Long> outboundStamp = new HashMap<>();
    /** One AppendEntries or InstallSnapshot is outstanding per peer. A heartbeat may replace it. */
    private final Map<String, Boolean> appendInFlight = new HashMap<>();
    private final Map<String, Long> appendSentAt = new HashMap<>();
    private final Set<String> voteGranted = new HashSet<>();
    private final Set<String> preVoteGranted = new HashSet<>();

    private int currentTerm;
    private String votedFor;
    private int commitIndex;
    private int lastApplied;
    private Role role = Role.FOLLOWER;
    private String leaderId;
    private long electionDeadline;
    private long nextHeartbeatAt;
    private long nextPreVoteSend;
    private long lastLeaderContactMs;
    private boolean everHeardLeader;
    private int preVoteTerm;
    private long stampSeq;
    private boolean crashed;

    public RaftNode(
            String id,
            List<String> peerIds,
            LogStore store,
            KvStateMachine stateMachine,
            Transport transport,
            RaftConfig config,
            Clock clock,
            RandomSource random,
            Observer observer) {
        if (!peerIds.contains(id)) {
            throw new IllegalArgumentException("peer list must include " + id);
        }
        this.id = id;
        this.peerIds = List.copyOf(peerIds);
        this.others = peerIds.stream().filter(peer -> !peer.equals(id)).toList();
        this.store = store;
        this.stateMachine = stateMachine;
        this.transport = transport;
        this.config = config;
        this.clock = clock;
        this.random = random;
        this.observer = observer == null ? Observer.NONE : observer;
        recover();
    }

    private void recover() {
        LogStore.Meta meta = store.loadMeta();
        currentTerm = meta.currentTerm();
        votedFor = meta.votedFor();
        LogStore.Snapshot snapshot = store.loadSnapshot();
        if (snapshot != null) {
            log.resetToSnapshot(snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm());
            stateMachine.restore(snapshot.data(), snapshot.lastIncludedIndex());
            commitIndex = snapshot.lastIncludedIndex();
            lastApplied = snapshot.lastIncludedIndex();
        }
        for (LogEntry entry : store.loadEntries()) {
            if (entry.index() != log.lastIndex() + 1) {
                break;
            }
            log.append(entry);
        }
        resetElectionDeadline(clock.millis());
    }

    public String id() {
        return id;
    }

    public Role role() {
        return role;
    }

    public int term() {
        return currentTerm;
    }

    public int commitIndex() {
        return commitIndex;
    }

    public int snapshotIndex() {
        return log.lastIncludedIndex();
    }

    public int lastLogIndex() {
        return log.lastIndex();
    }

    public int termAt(int index) {
        return log.termAt(index);
    }

    public Command commandAt(int index) {
        return log.entryAt(index).command();
    }

    public int lastApplied() {
        return lastApplied;
    }

    public String leaderId() {
        return leaderId;
    }

    public Map<String, String> data() {
        return stateMachine.copy();
    }

    public Status status() {
        return new Status(
                id,
                roleName(),
                currentTerm,
                leaderId,
                commitIndex,
                lastApplied,
                log.lastIndex(),
                log.lastIncludedIndex(),
                votedFor);
    }

    public RaftView view() {
        return new RaftView(
                id,
                roleName(),
                currentTerm,
                commitIndex,
                lastApplied,
                log.lastIndex(),
                log.lastIncludedIndex(),
                votedFor,
                leaderId,
                log.tail(12));
    }

    public String dump() {
        return id + " role=" + roleName()
                + " term=" + currentTerm
                + " commit=" + commitIndex
                + " applied=" + lastApplied
                + " last=" + log.lastIndex()
                + " snap=" + log.lastIncludedIndex()
                + " voted=" + votedFor
                + " leader=" + leaderId
                + " data=" + stateMachine.copy();
    }

    /** Drop volatile leadership state. Durable state stays in {@link LogStore}. */
    public void crash() {
        crashed = true;
        role = Role.FOLLOWER;
        failInflight();
    }

    public void onTick() {
        if (crashed) {
            return;
        }
        long now = clock.millis();
        switch (role) {
            case LEADER -> {
                lastLeaderContactMs = now;
                if (!quorumAlive(now)) {
                    observer.note(id, "check-quorum step down");
                    becomeFollower(currentTerm, null);
                    return;
                }
                if (now >= nextHeartbeatAt) {
                    nextHeartbeatAt = now + config.heartbeatMs();
                    for (String peer : others) {
                        replicate(peer, true);
                    }
                }
                pumpReads();
            }
            case PRE_CANDIDATE -> {
                if (now >= electionDeadline) {
                    beginPreVote(now);
                } else if (now >= nextPreVoteSend) {
                    nextPreVoteSend = now + config.heartbeatMs();
                    sendPreVotes();
                }
            }
            case CANDIDATE -> {
                if (now >= electionDeadline) {
                    startElection(now);
                }
            }
            case FOLLOWER -> {
                if (now >= electionDeadline) {
                    if (peerIds.size() == 1) {
                        startElection(now);
                    } else {
                        beginPreVote(now);
                    }
                }
            }
        }
    }

    public void propose(Command command, Consumer<ClientResult> callback) {
        if (crashed || role != Role.LEADER) {
            callback.accept(ClientResult.notLeader(leaderId));
            return;
        }
        if (!quorumAlive(clock.millis())) {
            callback.accept(ClientResult.noQuorum(leaderId));
            return;
        }
        if (command.op() != Types.Op.NOOP) {
            KvStateMachine.Result duplicate = stateMachine.ifDuplicate(command);
            if (duplicate != null) {
                callback.accept(ClientResult.ok(lastApplied, duplicate.found(), duplicate.value(), id));
                return;
            }
            for (int index = lastApplied + 1; index <= log.lastIndex(); index++) {
                Command existing = log.entryAt(index).command();
                if (existing.clientId().equals(command.clientId()) && existing.seq() == command.seq()) {
                    pending.put(index, callback);
                    return;
                }
            }
        }
        appendLocal(command, callback);
        for (String peer : others) {
            replicate(peer);
        }
        advanceCommit();
    }

    public void linearizableGet(String key, Consumer<ClientResult> callback) {
        if (crashed || role != Role.LEADER) {
            callback.accept(ClientResult.notLeader(leaderId));
            return;
        }
        if (!quorumAlive(clock.millis())) {
            callback.accept(ClientResult.noQuorum(leaderId));
            return;
        }
        pendingReads.add(new PendingRead(key, callback));
        pumpReads();
    }

    public RequestVoteReply handleRequestVote(RequestVote request) {
        if (crashed) {
            return new RequestVoteReply(currentTerm, false, request.prevote(), request.term());
        }
        long now = clock.millis();
        if (!request.prevote() && request.term() > currentTerm) {
            becomeFollower(request.term(), null);
        }
        if (request.prevote()) {
            boolean grant = grantPreVote(request, now);
            observer.vote(id, request.candidateId(), grant, request.term(), true);
            return new RequestVoteReply(currentTerm, grant, true, request.term());
        }
        if (request.term() < currentTerm) {
            observer.vote(id, request.candidateId(), false, request.term(), false);
            return new RequestVoteReply(currentTerm, false, false, request.term());
        }
        boolean grant = false;
        if (logUpToDate(request.lastLogTerm(), request.lastLogIndex())
                && (votedFor == null || votedFor.equals(request.candidateId()))) {
            grant = true;
            votedFor = request.candidateId();
            store.storeMeta(currentTerm, votedFor);
            resetElectionDeadline(now);
        }
        observer.vote(id, request.candidateId(), grant, currentTerm, false);
        return new RequestVoteReply(currentTerm, grant, false, request.term());
    }

    public void handleRequestVoteReply(String from, RequestVoteReply reply) {
        if (crashed) {
            return;
        }
        if (reply.term() > currentTerm) {
            becomeFollower(reply.term(), null);
            return;
        }
        long now = clock.millis();
        if (reply.prevote()) {
            if (role != Role.PRE_CANDIDATE || reply.requestTerm() != preVoteTerm) {
                return;
            }
            if (reply.voteGranted() && preVoteGranted.add(from)) {
                if (preVoteGranted.size() >= majority()) {
                    startElection(now);
                }
            }
            return;
        }
        if (role != Role.CANDIDATE || reply.requestTerm() != currentTerm) {
            return;
        }
        if (reply.voteGranted() && voteGranted.add(from)) {
            lastContact.put(from, now);
            if (voteGranted.size() >= majority()) {
                becomeLeader(now);
            }
        }
    }

    public AppendEntriesReply handleAppendEntries(AppendEntries request) {
        if (crashed) {
            return rejectAppend(request, -1, -1);
        }
        if (request.term() > currentTerm) {
            becomeFollower(request.term(), request.leaderId());
        }
        if (request.term() < currentTerm) {
            return rejectAppend(request, -1, -1);
        }
        acknowledgeLeader(request.leaderId(), clock.millis());

        int prev = request.prevLogIndex();
        if (prev > log.lastIndex()) {
            return rejectAppend(request, log.lastIndex() + 1, -1);
        }
        if (prev >= log.lastIncludedIndex() && log.termAt(prev) != request.prevLogTerm()) {
            int conflictTerm = log.termAt(prev);
            int conflictIndex = log.firstIndexOfTerm(conflictTerm);
            return rejectAppend(request, conflictIndex < 0 ? prev : conflictIndex, conflictTerm);
        }
        int batchEnd = prev + request.entries().size();
        if (prev < log.lastIncludedIndex() && batchEnd < log.lastIncludedIndex()) {
            maybeAdvanceCommit(request.leaderCommit(), log.lastIncludedIndex());
            applyCommitted();
            return acceptAppend(request, log.lastIncludedIndex());
        }

        boolean truncated = false;
        ArrayList<LogEntry> fresh = new ArrayList<>();
        int index = prev;
        for (LogEntry entry : request.entries()) {
            index++;
            if (index < log.lastIncludedIndex()) {
                continue;
            }
            if (index == log.lastIncludedIndex()) {
                if (entry.term() != log.lastIncludedTerm()) {
                    return rejectAppend(request, log.lastIncludedIndex(), log.lastIncludedTerm());
                }
                continue;
            }
            if (index <= log.lastIndex() && log.termAt(index) == entry.term()) {
                continue;
            }
            if (index <= log.lastIndex()) {
                log.truncateFrom(index);
                truncated = true;
            }
            if (index != log.lastIndex() + 1) {
                persistLog(truncated, fresh);
                return rejectAppend(request, log.lastIndex() + 1, -1);
            }
            LogEntry stored = new LogEntry(index, entry.term(), entry.command());
            log.append(stored);
            fresh.add(stored);
        }
        persistLog(truncated, fresh);

        int matched = batchEnd < log.lastIncludedIndex()
                ? log.lastIncludedIndex()
                : Math.min(batchEnd, log.lastIndex());
        maybeAdvanceCommit(request.leaderCommit(), matched);
        applyCommitted();
        return acceptAppend(request, matched);
    }

    public void handleAppendEntriesReply(String from, AppendEntriesReply reply) {
        if (crashed) {
            return;
        }
        if (reply.term() > currentTerm) {
            becomeFollower(reply.term(), null);
            return;
        }
        if (role != Role.LEADER || reply.term() != currentTerm) {
            return;
        }
        long now = clock.millis();
        lastContact.put(from, now);
        countReadAck(from, reply.stamp());
        boolean current = reply.stamp() == outboundStamp.getOrDefault(from, Long.MIN_VALUE);
        if (current) {
            appendInFlight.put(from, false);
        }
        if (reply.success()) {
            int matched = Math.min(reply.matchIndex(), log.lastIndex());
            int known = matchIndex.getOrDefault(from, 0);
            if (matched > known) {
                matchIndex.put(from, matched);
                nextIndex.put(from, matched + 1);
            }
            advanceCommit();
            if (current && nextIndex.getOrDefault(from, 1) <= log.lastIndex()) {
                replicate(from);
            }
        } else if (current) {
            int hint;
            if (reply.conflictTerm() > 0) {
                int have = log.lastIndexOfTerm(reply.conflictTerm());
                hint = have > 0 ? have + 1 : Math.max(1, reply.conflictIndex());
            } else if (reply.conflictIndex() > 0) {
                hint = reply.conflictIndex();
            } else {
                hint = Math.max(1, nextIndex.getOrDefault(from, 1) - 1);
            }
            int floor = matchIndex.getOrDefault(from, 0) + 1;
            nextIndex.put(from, Math.max(floor, hint));
            replicate(from);
        }
        pumpReads();
    }

    public InstallSnapshotReply handleInstallSnapshot(InstallSnapshot request) {
        if (crashed) {
            return new InstallSnapshotReply(currentTerm, false, 0, request.stamp());
        }
        if (request.term() > currentTerm) {
            becomeFollower(request.term(), request.leaderId());
        }
        if (request.term() < currentTerm) {
            return new InstallSnapshotReply(currentTerm, false, 0, request.stamp());
        }
        acknowledgeLeader(request.leaderId(), clock.millis());
        if (request.lastIncludedIndex() <= log.lastIncludedIndex()) {
            // Not newer. Reply with the snapshot index rather than our longer suffix so the
            // leader resends later entries and overwrites a divergent uncommitted tail.
            return new InstallSnapshotReply(currentTerm, true, log.lastIncludedIndex(), request.stamp());
        }
        byte[] data = request.data().clone();
        stateMachine.restore(data, request.lastIncludedIndex());
        store.saveSnapshot(new LogStore.Snapshot(request.lastIncludedIndex(), request.lastIncludedTerm(), data));
        log.resetToSnapshot(request.lastIncludedIndex(), request.lastIncludedTerm());
        store.rewrite(List.of());
        commitIndex = request.lastIncludedIndex();
        lastApplied = request.lastIncludedIndex();
        observer.snapshot(id, request.lastIncludedIndex());
        return new InstallSnapshotReply(currentTerm, true, request.lastIncludedIndex(), request.stamp());
    }

    public void handleInstallSnapshotReply(String from, InstallSnapshotReply reply) {
        if (crashed) {
            return;
        }
        if (reply.term() > currentTerm) {
            becomeFollower(reply.term(), null);
            return;
        }
        if (role != Role.LEADER || reply.term() != currentTerm) {
            return;
        }
        lastContact.put(from, clock.millis());
        countReadAck(from, reply.stamp());
        boolean current = reply.stamp() == outboundStamp.getOrDefault(from, Long.MIN_VALUE);
        if (current) {
            appendInFlight.put(from, false);
        }
        if (reply.success()) {
            int matched = Math.min(reply.matchIndex(), log.lastIndex());
            if (matched > matchIndex.getOrDefault(from, 0)) {
                matchIndex.put(from, matched);
                nextIndex.put(from, matched + 1);
            }
            advanceCommit();
            if (current && (nextIndex.getOrDefault(from, 1) <= log.lastIndex()
                    || nextIndex.getOrDefault(from, 1) <= log.lastIncludedIndex())) {
                replicate(from);
            }
        }
        pumpReads();
    }

    private void beginPreVote(long now) {
        role = Role.PRE_CANDIDATE;
        preVoteTerm = currentTerm + 1;
        preVoteGranted.clear();
        preVoteGranted.add(id);
        resetElectionDeadline(now);
        nextPreVoteSend = now + config.heartbeatMs();
        observer.role(id, "pre-candidate", currentTerm);
        sendPreVotes();
    }

    private void sendPreVotes() {
        RequestVote request = new RequestVote(preVoteTerm, id, log.lastIndex(), log.lastTerm(), true);
        for (String peer : others) {
            transport.sendRequestVote(peer, request);
        }
    }

    private void startElection(long now) {
        role = Role.CANDIDATE;
        currentTerm++;
        votedFor = id;
        leaderId = null;
        store.storeMeta(currentTerm, votedFor);
        voteGranted.clear();
        voteGranted.add(id);
        resetElectionDeadline(now);
        observer.role(id, "candidate", currentTerm);
        if (voteGranted.size() >= majority()) {
            becomeLeader(now);
            return;
        }
        RequestVote request = new RequestVote(currentTerm, id, log.lastIndex(), log.lastTerm(), false);
        for (String peer : others) {
            transport.sendRequestVote(peer, request);
        }
    }

    private void becomeLeader(long now) {
        role = Role.LEADER;
        leaderId = id;
        everHeardLeader = true;
        lastLeaderContactMs = now;
        nextIndex.clear();
        matchIndex.clear();
        outboundStamp.clear();
        appendInFlight.clear();
        appendSentAt.clear();
        for (String peer : others) {
            nextIndex.put(peer, log.lastIndex() + 1);
            matchIndex.put(peer, 0);
        }
        observer.role(id, "leader", currentTerm);
        // A no-op in this term commits every earlier entry once a majority replicates it.
        // Until that happens, reads would be allowed to miss a committed prefix.
        appendLocal(Command.noop(), null);
        nextHeartbeatAt = now + config.heartbeatMs();
        for (String peer : others) {
            replicate(peer);
        }
        advanceCommit();
    }

    private void becomeFollower(int term, String leader) {
        Role previous = role;
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
            store.storeMeta(currentTerm, null);
        }
        role = Role.FOLLOWER;
        if (leader != null) {
            leaderId = leader;
        } else if (term > 0 && previous == Role.LEADER) {
            leaderId = null;
        }
        resetElectionDeadline(clock.millis());
        if (previous != Role.FOLLOWER) {
            failInflight();
            observer.role(id, "follower", currentTerm);
        }
    }

    private void acknowledgeLeader(String leader, long now) {
        everHeardLeader = true;
        lastLeaderContactMs = now;
        leaderId = leader;
        if (role != Role.FOLLOWER) {
            Role previous = role;
            role = Role.FOLLOWER;
            resetElectionDeadline(now);
            if (previous != Role.FOLLOWER) {
                failInflight();
                observer.role(id, "follower", currentTerm);
            }
        } else {
            resetElectionDeadline(now);
        }
    }

    private boolean grantPreVote(RequestVote request, long now) {
        if (request.term() <= currentTerm || role == Role.LEADER) {
            return false;
        }
        if (!logUpToDate(request.lastLogTerm(), request.lastLogIndex())) {
            return false;
        }
        // Before the first leader is known, the shortest timer should win immediately.
        // Afterwards a node grants a pre-vote once the leader has been silent for the
        // grant window, even if this node's own (longer) election timer has not fired.
        if (!everHeardLeader) {
            return true;
        }
        return now - lastLeaderContactMs >= config.grantSilenceMs();
    }

    private boolean logUpToDate(int lastLogTerm, int lastLogIndex) {
        if (lastLogTerm != log.lastTerm()) {
            return lastLogTerm > log.lastTerm();
        }
        return lastLogIndex >= log.lastIndex();
    }

    private void appendLocal(Command command, Consumer<ClientResult> callback) {
        int index = log.lastIndex() + 1;
        LogEntry entry = new LogEntry(index, currentTerm, command);
        store.append(List.of(entry));
        log.append(entry);
        if (callback != null) {
            pending.put(index, callback);
        }
    }

    private void replicate(String peer) {
        replicate(peer, false);
    }

    /**
     * At most one append or snapshot is in flight to each peer. Further entries wait and
     * leave in the next batch, which is what keeps a busy leader from resending the same
     * prefix on every client call. A heartbeat may assume the previous RPC was lost.
     */
    private void replicate(String peer, boolean heartbeat) {
        if (crashed || role != Role.LEADER) {
            return;
        }
        if (appendInFlight.getOrDefault(peer, false)) {
            long sent = appendSentAt.getOrDefault(peer, Long.MIN_VALUE);
            if (!heartbeat || clock.millis() - sent < config.heartbeatMs()) {
                return;
            }
        }
        int next = nextIndex.getOrDefault(peer, log.lastIndex() + 1);
        if (next <= log.lastIncludedIndex()) {
            sendSnapshot(peer);
            return;
        }
        int prev = next - 1;
        List<LogEntry> entries = log.slice(next, log.lastIndex(), config.maxAppendEntries());
        long stamp = nextStamp(peer);
        transport.sendAppendEntries(peer, new AppendEntries(
                currentTerm,
                id,
                prev,
                log.termAt(prev),
                entries,
                commitIndex,
                stamp));
        appendInFlight.put(peer, true);
        appendSentAt.put(peer, clock.millis());
    }

    private void sendSnapshot(String peer) {
        LogStore.Snapshot snapshot = store.loadSnapshot();
        if (snapshot == null) {
            throw new IllegalStateException(id + " missing snapshot below index " + log.lastIncludedIndex());
        }
        long stamp = nextStamp(peer);
        transport.sendInstallSnapshot(peer, new InstallSnapshot(
                currentTerm,
                id,
                snapshot.lastIncludedIndex(),
                snapshot.lastIncludedTerm(),
                snapshot.data(),
                stamp));
        appendInFlight.put(peer, true);
        appendSentAt.put(peer, clock.millis());
    }

    private long nextStamp(String peer) {
        long stamp = ++stampSeq;
        outboundStamp.put(peer, stamp);
        return stamp;
    }

    private void advanceCommit() {
        int before = commitIndex;
        for (int index = log.lastIndex(); index > commitIndex; index--) {
            if (index <= log.lastIncludedIndex()) {
                break;
            }
            if (log.termAt(index) != currentTerm) {
                break;
            }
            int replicas = 1;
            for (String peer : others) {
                if (matchIndex.getOrDefault(peer, 0) >= index) {
                    replicas++;
                }
            }
            if (replicas >= majority()) {
                commitIndex = index;
                break;
            }
        }
        applyCommitted();
        if (role == Role.LEADER && commitIndex > before) {
            for (String peer : others) {
                replicate(peer);
            }
        }
    }

    private void maybeAdvanceCommit(int leaderCommit, int matched) {
        if (leaderCommit <= commitIndex) {
            return;
        }
        int next = Math.min(leaderCommit, matched);
        next = Math.min(next, log.lastIndex());
        if (next > commitIndex) {
            commitIndex = next;
        }
    }

    private void applyCommitted() {
        boolean applied = false;
        while (lastApplied < commitIndex) {
            int index = lastApplied + 1;
            if (index <= log.lastIncludedIndex()) {
                lastApplied = log.lastIncludedIndex();
                continue;
            }
            LogEntry entry = log.entryAt(index);
            KvStateMachine.Result result = stateMachine.apply(entry);
            lastApplied = index;
            applied = true;
            Consumer<ClientResult> callback = pending.remove(index);
            if (callback != null) {
                callback.accept(ClientResult.ok(index, result.found(), result.value(), id));
            }
            observer.commit(id, index);
        }
        if (applied) {
            maybeSnapshot();
        }
        pumpReads();
    }

    private void maybeSnapshot() {
        if (lastApplied - log.lastIncludedIndex() < config.snapshotEvery() || lastApplied <= 0) {
            return;
        }
        int index = lastApplied;
        int term = log.termAt(index);
        byte[] data = stateMachine.snapshot();
        store.saveSnapshot(new LogStore.Snapshot(index, term, data));
        log.compact(index, term);
        store.rewrite(log.all());
        observer.snapshot(id, index);
    }

    private void pumpReads() {
        if (role != Role.LEADER || !currentTermCommitted()) {
            return;
        }
        for (PendingRead read : pendingReads) {
            if (read.done) {
                continue;
            }
            if (!read.confirming) {
                read.confirming = true;
                read.readIndex = commitIndex;
                read.acks = 1;
                for (String peer : others) {
                    replicate(peer);
                    Long stamp = outboundStamp.get(peer);
                    if (stamp != null) {
                        read.needStamp.put(peer, stamp);
                    }
                }
            }
            if (read.acks >= majority() && lastApplied >= read.readIndex) {
                read.done = true;
                String value = stateMachine.get(read.key);
                read.callback.accept(ClientResult.ok(read.readIndex, value != null, value, id));
            }
        }
        pendingReads.removeIf(read -> read.done);
    }

    private void countReadAck(String from, long stamp) {
        for (PendingRead read : pendingReads) {
            Long need = read.needStamp.get(from);
            if (need != null && stamp >= need) {
                read.needStamp.remove(from);
                read.acks++;
            }
        }
    }

    private boolean currentTermCommitted() {
        if (commitIndex <= 0) {
            return false;
        }
        if (commitIndex == log.lastIncludedIndex()) {
            return log.lastIncludedTerm() == currentTerm;
        }
        if (commitIndex > log.lastIndex()) {
            return false;
        }
        return log.termAt(commitIndex) == currentTerm;
    }

    private boolean quorumAlive(long now) {
        long windowStart = now - config.electionMaxMs();
        int count = 1;
        for (String peer : others) {
            Long seen = lastContact.get(peer);
            if (seen != null && seen >= windowStart) {
                count++;
            }
        }
        return count >= majority();
    }

    private void failInflight() {
        List<Consumer<ClientResult>> writes = new ArrayList<>(pending.values());
        pending.clear();
        for (Consumer<ClientResult> callback : writes) {
            callback.accept(ClientResult.unknown(leaderId));
        }
        List<PendingRead> reads = new ArrayList<>(pendingReads);
        pendingReads.clear();
        for (PendingRead read : reads) {
            if (!read.done) {
                read.done = true;
                read.callback.accept(ClientResult.notLeader(leaderId));
            }
        }
    }

    private void persistLog(boolean truncated, List<LogEntry> fresh) {
        if (truncated) {
            store.rewrite(log.all());
        } else if (!fresh.isEmpty()) {
            store.append(fresh);
        }
    }

    private void resetElectionDeadline(long now) {
        int span = config.electionMaxMs() - config.electionMinMs();
        int extra = span == 0 ? 0 : random.nextInt(span + 1);
        electionDeadline = now + config.electionMinMs() + extra;
    }

    private int majority() {
        return peerIds.size() / 2 + 1;
    }

    private String roleName() {
        return role.name().toLowerCase().replace('_', '-');
    }

    private AppendEntriesReply rejectAppend(AppendEntries request, int conflictIndex, int conflictTerm) {
        return new AppendEntriesReply(currentTerm, false, 0, conflictIndex, conflictTerm, request.stamp());
    }

    private AppendEntriesReply acceptAppend(AppendEntries request, int matched) {
        return new AppendEntriesReply(currentTerm, true, matched, -1, -1, request.stamp());
    }

    private static final class PendingRead {
        private final String key;
        private final Consumer<ClientResult> callback;
        private final Map<String, Long> needStamp = new HashMap<>();
        private boolean confirming;
        private int readIndex;
        private int acks;
        private boolean done;

        private PendingRead(String key, Consumer<ClientResult> callback) {
            this.key = key;
            this.callback = callback;
        }
    }

}
