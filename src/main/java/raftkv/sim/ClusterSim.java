package raftkv.sim;

import raftkv.json.Json;
import raftkv.kv.KvStateMachine;
import raftkv.linear.History;
import raftkv.persist.LogStore;
import raftkv.persist.MemoryLogStore;
import raftkv.raft.RaftNode;
import raftkv.raft.Types.AppendEntries;
import raftkv.raft.Types.AppendEntriesReply;
import raftkv.raft.Types.ClientResult;
import raftkv.raft.Types.ClientStatus;
import raftkv.raft.Types.Clock;
import raftkv.raft.Types.Command;
import raftkv.raft.Types.InstallSnapshot;
import raftkv.raft.Types.InstallSnapshotReply;
import raftkv.raft.Types.JdkRandom;
import raftkv.raft.Types.LogCell;
import raftkv.raft.Types.Observer;
import raftkv.raft.Types.Op;
import raftkv.raft.Types.RaftConfig;
import raftkv.raft.Types.RaftView;
import raftkv.raft.Types.RequestVote;
import raftkv.raft.Types.RequestVoteReply;
import raftkv.raft.Types.Role;
import raftkv.raft.Types.Transport;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;

/**
 * Discrete-event cluster. One virtual clock, deterministic delivery, and injected
 * loss, reordering (via random delay), partitions, and crashes. Raft nodes see only
 * their own timeouts; nothing in the protocol reads another node's clock.
 */
public final class ClusterSim implements Observer {
    @FunctionalInterface
    public interface DropRule {
        boolean drop(String from, String to, Object message);
    }

    private final SimClock clock = new SimClock();
    private final PriorityQueue<Ev> queue = new PriorityQueue<>();
    private final ArrayDeque<Runnable> deferred = new ArrayDeque<>();
    private final ArrayDeque<String> debugLog = new ArrayDeque<>();
    private final Map<String, Holder> byId = new LinkedHashMap<>();
    private final List<String> ids;
    private final History history = new History();
    private final Json.Arr events = new Json.Arr();
    private final Map<Integer, String> termLeader = new HashMap<>();
    private final Map<String, String> prefer = new HashMap<>();
    private final Map<String, Integer> cursor = new HashMap<>();
    private final Map<String, Long> seqs = new HashMap<>();
    private final Random rng;
    private final long seed;
    private final int tickMs;
    private final boolean trace;
    private long seq;
    private boolean running;
    private int minDelay;
    private int maxDelay;
    private double loss;
    private List<Set<String>> groups;
    private DropRule drop;

    private ClusterSim(Builder builder) {
        this.seed = builder.seed;
        this.rng = new Random(builder.seed);
        this.tickMs = builder.tickMs;
        this.trace = builder.trace;
        this.minDelay = builder.minDelay;
        this.maxDelay = builder.maxDelay;
        this.loss = builder.loss;
        int n = builder.nodes;
        ArrayList<String> idList = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            idList.add("n" + (i + 1));
        }
        this.ids = List.copyOf(idList);
        for (int i = 0; i < n; i++) {
            String id = ids.get(i);
            RaftConfig config = builder.configFor(i);
            JdkRandom random = new JdkRandom(builder.seed * 997 + i * 17L);
            MemoryLogStore store = new MemoryLogStore();
            Net net = new Net(id);
            RaftNode node = new RaftNode(
                    id, ids, store, new KvStateMachine(), net, config, clock, random, this);
            byId.put(id, new Holder(id, store, node, random, config));
        }
        for (String id : ids) {
            armTick(id);
        }
        if (trace) {
            addState();
            armKeyframe();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public long now() {
        return clock.now;
    }

    public List<String> ids() {
        return ids;
    }

    public History history() {
        return history;
    }

    public long seed() {
        return seed;
    }

    public void setDelay(int min, int max) {
        if (max < min || min < 0) {
            throw new IllegalArgumentException("delay");
        }
        this.minDelay = min;
        this.maxDelay = max;
    }

    public void setLoss(double probability) {
        if (probability < 0 || probability > 1) {
            throw new IllegalArgumentException("loss");
        }
        this.loss = probability;
    }

    public void setDrop(DropRule drop) {
        this.drop = drop;
    }

    public void runFor(long ms) {
        runUntil(clock.now + ms);
    }

    public void runUntil(long target) {
        runUntil(target, () -> false);
    }

    /**
     * Advance to {@code target}, or stop at the event that makes {@code stop} true.
     * A completed client call must not burn the rest of its timeout: the next call
     * starts at the virtual time the previous one actually returned.
     */
    private void runUntil(long target, java.util.function.BooleanSupplier stop) {
        if (running) {
            throw new IllegalStateException("re-entrant runUntil");
        }
        running = true;
        try {
            while (!stop.getAsBoolean() && !queue.isEmpty() && queue.peek().time <= target) {
                if (queue.size() > 250_000) {
                    throw new IllegalStateException("event explosion\n" + debug());
                }
                Ev event = queue.poll();
                clock.now = event.time;
                event.run.run();
                pumpDeferred();
            }
            if (!stop.getAsBoolean()) {
                clock.now = Math.max(clock.now, target);
            }
        } finally {
            running = false;
        }
    }

    @SafeVarargs
    public final void cut(Set<String>... parts) {
        ArrayList<Set<String>> copy = new ArrayList<>();
        for (Set<String> part : parts) {
            copy.add(new LinkedHashSet<>(part));
        }
        partition(copy);
    }

    public void partition(List<Set<String>> parts) {
        this.groups = new ArrayList<>();
        Json.Arr encoded = new Json.Arr();
        for (Set<String> part : parts) {
            LinkedHashSet<String> copy = new LinkedHashSet<>(part);
            groups.add(copy);
            Json.Arr one = new Json.Arr();
            for (String id : copy) {
                one.add(id);
            }
            encoded.add(one);
        }
        dbg("partition " + groups);
        if (trace) {
            events.add(new Json.Obj().put("t", clock.now).put("kind", "partition").put("groups", encoded));
            addState();
        }
    }

    public void heal() {
        groups = null;
        dbg("heal");
        if (trace) {
            events.add(new Json.Obj().put("t", clock.now).put("kind", "heal"));
            addState();
        }
    }

    public void crash(String id) {
        Holder holder = holder(id);
        if (holder.crashed) {
            return;
        }
        holder.lastView = holder.node.view();
        holder.crashed = true;
        holder.node.crash();
        dbg("crash " + id);
        if (trace) {
            events.add(new Json.Obj().put("t", clock.now).put("kind", "crash").put("node", id));
            addState();
        }
    }

    public void restart(String id) {
        Holder holder = holder(id);
        if (!holder.crashed) {
            return;
        }
        Net net = new Net(id);
        holder.node = new RaftNode(
                id,
                ids,
                holder.store,
                new KvStateMachine(),
                net,
                holder.config,
                clock,
                holder.random,
                this);
        holder.crashed = false;
        dbg("restart " + id + " " + holder.node.dump());
        if (trace) {
            events.add(new Json.Obj().put("t", clock.now).put("kind", "restart").put("node", id));
            addState();
        }
    }

    public ClientResult put(String clientId, String key, String value, long timeoutMs) {
        return submit(clientId, Op.PUT, key, value, timeoutMs);
    }

    public ClientResult delete(String clientId, String key, long timeoutMs) {
        return submit(clientId, Op.DELETE, key, null, timeoutMs);
    }

    public ClientResult get(String clientId, String key, long timeoutMs) {
        return submit(clientId, null, key, null, timeoutMs);
    }

    /** One attempt, no history. Used to retry the same client sequence on purpose. */
    public ClientResult proposeOnce(String nodeId, Command command, long timeoutMs) {
        ClientResult[] box = new ClientResult[1];
        at(0, () -> holder(nodeId).node.propose(command, result -> defer(() -> box[0] = result)));
        runUntil(clock.now + timeoutMs);
        return box[0] == null ? ClientResult.unknown(nodeId) : box[0];
    }

    public String awaitLeader(long timeoutMs) {
        long deadline = clock.now + timeoutMs;
        while (clock.now < deadline) {
            List<String> leaders = leaderIds();
            if (leaders.size() == 1) {
                return leaders.get(0);
            }
            runFor(tickMs);
        }
        throw new IllegalStateException("no single leader\n" + dump());
    }

    public void awaitConverged(long timeoutMs) {
        long deadline = clock.now + timeoutMs;
        while (clock.now < deadline) {
            if (converged()) {
                return;
            }
            runFor(tickMs);
        }
        throw new IllegalStateException("not converged\n" + dump());
    }

    public boolean converged() {
        List<Holder> alive = alive();
        if (alive.size() < 2) {
            return !alive.isEmpty();
        }
        Holder base = alive.get(0);
        Map<String, String> data = base.node.data();
        int commit = base.node.commitIndex();
        for (int i = 1; i < alive.size(); i++) {
            Holder other = alive.get(i);
            if (other.node.commitIndex() != commit) {
                return false;
            }
            if (!other.node.data().equals(data)) {
                return false;
            }
            if (!samePrefix(base.node, other.node, commit)) {
                return false;
            }
        }
        return true;
    }

    public List<String> leaderIds() {
        ArrayList<String> leaders = new ArrayList<>();
        for (Holder holder : byId.values()) {
            if (!holder.crashed && holder.node.role() == Role.LEADER) {
                leaders.add(holder.id);
            }
        }
        return leaders;
    }

    public Map<String, String> data(String id) {
        return holder(id).node.data();
    }

    public RaftView view(String id) {
        Holder holder = holder(id);
        if (holder.crashed) {
            return holder.lastView;
        }
        return holder.node.view();
    }

    public int commitIndex(String id) {
        return holder(id).node.commitIndex();
    }

    public String dump() {
        StringBuilder out = new StringBuilder();
        for (Holder holder : byId.values()) {
            out.append(holder.crashed ? holder.id + " CRASHED " + holder.lastView : holder.node.dump());
            out.append('\n');
        }
        out.append(debug());
        return out.toString();
    }

    public String debug() {
        return String.join("\n", debugLog);
    }

    public Json.Obj traceDocument(String id, String title, String blurb) {
        Json.Arr nodes = new Json.Arr();
        for (String node : ids) {
            nodes.add(node);
        }
        return new Json.Obj()
                .put("id", id)
                .put("title", title)
                .put("blurb", blurb)
                .put("seed", seed)
                .put("nodes", nodes)
                .put("durationMs", clock.now)
                .put("events", events);
    }

    @Override
    public void role(String id, String role, int term) {
        dbg(id + " -> " + role + " t=" + term);
        if ("leader".equals(role)) {
            String previous = termLeader.put(term, id);
            if (previous != null && !previous.equals(id)) {
                throw new IllegalStateException(
                        "two leaders in term " + term + ": " + previous + " and " + id + "\n" + dump());
            }
        }
        if (trace) {
            events.add(new Json.Obj()
                    .put("t", clock.now)
                    .put("kind", "role")
                    .put("node", id)
                    .put("role", role)
                    .put("term", term));
            addState();
        }
    }

    @Override
    public void note(String id, String message) {
        dbg(id + " " + message);
    }

    @Override
    public void snapshot(String id, int index) {
        dbg(id + " snapshot " + index);
        if (trace) {
            events.add(new Json.Obj()
                    .put("t", clock.now)
                    .put("kind", "snapshot")
                    .put("node", id)
                    .put("index", index));
        }
    }

    private ClientResult submit(String clientId, Op op, String key, String value, long timeoutMs) {
        boolean read = op == null;
        boolean delete = op == Op.DELETE;
        long commandSeq = read ? 0 : seqs.merge(clientId, 1L, Long::sum);
        Command command = read ? null : new Command(op, clientId, commandSeq, key, value == null ? "" : value);
        int historyId = read
                ? history.beginRead(clock.now, key)
                : history.beginWrite(clock.now, key, delete ? null : value, delete);
        ClientResult[] box = new ClientResult[1];
        boolean[] sawUnknown = {false};
        boolean[] sawReply = {false};
        // True from the moment propose/get is invoked until its callback runs.
        // A timed-out call may already be in the log, so it must not be recorded as FAIL.
        boolean[] inFlight = {false};
        long deadline = clock.now + timeoutMs;
        class Attempt {
            void go() {
                if (box[0] != null) {
                    return;
                }
                if (clock.now > deadline) {
                    finish(null);
                    return;
                }
                String target = pick(clientId);
                if (target == null) {
                    at(tickMs, this::go);
                    return;
                }
                Holder holder = holder(target);
                if (holder.crashed) {
                    at(tickMs, this::go);
                    return;
                }
                java.util.function.Consumer<ClientResult> callback = result -> defer(() -> {
                    inFlight[0] = false;
                    if (box[0] != null) {
                        return;
                    }
                    sawReply[0] = true;
                    switch (result.status()) {
                        case OK -> finish(result);
                        case UNKNOWN -> {
                            sawUnknown[0] = true;
                            at(tickMs, Attempt.this::go);
                        }
                        case NOT_LEADER, NO_QUORUM -> {
                            if (result.leaderId() != null && !result.leaderId().equals(target)) {
                                prefer.put(clientId, result.leaderId());
                            } else {
                                prefer.remove(clientId);
                            }
                            at(tickMs, Attempt.this::go);
                        }
                    }
                });
                inFlight[0] = true;
                if (read) {
                    holder.node.linearizableGet(key, callback);
                } else {
                    holder.node.propose(command, callback);
                }
            }

            void finish(ClientResult result) {
                if (box[0] != null) {
                    return;
                }
                History.Status status;
                String readValue = null;
                if (result != null && result.status() == ClientStatus.OK) {
                    status = History.Status.OK;
                    readValue = result.value();
                    if (result.leaderId() != null) {
                        prefer.put(clientId, result.leaderId());
                    }
                } else if (read) {
                    status = History.Status.FAIL;
                } else if (sawUnknown[0] || inFlight[0] || !sawReply[0]) {
                    status = History.Status.INFO;
                } else {
                    status = History.Status.FAIL;
                }
                history.end(historyId, clock.now, status, read ? readValue : null);
                box[0] = result == null
                        ? (status == History.Status.INFO ? ClientResult.unknown(null) : ClientResult.notLeader(null))
                        : result;
                if (trace) {
                    Json.Obj event = new Json.Obj()
                            .put("t", clock.now)
                            .put("kind", "client")
                            .put("op", read ? "get" : delete ? "delete" : "put")
                            .put("key", key)
                            .put("status", status.name().toLowerCase())
                            .put("client", clientId);
                    if (!delete && value != null) {
                        event.put("value", value);
                    }
                    if (read && status == History.Status.OK) {
                        event.put("value", readValue);
                        event.put("found", readValue != null);
                    }
                    events.add(event);
                }
                dbg("client " + clientId + " " + (read ? "get " : "put ") + key + " -> " + status);
            }
        }
        Attempt attempt = new Attempt();
        at(0, attempt::go);
        runUntil(deadline + tickMs, () -> box[0] != null);
        if (box[0] == null) {
            attempt.finish(null);
        }
        return box[0];
    }

    private String pick(String clientId) {
        String preferred = prefer.get(clientId);
        if (preferred != null) {
            Holder holder = byId.get(preferred);
            if (holder != null && !holder.crashed) {
                return preferred;
            }
        }
        ArrayList<String> alive = new ArrayList<>();
        for (Holder holder : byId.values()) {
            if (!holder.crashed) {
                alive.add(holder.id);
            }
        }
        if (alive.isEmpty()) {
            return null;
        }
        int next = cursor.merge(clientId, 1, Integer::sum);
        return alive.get(Math.floorMod(next, alive.size()));
    }

    private void armTick(String id) {
        at(tickMs, () -> {
            Holder holder = holder(id);
            if (!holder.crashed) {
                holder.node.onTick();
            }
            armTick(id);
        });
    }

    private void armKeyframe() {
        at(40, () -> {
            addState();
            armKeyframe();
        });
    }

    private void addState() {
        Json.Obj nodes = new Json.Obj();
        for (Holder holder : byId.values()) {
            RaftView view = holder.crashed ? holder.lastView : holder.node.view();
            if (!holder.crashed) {
                holder.lastView = view;
            }
            nodes.put(holder.id, view == null ? down(holder.id) : viewJson(view, holder.crashed));
        }
        events.add(new Json.Obj().put("t", clock.now).put("kind", "state").put("nodes", nodes));
    }

    private static Json.Obj down(String id) {
        return new Json.Obj().put("role", "down").put("crashed", true).put("term", 0).put("commit", 0);
    }

    private static Json.Obj viewJson(RaftView view, boolean crashed) {
        Json.Obj obj = new Json.Obj()
                .put("role", crashed ? "down" : view.role())
                .put("term", view.term())
                .put("commit", view.commit())
                .put("applied", view.applied())
                .put("lastIndex", view.lastIndex())
                .put("snapshot", view.snapshotIndex())
                .put("votedFor", view.votedFor())
                .put("leader", view.leaderId())
                .put("crashed", crashed);
        Json.Arr log = new Json.Arr();
        for (LogCell cell : view.log()) {
            Json.Obj entry = new Json.Obj()
                    .put("i", cell.index())
                    .put("t", cell.term())
                    .put("op", cell.op().name());
            if (cell.key() != null && !cell.key().isEmpty()) {
                entry.put("key", cell.key());
            }
            if (cell.value() != null && !cell.value().isEmpty()) {
                entry.put("value", cell.value());
            }
            log.add(entry);
        }
        obj.put("log", log);
        return obj;
    }

    private boolean samePrefix(RaftNode left, RaftNode right, int commit) {
        int start = Math.max(left.snapshotIndex(), right.snapshotIndex()) + 1;
        for (int index = start; index <= commit; index++) {
            if (index > left.lastLogIndex() || index > right.lastLogIndex()) {
                return false;
            }
            if (left.termAt(index) != right.termAt(index)) {
                return false;
            }
            if (!left.commandAt(index).equals(right.commandAt(index))) {
                return false;
            }
        }
        return true;
    }

    private List<Holder> alive() {
        ArrayList<Holder> alive = new ArrayList<>();
        for (Holder holder : byId.values()) {
            if (!holder.crashed) {
                alive.add(holder);
            }
        }
        return alive;
    }

    private Holder holder(String id) {
        Holder holder = byId.get(id);
        if (holder == null) {
            throw new IllegalArgumentException("unknown node " + id);
        }
        return holder;
    }

    private void at(long delay, Runnable run) {
        queue.add(new Ev(clock.now + Math.max(0, delay), seq++, run));
    }

    private void pumpDeferred() {
        int guard = 0;
        while (!deferred.isEmpty()) {
            if (++guard > 20_000) {
                throw new IllegalStateException("deferred explosion");
            }
            deferred.poll().run();
        }
    }

    private void defer(Runnable run) {
        deferred.add(run);
    }

    private void dbg(String line) {
        if (debugLog.size() == 300) {
            debugLog.poll();
        }
        debugLog.add(clock.now + " " + line);
    }

    private void dispatch(String from, String to, Object message, Runnable deliver) {
        if (drop != null && drop.drop(from, to, message)) {
            dbg("drop " + from + "->" + to);
            return;
        }
        if (!sameGroup(from, to)) {
            dbg("partition-drop " + from + "->" + to);
            return;
        }
        if (loss > 0 && rng.nextDouble() < loss) {
            dbg("loss " + from + "->" + to);
            return;
        }
        int span = maxDelay - minDelay;
        int delay = minDelay + (span > 0 ? rng.nextInt(span + 1) : 0);
        at(delay, deliver);
    }

    private boolean sameGroup(String from, String to) {
        if (from.equals(to) || groups == null) {
            return true;
        }
        for (Set<String> group : groups) {
            if (group.contains(from) && group.contains(to)) {
                return true;
            }
        }
        return false;
    }

    private final class Net implements Transport {
        private final String self;

        private Net(String self) {
            this.self = self;
        }

        @Override
        public void sendRequestVote(String to, RequestVote message) {
            if (trace) {
                traceRpc(message.prevote() ? "PreVote" : "RequestVote", self, to, message.term(), null, 0, false);
            }
            dispatch(self, to, message, () -> {
                Holder target = holder(to);
                if (target.crashed) {
                    return;
                }
                RequestVoteReply reply = target.node.handleRequestVote(message);
                if (trace) {
                    traceRpc(message.prevote() ? "PreVoteReply" : "VoteReply", to, self, reply.term(), reply.voteGranted(), 0, false);
                }
                dispatch(to, self, reply, () -> {
                    Holder source = holder(self);
                    if (!source.crashed) {
                        source.node.handleRequestVoteReply(to, reply);
                    }
                });
            });
        }

        @Override
        public void sendAppendEntries(String to, AppendEntries message) {
            boolean heartbeat = message.entries().isEmpty();
            if (trace && !heartbeat) {
                traceRpc("AppendEntries", self, to, message.term(), null, message.entries().size(), false);
            }
            dispatch(self, to, message, () -> {
                Holder target = holder(to);
                if (target.crashed) {
                    return;
                }
                AppendEntriesReply reply = target.node.handleAppendEntries(message);
                if (trace && (!heartbeat || !reply.success())) {
                    traceRpc(reply.success() ? "AppendOk" : "AppendReject", to, self, reply.term(), reply.success(), message.entries().size(), heartbeat);
                }
                dispatch(to, self, reply, () -> {
                    Holder source = holder(self);
                    if (!source.crashed) {
                        source.node.handleAppendEntriesReply(to, reply);
                    }
                });
            });
        }

        @Override
        public void sendInstallSnapshot(String to, InstallSnapshot message) {
            if (trace) {
                traceRpc("InstallSnapshot", self, to, message.term(), null, 0, false);
            }
            dispatch(self, to, message, () -> {
                Holder target = holder(to);
                if (target.crashed) {
                    return;
                }
                InstallSnapshotReply reply = target.node.handleInstallSnapshot(message);
                if (trace) {
                    traceRpc("SnapshotReply", to, self, reply.term(), reply.success(), 0, false);
                }
                dispatch(to, self, reply, () -> {
                    Holder source = holder(self);
                    if (!source.crashed) {
                        source.node.handleInstallSnapshotReply(to, reply);
                    }
                });
            });
        }
    }

    private void traceRpc(String rpc, String from, String to, int term, Boolean ok, int entries, boolean heartbeat) {
        Json.Obj event = new Json.Obj()
                .put("t", clock.now)
                .put("kind", "rpc")
                .put("rpc", rpc)
                .put("from", from)
                .put("to", to)
                .put("term", term);
        if (ok != null) {
            event.put("ok", ok);
        }
        if (entries > 0) {
            event.put("entries", entries);
        }
        if (heartbeat) {
            event.put("heartbeat", true);
        }
        events.add(event);
    }

    private static final class Holder {
        final String id;
        final LogStore store;
        final JdkRandom random;
        final RaftConfig config;
        RaftNode node;
        RaftView lastView;
        boolean crashed;

        Holder(String id, LogStore store, RaftNode node, JdkRandom random, RaftConfig config) {
            this.id = id;
            this.store = store;
            this.node = node;
            this.random = random;
            this.config = config;
        }
    }

    private record Ev(long time, long seq, Runnable run) implements Comparable<Ev> {
        @Override
        public int compareTo(Ev other) {
            int byTime = Long.compare(time, other.time);
            return byTime != 0 ? byTime : Long.compare(seq, other.seq);
        }
    }

    private static final class SimClock implements Clock {
        private long now;

        @Override
        public long millis() {
            return now;
        }
    }

    public static final class Builder {
        private int nodes = 3;
        private long seed = 1;
        private int minDelay = 5;
        private int maxDelay = 5;
        private double loss;
        private int tickMs = 10;
        private int heartbeat = 20;
        private int snapshotEvery = 48;
        private boolean trace;
        private Integer uniformMin;
        private Integer uniformMax;
        private Integer uniformGrant;

        public Builder nodes(int nodes) {
            this.nodes = nodes;
            return this;
        }

        public Builder seed(long seed) {
            this.seed = seed;
            return this;
        }

        public Builder delay(int min, int max) {
            this.minDelay = min;
            this.maxDelay = max;
            return this;
        }

        public Builder loss(double loss) {
            this.loss = loss;
            return this;
        }

        public Builder heartbeat(int heartbeat) {
            this.heartbeat = heartbeat;
            return this;
        }

        public Builder snapshotEvery(int entries) {
            this.snapshotEvery = entries;
            return this;
        }

        public Builder trace(boolean trace) {
            this.trace = trace;
            return this;
        }

        /** Use one election window for every node. */
        public Builder election(int min, int max, int grantSilence) {
            this.uniformMin = min;
            this.uniformMax = max;
            this.uniformGrant = grantSilence;
            return this;
        }

        public ClusterSim start() {
            if (nodes < 1 || nodes > 5) {
                throw new IllegalArgumentException("cluster size must be 1..5");
            }
            return new ClusterSim(this);
        }

        private RaftConfig configFor(int index) {
            if (uniformMin != null) {
                return RaftConfig.sim(uniformMin, uniformMax, heartbeat, snapshotEvery, uniformGrant);
            }
            if (index == 0) {
                return RaftConfig.sim(100, 100, heartbeat, snapshotEvery, 100);
            }
            int timer = 400 + index * 200;
            return RaftConfig.sim(timer, timer, heartbeat, snapshotEvery, Math.max(heartbeat + 1, 140));
        }
    }

}
