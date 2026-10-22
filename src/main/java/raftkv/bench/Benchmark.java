package raftkv.bench;

import raftkv.client.KvClient;
import raftkv.json.Json;
import raftkv.kv.KvStateMachine;
import raftkv.persist.FileLogStore;
import raftkv.persist.LogStore;
import raftkv.persist.MemoryLogStore;
import raftkv.raft.RaftNode;
import raftkv.raft.Types.ClientResult;
import raftkv.raft.Types.ClientStatus;
import raftkv.raft.Types.Command;
import raftkv.raft.Types.JdkRandom;
import raftkv.raft.Types.Observer;
import raftkv.raft.Types.RaftConfig;
import raftkv.raft.Types.SystemClock;
import raftkv.raft.Types.Transport;
import raftkv.server.HttpCluster;
import raftkv.server.NodeLoop;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Measures client-observed throughput and latency. Numbers written by this main are from a
 * single run on this machine; nothing here is estimated.
 */
public final class Benchmark {
    /** One client thread, this many calls in flight. Extra client threads just fight the replicas for cores. */
    private static final int PIPELINE = 8;
    private static final int HTTP_CLIENTS = 4;
    private static final int KEYS = 256;
    private static final String VALUE = "0123456789abcdef";
    private static final long WARMUP_MS = 1_000;
    private static final long MEASURE_MS = 4_000;

    public static void main(String[] args) throws Exception {
        RaftConfig config = RaftConfig.sim(400, 400, 30, 1_000_000, 400);
        // Discarded pass so later samples are not the first time the JIT sees replication.
        inProcess(3, "memory", "put", false, config);
        List<Sample> samples = new ArrayList<>();
        for (int nodes : List.of(1, 3, 5)) {
            samples.add(inProcess(nodes, "memory", "put", false, config));
            samples.add(inProcess(nodes, "memory", "mixed", false, config));
        }
        for (int nodes : List.of(1, 3, 5)) {
            samples.add(inProcess(nodes, "fsync", "put", true, config));
        }
        samples.add(httpPut(config));
        Path out = Path.of("results", "benchmarks.json");
        Files.createDirectories(out.getParent());
        String json = document(samples);
        Files.writeString(out, json);
        System.out.println(json);
    }

    private static Sample inProcess(int nodes, String durability, String workload, boolean fsync, RaftConfig config)
            throws Exception {
        System.out.println("bench " + nodes + " " + durability + " " + workload);
        Path dir = Files.createTempDirectory("raftkv-bench");
        DirectCluster cluster = DirectCluster.start(nodes, dir, fsync, config);
        try {
            cluster.awaitLeader(Duration.ofSeconds(5));
            Sample sample = pipelined(nodes, durability, workload, cluster);
            if (cluster.error() != null) {
                throw new IllegalStateException("raft thread failed", cluster.error());
            }
            return sample;
        } finally {
            cluster.close();
            delete(dir);
        }
    }

    private static Sample httpPut(RaftConfig config) throws Exception {
        System.out.println("bench http 3 memory put");
        List<String> ids = List.of("n1", "n2", "n3");
        Path dir = Files.createTempDirectory("raftkv-http-bench");
        try (HttpCluster cluster = HttpCluster.start(ids, dir, false, true, config)) {
            cluster.awaitLeader(Duration.ofSeconds(5));
            Map<String, String> urls = new LinkedHashMap<>();
            for (HttpCluster.Node node : cluster.nodes()) {
                urls.put(node.id, node.baseUrl);
            }
            int threads = HTTP_CLIENTS;
            ConcurrentLinkedQueue<Long> latencies = new ConcurrentLinkedQueue<>();
            AtomicInteger errors = new AtomicInteger();
            AtomicBoolean go = new AtomicBoolean(true);
            long start = System.nanoTime();
            long warmupEnd = start + WARMUP_MS * 1_000_000L;
            long measureEnd = warmupEnd + MEASURE_MS * 1_000_000L;
            CountDownLatch done = new CountDownLatch(threads);
            for (int i = 0; i < threads; i++) {
                int id = i;
                Thread thread = new Thread(() -> {
                    KvClient client = new KvClient(urls);
                    try {
                        long seq = 0;
                        while (go.get() && System.nanoTime() < measureEnd) {
                            seq++;
                            String key = "k" + Math.floorMod(seq, KEYS);
                            long t0 = System.nanoTime();
                            boolean measuring = t0 >= warmupEnd;
                            try {
                                client.put(key, VALUE, Duration.ofSeconds(2));
                                long t1 = System.nanoTime();
                                if (measuring && t1 <= measureEnd + 2_000_000_000L) {
                                    latencies.add(t1 - t0);
                                }
                            } catch (RuntimeException error) {
                                if (measuring) {
                                    errors.incrementAndGet();
                                }
                            }
                        }
                    } finally {
                        done.countDown();
                    }
                }, "http-client-" + id);
                thread.start();
            }
            Thread.sleep(WARMUP_MS + MEASURE_MS);
            go.set(false);
            done.await();
            long elapsed = System.nanoTime() - warmupEnd;
            return new Sample(3, "memory", "put", "http", latencies.size(), elapsed, percentiles(latencies), errors.get());
        } finally {
            delete(dir);
        }
    }

    /**
     * One thread keeps {@link #PIPELINE} calls in flight. Latency is from submit to the client
     * observing the result, so it includes queueing behind the other in-flight calls.
     */
    private static Sample pipelined(int nodes, String durability, String workload, DirectCluster cluster)
            throws Exception {
        ConcurrentLinkedQueue<Long> latencies = new ConcurrentLinkedQueue<>();
        int errors = 0;
        long seq = 0;
        ArrayDeque<Inflight> inflight = new ArrayDeque<>();
        long start = System.nanoTime();
        long warmupEnd = start + WARMUP_MS * 1_000_000L;
        long measureEnd = warmupEnd + MEASURE_MS * 1_000_000L;
        while (System.nanoTime() < measureEnd || !inflight.isEmpty()) {
            while (inflight.size() < PIPELINE && System.nanoTime() < measureEnd) {
                seq++;
                String key = "k" + Math.floorMod(seq, KEYS);
                boolean read = "mixed".equals(workload) && (seq % 2 == 0);
                NodeLoop leader = cluster.leaderLoop();
                long t0 = System.nanoTime();
                CompletableFuture<ClientResult> future = read
                        ? leader.getAsync(key)
                        : leader.proposeAsync(Command.put("bench", seq, key, VALUE));
                inflight.add(new Inflight(future, t0, read, seq, key));
            }
            Inflight item = inflight.pollFirst();
            if (item == null) {
                break;
            }
            ClientResult result = await(item);
            if (result.status() != ClientStatus.OK) {
                cluster.forgetLeader();
                NodeLoop leader = cluster.leaderLoop();
                result = item.read
                        ? leader.get(item.key, 2_000)
                        : leader.propose(Command.put("bench", item.seq, item.key, VALUE), 2_000);
            }
            long t1 = System.nanoTime();
            if (item.started < warmupEnd) {
                continue;
            }
            if (result.status() == ClientStatus.OK) {
                latencies.add(t1 - item.started);
            } else {
                errors++;
            }
        }
        long elapsed = System.nanoTime() - warmupEnd;
        return new Sample(nodes, durability, workload, "in-process", latencies.size(), elapsed, percentiles(latencies), errors);
    }

    private static ClientResult await(Inflight item) {
        try {
            return item.future.get(2, TimeUnit.SECONDS);
        } catch (Exception error) {
            return ClientResult.unknown(null);
        }
    }

    private record Inflight(CompletableFuture<ClientResult> future, long started, boolean read, long seq, String key) {}

    private static long[] percentiles(ConcurrentLinkedQueue<Long> latencies) {
        long[] values = latencies.stream().mapToLong(Long::longValue).sorted().toArray();
        if (values.length == 0) {
            return new long[]{0, 0};
        }
        return new long[]{values[rank(0.50, values.length)], values[rank(0.99, values.length)]};
    }

    /** Nearest-rank percentile, 1-based ceil. */
    private static int rank(double quantile, int n) {
        int index = (int) Math.ceil(quantile * n) - 1;
        return Math.max(0, Math.min(n - 1, index));
    }

    private static String document(List<Sample> samples) throws IOException {
        String cpu = "unknown";
        Path cpuinfo = Path.of("/proc/cpuinfo");
        if (Files.exists(cpuinfo)) {
            for (String line : Files.readAllLines(cpuinfo)) {
                if (line.startsWith("model name")) {
                    cpu = line.substring(line.indexOf(':') + 1).trim();
                    break;
                }
            }
        }
        String fs = "unknown";
        try {
            fs = Files.getFileStore(Path.of(".").toAbsolutePath()).type();
        } catch (IOException ignored) {
            // The table still records the numbers; the filesystem type is context.
        }
        Json.Arr runs = new Json.Arr();
        for (Sample sample : samples) {
            double seconds = sample.elapsedNanos / 1_000_000_000d;
            double opsPerSec = seconds == 0 ? 0 : sample.okOps / seconds;
            runs.add(new Json.Obj()
                    .put("nodes", sample.nodes)
                    .put("durability", sample.durability)
                    .put("workload", sample.workload)
                    .put("transport", sample.transport)
                    .put("clients", "http".equals(sample.transport) ? HTTP_CLIENTS : 1)
                    .put("pipeline", "http".equals(sample.transport) ? 1 : PIPELINE)
                    .put("valueBytes", VALUE.length())
                    .put("keys", KEYS)
                    .put("warmupMs", WARMUP_MS)
                    .put("measureMs", MEASURE_MS)
                    .put("okOps", sample.okOps)
                    .put("elapsedNanos", sample.elapsedNanos)
                    .put("opsPerSec", round1(opsPerSec))
                    .put("p50Nanos", sample.p50p99[0])
                    .put("p99Nanos", sample.p50p99[1])
                    .put("errors", sample.errors));
        }
        Json.Obj root = new Json.Obj()
                .put("recordedAt", Instant.now().toString())
                .put("cpu", cpu)
                .put("cpus", Runtime.getRuntime().availableProcessors())
                .put("os", System.getProperty("os.name") + " " + System.getProperty("os.version"))
                .put("java", Runtime.version().toString())
                .put("filesystem", fs)
                .put("percentile", "nearest-rank, ceil(p * n) - 1")
                .put("notes", "One recorded window per row, after a discarded 3-node memory pass of the same length so the JIT is already warm. That warmup is not in the table. In-process rows use one client thread with 8 operations in flight; each replica has its own thread. Latency is from submit until the client sees the result. Each mutating operation is one Raft log entry; commands are not batched. fsync calls FileDescriptor.sync on the WAL and on metadata replacements. Linearizable reads use a majority heartbeat round trip. The HTTP row uses 4 client threads, JDK HttpServer, and java.net.http over HTTP/1.1 on localhost with TCP_NODELAY.")
                .put("runs", runs);
        StringBuilder pretty = new StringBuilder();
        pretty.append(Json.writePretty(root));
        return pretty.toString();
    }

    private static double round1(double value) {
        return Math.round(value * 10d) / 10d;
    }

    private static void delete(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException ignored) {
                    // Best-effort cleanup of the temp cluster directory.
                }
            });
        }
    }

    private record Sample(
            int nodes,
            String durability,
            String workload,
            String transport,
            int okOps,
            long elapsedNanos,
            long[] p50p99,
            int errors) {}

    /** In-process cluster. Peer RPCs are handed to the destination thread, never run inline. */
    private static final class DirectCluster implements AutoCloseable {
        private final List<NodeLoop> loops;
        private final AtomicReference<String> leader = new AtomicReference<>();

        private DirectCluster(List<NodeLoop> loops) {
            this.loops = loops;
        }

        static DirectCluster start(int n, Path dir, boolean fsync, RaftConfig config) throws IOException {
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                ids.add("n" + (i + 1));
            }
            Map<String, NodeLoop> byId = new LinkedHashMap<>();
            List<NodeLoop> loops = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                String id = ids.get(i);
                LogStore store = fsync
                        ? FileLogStore.open(dir.resolve(id), true)
                        : new MemoryLogStore();
                DirectTransport transport = new DirectTransport(id, byId);
                RaftNode node = new RaftNode(
                        id,
                        ids,
                        store,
                        new KvStateMachine(),
                        transport,
                        config,
                        new SystemClock(),
                        new JdkRandom(1000 + i),
                        Observer.NONE);
                NodeLoop loop = new NodeLoop(node);
                byId.put(id, loop);
                loops.add(loop);
            }
            for (NodeLoop loop : loops) {
                loop.start();
            }
            return new DirectCluster(loops);
        }

        void awaitLeader(Duration timeout) throws InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                for (NodeLoop loop : loops) {
                    try {
                        var status = loop.status(300);
                        if ("leader".equals(status.role())) {
                            leader.set(loop.node().id());
                            return;
                        }
                    } catch (RuntimeException ignored) {
                        // Not up yet.
                    }
                }
                Thread.sleep(20);
            }
            throw new IllegalStateException("no leader");
        }

        /** Cached leader. A status round trip happens only after a failed call clears the cache. */
        NodeLoop leaderLoop() {
            String id = leader.get();
            if (id != null) {
                for (NodeLoop loop : loops) {
                    if (loop.node().id().equals(id)) {
                        return loop;
                    }
                }
            }
            for (NodeLoop loop : loops) {
                try {
                    var status = loop.status(300);
                    if ("leader".equals(status.role())) {
                        leader.set(loop.node().id());
                        return loop;
                    }
                } catch (RuntimeException ignored) {
                    // Try the next replica.
                }
            }
            return loops.get(0);
        }

        void forgetLeader() {
            leader.set(null);
        }

        Throwable error() {
            for (NodeLoop loop : loops) {
                if (loop.lastError() != null) {
                    return loop.lastError();
                }
            }
            return null;
        }

        @Override
        public void close() {
            for (NodeLoop loop : loops) {
                loop.close();
            }
        }
    }

    private static final class DirectTransport implements Transport {
        private final String self;
        private final Map<String, NodeLoop> nodes;

        private DirectTransport(String self, Map<String, NodeLoop> nodes) {
            this.self = self;
            this.nodes = nodes;
        }

        @Override
        public void sendRequestVote(String to, raftkv.raft.Types.RequestVote message) {
            NodeLoop target = nodes.get(to);
            if (target == null) {
                return;
            }
            target.submit(() -> {
                var reply = target.node().handleRequestVote(message);
                NodeLoop source = nodes.get(self);
                source.submit(() -> source.node().handleRequestVoteReply(to, reply));
            });
        }

        @Override
        public void sendAppendEntries(String to, raftkv.raft.Types.AppendEntries message) {
            NodeLoop target = nodes.get(to);
            if (target == null) {
                return;
            }
            target.submit(() -> {
                var reply = target.node().handleAppendEntries(message);
                NodeLoop source = nodes.get(self);
                source.submit(() -> source.node().handleAppendEntriesReply(to, reply));
            });
        }

        @Override
        public void sendInstallSnapshot(String to, raftkv.raft.Types.InstallSnapshot message) {
            NodeLoop target = nodes.get(to);
            if (target == null) {
                return;
            }
            target.submit(() -> {
                var reply = target.node().handleInstallSnapshot(message);
                NodeLoop source = nodes.get(self);
                source.submit(() -> source.node().handleInstallSnapshotReply(to, reply));
            });
        }
    }
}
