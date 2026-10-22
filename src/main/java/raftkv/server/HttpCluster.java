package raftkv.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import raftkv.kv.KvStateMachine;
import raftkv.persist.FileLogStore;
import raftkv.persist.LogStore;
import raftkv.persist.MemoryLogStore;
import raftkv.raft.RaftNode;
import raftkv.raft.Types.AppendEntries;
import raftkv.raft.Types.ClientResult;
import raftkv.raft.Types.ClientStatus;
import raftkv.raft.Types.Command;
import raftkv.raft.Types.InstallSnapshot;
import raftkv.raft.Types.JdkRandom;
import raftkv.raft.Types.Op;
import raftkv.raft.Types.RaftConfig;
import raftkv.raft.Types.RequestVote;
import raftkv.raft.Types.SystemClock;
import raftkv.raft.Types.Transport;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** A real process-local cluster speaking HTTP between replicas and to clients. */
public final class HttpCluster implements AutoCloseable {
    static {
        // Read by the JDK server the first time it accepts a socket. Without it, Nagle
        // plus delayed ACKs add about 40ms to every small response.
        System.setProperty("sun.net.httpserver.nodelay", "true");
    }

    private final List<Node> nodes;

    private HttpCluster(List<Node> nodes) {
        this.nodes = nodes;
    }

    public static HttpCluster start(List<String> ids, Path dataRoot, boolean fsync, boolean memory, RaftConfig config) {
        return start(ids, dataRoot, fsync, memory, config, 0);
    }

    /** {@code basePort} 0 binds ephemeral ports. Otherwise node i listens on basePort + i. */
    public static HttpCluster start(
            List<String> ids, Path dataRoot, boolean fsync, boolean memory, RaftConfig config, int basePort) {
        try {
            return startThrows(ids, dataRoot, fsync, memory, config, basePort);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public List<Node> nodes() {
        return nodes;
    }

    public Node node(String id) {
        for (Node node : nodes) {
            if (node.id.equals(id)) {
                return node;
            }
        }
        throw new IllegalArgumentException(id);
    }

    public String awaitLeader(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            for (Node node : nodes) {
                try {
                    var status = node.loop.status(500);
                    if ("leader".equals(status.role())) {
                        return node.id;
                    }
                } catch (RuntimeException ignored) {
                    // The replica may still be starting.
                }
            }
            Thread.sleep(30);
        }
        throw new IllegalStateException("no leader elected");
    }

    @Override
    public void close() {
        for (Node node : nodes) {
            node.http.stop(0);
            node.loop.close();
        }
    }

    private static HttpCluster startThrows(
            List<String> ids, Path dataRoot, boolean fsync, boolean memory, RaftConfig config, int basePort)
            throws IOException {
        List<HttpServer> servers = new ArrayList<>();
        Map<String, String> base = new LinkedHashMap<>();
        for (int i = 0; i < ids.size(); i++) {
            String id = ids.get(i);
            int port = basePort == 0 ? 0 : basePort + i;
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 128);
            servers.add(server);
            base.put(id, "http://127.0.0.1:" + server.getAddress().getPort());
        }
        HttpClient http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofMillis(300))
                .build();
        List<Node> nodes = new ArrayList<>();
        Map<String, NodeLoop> loops = new LinkedHashMap<>();
        for (int i = 0; i < ids.size(); i++) {
            String id = ids.get(i);
            LogStore store = memory
                    ? new MemoryLogStore()
                    : FileLogStore.open(dataRoot.resolve(id), fsync);
            HttpTransport transport = new HttpTransport(id, base, http);
            RaftNode raft = new RaftNode(
                    id,
                    ids,
                    store,
                    new KvStateMachine(),
                    transport,
                    config,
                    new SystemClock(),
                    new JdkRandom(System.nanoTime() + i * 17L),
                    new raftkv.raft.Types.Observer() {
                        @Override
                        public void role(String nodeId, String role, int term) {
                            if ("leader".equals(role)) {
                                System.out.println(nodeId + " became leader in term " + term);
                            }
                        }
                    });
            NodeLoop loop = new NodeLoop(raft);
            loops.put(id, loop);
            transport.bind(loop);
            Node node = new Node(id, base.get(id), servers.get(i), loop);
            servers.get(i).createContext("/", exchange -> handle(exchange, node, loop));
            servers.get(i).setExecutor(Executors.newFixedThreadPool(4, runnable -> {
                Thread thread = new Thread(runnable, "http-" + id);
                thread.setDaemon(true);
                return thread;
            }));
            nodes.add(node);
        }
        for (NodeLoop loop : loops.values()) {
            loop.start();
        }
        for (HttpServer server : servers) {
            server.start();
        }
        return new HttpCluster(nodes);
    }

    private static void handle(HttpExchange exchange, Node node, NodeLoop loop) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String method = exchange.getRequestMethod();
        try {
            if ("GET".equals(method) && "/health".equals(path)) {
                write(exchange, 200, "{\"ok\":true}");
                return;
            }
            if ("GET".equals(method) && "/v1/status".equals(path)) {
                write(exchange, 200, RpcJson.status(loop.status(1_000)));
                return;
            }
            if ("POST".equals(method) && path.startsWith("/raft/")) {
                String body = read(exchange);
                switch (path) {
                    case "/raft/request-vote" -> {
                        RequestVote request = RpcJson.requestVote(body);
                        write(exchange, 200, RpcJson.requestVoteReply(loop.call(() -> loop.node().handleRequestVote(request), 1_000)));
                    }
                    case "/raft/append-entries" -> {
                        AppendEntries request = RpcJson.appendEntries(body);
                        write(exchange, 200, RpcJson.appendEntriesReply(loop.call(() -> loop.node().handleAppendEntries(request), 1_000)));
                    }
                    case "/raft/install-snapshot" -> {
                        InstallSnapshot request = RpcJson.installSnapshot(body);
                        write(exchange, 200, RpcJson.installSnapshotReply(loop.call(() -> loop.node().handleInstallSnapshot(request), 2_000)));
                    }
                    default -> write(exchange, 404, "{\"error\":\"unknown rpc\"}");
                }
                return;
            }
            if (path.startsWith("/v1/kv/")) {
                String key = urlDecode(path.substring("/v1/kv/".length()));
                if ("GET".equals(method)) {
                    ClientResult result = loop.get(key, 2_000);
                    write(exchange, httpStatus(result.status()), RpcJson.clientResult(result, node.id));
                    return;
                }
                if ("PUT".equals(method) || "DELETE".equals(method)) {
                    Command command = RpcJson.command("DELETE".equals(method) ? Op.DELETE : Op.PUT, read(exchange), key);
                    ClientResult result = loop.propose(command, 3_000);
                    write(exchange, httpStatus(result.status()), RpcJson.clientResult(result, node.id));
                    return;
                }
            }
            write(exchange, 404, "{\"error\":\"not found\"}");
        } catch (IllegalArgumentException bad) {
            write(exchange, 400, "{\"error\":\"bad request\"}");
        } catch (RuntimeException failure) {
            write(exchange, 500, "{\"error\":\"internal\"}");
        }
    }

    private static int httpStatus(ClientStatus status) {
        return switch (status) {
            case OK -> 200;
            case NOT_LEADER -> 409;
            case NO_QUORUM, UNKNOWN -> 503;
        };
    }

    private static String urlDecode(String raw) {
        return java.net.URLDecoder.decode(raw, StandardCharsets.UTF_8);
    }

    private static String read(HttpExchange exchange) throws IOException {
        try (InputStream in = exchange.getRequestBody()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static void write(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("content-type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    public static final class Node {
        public final String id;
        public final String baseUrl;
        final HttpServer http;
        final NodeLoop loop;

        Node(String id, String baseUrl, HttpServer http, NodeLoop loop) {
            this.id = id;
            this.baseUrl = baseUrl;
            this.http = http;
            this.loop = loop;
        }
    }

    private static final class HttpTransport implements Transport {
        private final String self;
        private final Map<String, String> base;
        private final HttpClient http;
        private final ExecutorService pool = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "raft-send");
            thread.setDaemon(true);
            return thread;
        });
        private volatile NodeLoop selfLoop;

        private HttpTransport(String self, Map<String, String> base, HttpClient http) {
            this.self = self;
            this.base = base;
            this.http = http;
        }

        private void bind(NodeLoop loop) {
            this.selfLoop = loop;
        }

        @Override
        public void sendRequestVote(String to, RequestVote message) {
            send(to, "/raft/request-vote", RpcJson.requestVote(message), body -> {
                var reply = RpcJson.requestVoteReply(body);
                selfLoop.submit(() -> selfLoop.node().handleRequestVoteReply(to, reply));
            });
        }

        @Override
        public void sendAppendEntries(String to, AppendEntries message) {
            send(to, "/raft/append-entries", RpcJson.appendEntries(message), body -> {
                var reply = RpcJson.appendEntriesReply(body);
                selfLoop.submit(() -> selfLoop.node().handleAppendEntriesReply(to, reply));
            });
        }

        @Override
        public void sendInstallSnapshot(String to, InstallSnapshot message) {
            send(to, "/raft/install-snapshot", RpcJson.installSnapshot(message), body -> {
                var reply = RpcJson.installSnapshotReply(body);
                selfLoop.submit(() -> selfLoop.node().handleInstallSnapshotReply(to, reply));
            });
        }

        private void send(String to, String path, String json, java.util.function.Consumer<String> ok) {
            String url = base.get(to);
            if (url == null || to.equals(self)) {
                return;
            }
            pool.execute(() -> {
                try {
                    HttpRequest request = HttpRequest.newBuilder(URI.create(url + path))
                            .timeout(Duration.ofMillis(1_500))
                            .header("content-type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(json))
                            .build();
                    HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
                    if (response.statusCode() == 200 && selfLoop != null) {
                        ok.accept(response.body());
                    }
                } catch (Exception ignored) {
                    // Dropped RPCs are retried by the next heartbeat or election.
                }
            });
        }
    }
}
