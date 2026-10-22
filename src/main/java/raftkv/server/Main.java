package raftkv.server;

import raftkv.raft.Types.RaftConfig;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Starts a RaftKV cluster in this process.
 * <pre>
 *   ./gradlew run --args="--nodes 3 --port 7101 --data data --fsync true"
 * </pre>
 */
public final class Main {
    public static void main(String[] args) throws InterruptedException {
        int nodes = 3;
        int port = 7101;
        Path data = Path.of("data");
        boolean fsync = true;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--nodes" -> nodes = Integer.parseInt(args[++i]);
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--data" -> data = Path.of(args[++i]);
                case "--fsync" -> fsync = Boolean.parseBoolean(args[++i]);
                default -> throw new IllegalArgumentException("unknown arg " + args[i]);
            }
        }
        if (nodes < 1 || nodes > 5) {
            throw new IllegalArgumentException("nodes must be 1..5");
        }
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < nodes; i++) {
            ids.add("n" + (i + 1));
        }
        RaftConfig config = RaftConfig.standard();
        HttpCluster cluster = HttpCluster.start(ids, data, fsync, false, config, port);
        Runtime.getRuntime().addShutdownHook(new Thread(cluster::close));
        System.out.println("RaftKV listening");
        for (HttpCluster.Node node : cluster.nodes()) {
            System.out.println("  " + node.id + "  " + node.baseUrl);
        }
        Thread.currentThread().join();
    }
}
