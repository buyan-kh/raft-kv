package raftkv.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import raftkv.client.KvClient;
import raftkv.raft.Types.RaftConfig;

import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class HttpClusterTest {
    @TempDir
    Path data;

    @Test
    void putGetRedirectAndRestartFromDisk() throws Exception {
        RaftConfig config = RaftConfig.sim(200, 350, 40, 64, 200);
        List<String> ids = List.of("n1", "n2", "n3");
        Map<String, String> urls;
        try (HttpCluster cluster = HttpCluster.start(ids, data, true, false, config)) {
            cluster.awaitLeader(Duration.ofSeconds(5));
            urls = new LinkedHashMap<>();
            for (HttpCluster.Node node : cluster.nodes()) {
                urls.put(node.id, node.baseUrl);
            }
            KvClient client = new KvClient(urls);
            client.put("city", "lisbon", Duration.ofSeconds(4));
            assertEquals("lisbon", client.get("city", Duration.ofSeconds(4)));
            KvClient other = new KvClient(Map.of("n2", urls.get("n2"), "n3", urls.get("n3"), "n1", urls.get("n1")));
            assertEquals("lisbon", other.get("city", Duration.ofSeconds(4)));
            other.delete("city", Duration.ofSeconds(4));
            assertNull(other.get("city", Duration.ofSeconds(4)));
            other.put("city", "porto", Duration.ofSeconds(4));
        }
        try (HttpCluster restarted = HttpCluster.start(ids, data, true, false, config)) {
            restarted.awaitLeader(Duration.ofSeconds(5));
            Map<String, String> again = new LinkedHashMap<>();
            for (HttpCluster.Node node : restarted.nodes()) {
                again.put(node.id, node.baseUrl);
            }
            KvClient client = new KvClient(again);
            assertEquals("porto", client.get("city", Duration.ofSeconds(4)));
        }
    }
}
