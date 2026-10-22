package raftkv.sim;

import raftkv.json.Json;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

/** Records short deterministic cluster traces for the web flight recorder. */
public final class TraceGen {
    public static void main(String[] args) throws Exception {
        Path dir = Path.of("web", "public", "traces");
        Files.createDirectories(dir);
        Json.Arr index = new Json.Arr();
        write(dir, index, election());
        write(dir, index, partition());
        write(dir, index, crash());
        write(dir, index, snapshot());
        Files.writeString(dir.resolve("index.json"), Json.writePretty(new Json.Obj().put("scenarios", index)));
        System.out.println("wrote traces to " + dir.toAbsolutePath());
    }

    private static void write(Path dir, Json.Arr index, Json.Obj document) throws Exception {
        String id = document.str("id");
        Files.writeString(dir.resolve(id + ".json"), Json.writePretty(document));
        index.add(new Json.Obj()
                .put("id", id)
                .put("title", document.str("title"))
                .put("blurb", document.str("blurb"))
                .put("file", id + ".json"));
    }

    private static Json.Obj election() {
        ClusterSim sim = ClusterSim.builder().nodes(3).seed(11).trace(true).delay(8, 8).start();
        sim.awaitLeader(1_000);
        sim.put("c1", "alpha", "1", 400);
        sim.put("c1", "beta", "2", 400);
        sim.get("c2", "alpha", 400);
        sim.runFor(200);
        return sim.traceDocument(
                "election",
                "Election and replication",
                "Three nodes start together. The shortest timer wins a pre-vote, then a real election, appends a no-op, and replicates two puts.");
    }

    private static Json.Obj partition() {
        ClusterSim sim = ClusterSim.builder().nodes(5).seed(12).trace(true).delay(6, 6).start();
        sim.awaitLeader(1_000);
        sim.put("c1", "k", "before", 400);
        sim.awaitConverged(800);
        sim.cut(Set.of("n1"), other());
        sim.put("c2", "k", "majority", 1_600);
        sim.runFor(200);
        sim.heal();
        sim.awaitConverged(1_500);
        sim.runFor(150);
        return sim.traceDocument(
                "partition",
                "Partition and re-election",
                "The leader is cut off alone. It cannot commit. The remaining four nodes elect a new leader, commit, and the old leader catches up after the partition heals.");
    }

    private static Set<String> other() {
        return Set.of("n2", "n3", "n4", "n5");
    }

    private static Json.Obj crash() {
        ClusterSim sim = ClusterSim.builder().nodes(3).seed(13).trace(true).delay(6, 6).start();
        sim.awaitLeader(1_000);
        sim.put("c1", "saved", "yes", 400);
        sim.awaitConverged(800);
        sim.crash("n1");
        sim.put("c2", "saved", "still", 1_600);
        sim.runFor(200);
        sim.restart("n1");
        sim.awaitConverged(1_500);
        sim.runFor(150);
        return sim.traceDocument(
                "crash",
                "Leader crash and restart",
                "A committed write survives the leader's crash. A new leader is elected from the surviving majority, and the restarted node rebuilds from its durable log.");
    }

    private static Json.Obj snapshot() {
        ClusterSim sim = ClusterSim.builder().nodes(3).seed(14).trace(true).delay(4, 4).snapshotEvery(6).start();
        sim.awaitLeader(1_000);
        sim.put("c1", "k0", "v0", 400);
        sim.awaitConverged(600);
        sim.crash("n3");
        for (int i = 1; i <= 16; i++) {
            sim.put("c1", "k" + i, "v" + i, 400);
        }
        sim.restart("n3");
        sim.awaitConverged(2_000);
        sim.runFor(100);
        return sim.traceDocument(
                "snapshot",
                "Snapshot catch-up",
                "A follower is down while the leader compacts its log. When the follower restarts, the leader installs a snapshot instead of replaying every entry.");
    }

    @SuppressWarnings("unused")
    private static List<String> ids() {
        return List.of("n1", "n2", "n3");
    }
}
