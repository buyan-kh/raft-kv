package raftkv.sim;

import org.junit.jupiter.api.Test;
import raftkv.linear.History;
import raftkv.linear.LinearizabilityChecker;
import raftkv.raft.Types.ClientStatus;
import raftkv.raft.Types.Command;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftSimTest {
    @Test
    void electsLeaderAndReplicates() {
        ClusterSim sim = ClusterSim.builder().nodes(3).seed(1).start();
        String leader = sim.awaitLeader(1_000);
        assertEquals("n1", leader);
        assertEquals(ClientStatus.OK, sim.put("c1", "user", "ada", 500).status());
        assertEquals("ada", sim.get("c1", "user", 500).value());
        sim.awaitConverged(1_000);
        for (String id : sim.ids()) {
            assertEquals("ada", sim.data(id).get("user"));
        }
        assertLinearizable(sim.history());
        assertEquals(1, sim.leaderIds().size());
    }

    @Test
    void partitionedLeaderCannotCommitAndMajorityTakesOver() {
        ClusterSim sim = ClusterSim.builder().nodes(3).seed(2).start();
        assertEquals("n1", sim.awaitLeader(1_000));
        assertEquals(ClientStatus.OK, sim.put("c1", "k", "before", 500).status());
        sim.awaitConverged(1_000);
        sim.cut(Set.of("n1"), Set.of("n2", "n3"));
        var write = sim.put("c1", "k", "after", 1_500);
        assertEquals(ClientStatus.OK, write.status());
        assertNotEquals("n1", write.leaderId());
        sim.heal();
        sim.awaitConverged(2_000);
        for (String id : sim.ids()) {
            assertEquals("after", sim.data(id).get("k"));
        }
        assertLinearizable(sim.history());
    }

    @Test
    void crashedLeaderDoesNotLoseCommittedData() {
        ClusterSim sim = ClusterSim.builder().nodes(3).seed(3).start();
        assertEquals("n1", sim.awaitLeader(1_000));
        assertEquals(ClientStatus.OK, sim.put("c1", "k", "kept", 500).status());
        sim.awaitConverged(1_000);
        sim.crash("n1");
        var read = sim.get("c2", "k", 2_000);
        assertEquals(ClientStatus.OK, read.status());
        assertEquals("kept", read.value());
        assertEquals(ClientStatus.OK, sim.put("c2", "m", "new", 2_000).status());
        sim.restart("n1");
        sim.awaitConverged(2_000);
        assertEquals("kept", sim.data("n1").get("k"));
        assertEquals("new", sim.data("n1").get("m"));
        assertLinearizable(sim.history());
    }

    @Test
    void uncommittedEntryIsReplacedByTheNewMajority() {
        ClusterSim sim = ClusterSim.builder().nodes(5).seed(4).start();
        assertEquals("n1", sim.awaitLeader(1_000));
        assertEquals(ClientStatus.OK, sim.put("c1", "seed", "1", 500).status());
        sim.awaitConverged(1_000);
        sim.crash("n3");
        sim.crash("n4");
        sim.crash("n5");
        var orphan = sim.put("c1", "x", "orphan", 400);
        assertNotEquals(ClientStatus.OK, orphan.status());
        sim.crash("n1");
        sim.crash("n2");
        sim.restart("n3");
        sim.restart("n4");
        sim.restart("n5");
        var repaired = sim.put("c2", "x", "committed", 2_000);
        assertEquals(ClientStatus.OK, repaired.status(), sim.dump());
        sim.restart("n1");
        sim.restart("n2");
        sim.awaitConverged(3_000);
        for (String id : sim.ids()) {
            assertEquals("1", sim.data(id).get("seed"), sim.dump());
            assertEquals("committed", sim.data(id).get("x"), sim.dump());
        }
        assertLinearizable(sim.history());
    }

    @Test
    void laggingNodeInstallsASnapshot() {
        ClusterSim sim = ClusterSim.builder().nodes(3).seed(5).snapshotEvery(8).start();
        assertEquals("n1", sim.awaitLeader(1_000));
        assertEquals(ClientStatus.OK, sim.put("c1", "k0", "v0", 500).status());
        sim.awaitConverged(1_000);
        sim.crash("n3");
        for (int i = 1; i <= 30; i++) {
            assertEquals(ClientStatus.OK, sim.put("c1", "k" + i, "v" + i, 500).status(), "i=" + i + "\n" + sim.dump());
        }
        sim.restart("n3");
        sim.awaitConverged(3_000);
        assertEquals("v30", sim.data("n3").get("k30"));
        assertEquals("v0", sim.data("n3").get("k0"));
        assertTrue(sim.view("n3").snapshotIndex() > 0 || sim.commitIndex("n3") > 8, sim.dump());
        assertLinearizable(sim.history());
    }

    @Test
    void duplicateClientSequenceDoesNotApplyTwice() {
        ClusterSim sim = ClusterSim.builder().nodes(3).seed(6).start();
        String leader = sim.awaitLeader(1_000);
        Command command = Command.put("client", 1, "k", "once");
        assertEquals(ClientStatus.OK, sim.proposeOnce(leader, command, 500).status());
        int index = sim.commitIndex(leader);
        assertEquals(ClientStatus.OK, sim.proposeOnce(leader, command, 500).status());
        assertEquals(index, sim.commitIndex(leader));
        assertEquals("once", sim.data(leader).get("k"));
        sim.awaitConverged(1_000);
    }

    @Test
    void deleteRemovesTheKey() {
        ClusterSim sim = ClusterSim.builder().nodes(3).seed(7).start();
        sim.awaitLeader(1_000);
        assertEquals(ClientStatus.OK, sim.put("c1", "k", "v", 500).status());
        assertEquals(ClientStatus.OK, sim.delete("c1", "k", 500).status());
        var read = sim.get("c1", "k", 500);
        assertEquals(ClientStatus.OK, read.status());
        assertNull(read.value());
        sim.awaitConverged(1_000);
        assertNull(sim.data("n2").get("k"));
    }

    @Test
    void chaosHistoriesAreLinearizable() {
        for (int seed = 1; seed <= 6; seed++) {
            ClusterSim sim = ClusterSim.builder()
                    .nodes(5)
                    .seed(seed)
                    .delay(2, 12)
                    .loss(0.05)
                    .heartbeat(40)
                    .snapshotEvery(10)
                    .election(160, 280, 160)
                    .start();
            Random rng = new Random(seed);
            String down = null;
            boolean partitioned = false;
            int ops = 0;
            while (sim.now() < 2_000 && ops < 36) {
                if (down != null && rng.nextDouble() < 0.45) {
                    sim.restart(down);
                    down = null;
                } else if (down == null && rng.nextDouble() < 0.08) {
                    down = sim.ids().get(rng.nextInt(sim.ids().size()));
                    sim.crash(down);
                }
                if (partitioned && rng.nextDouble() < 0.35) {
                    sim.heal();
                    partitioned = false;
                } else if (!partitioned && rng.nextDouble() < 0.12) {
                    int isolated = rng.nextInt(sim.ids().size());
                    sim.cut(Set.of(sim.ids().get(isolated)), otherThan(sim.ids(), isolated));
                    partitioned = true;
                }
                String client = "c" + rng.nextInt(3);
                String key = "k" + rng.nextInt(5);
                if (rng.nextDouble() < 0.35) {
                    sim.get(client, key, 350);
                } else {
                    sim.put(client, key, client + ":" + ops, 350);
                }
                ops++;
            }
            sim.heal();
            if (down != null) {
                sim.restart(down);
            }
            sim.awaitConverged(3_000);
            for (String key : List.of("k0", "k1", "k2", "k3", "k4")) {
                sim.get("final", key, 1_000);
            }
            assertLinearizable(sim.history());
            assertEquals(List.of(), divergentLeaders(sim), "seed " + seed + "\n" + sim.dump());
        }
    }

    private static Set<String> otherThan(List<String> ids, int isolated) {
        HashSet<String> rest = new HashSet<>(ids);
        rest.remove(ids.get(isolated));
        return rest;
    }

    private static void assertLinearizable(History history) {
        LinearizabilityChecker.Result result = new LinearizabilityChecker().check(history);
        assertTrue(result.ok(), result.detail());
    }

    private static List<String> divergentLeaders(ClusterSim sim) {
        return sim.leaderIds().size() > 1 ? sim.leaderIds() : List.of();
    }
}
