package raftkv.sim;

import raftkv.json.Json;
import raftkv.linear.History;
import raftkv.linear.LinearizabilityChecker;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/** The recorded chaos run. Same harness as the unit test, more seeds, results written to disk. */
public final class ChaosCampaign {
    public static void main(String[] args) throws Exception {
        int seeds = 20;
        Json.Arr runs = new Json.Arr();
        int violations = 0;
        int convergenceFailures = 0;
        long started = System.nanoTime();
        for (int seed = 1; seed <= seeds; seed++) {
            Run run = one(seed);
            if (!run.linearizable) {
                violations++;
            }
            if (!run.converged) {
                convergenceFailures++;
            }
            runs.add(new Json.Obj()
                    .put("seed", seed)
                    .put("ops", run.ops)
                    .put("ok", run.ok)
                    .put("info", run.info)
                    .put("fail", run.fail)
                    .put("linearizable", run.linearizable)
                    .put("converged", run.converged)
                    .put("detail", run.detail));
            System.out.println("seed " + seed + " ops=" + run.ops + " ok=" + run.ok
                    + " info=" + run.info + " fail=" + run.fail
                    + " linearizable=" + run.linearizable + " converged=" + run.converged);
        }
        long elapsedMs = (System.nanoTime() - started) / 1_000_000L;
        Json.Obj root = new Json.Obj()
                .put("seeds", seeds)
                .put("nodes", 5)
                .put("clients", 3)
                .put("keys", 5)
                .put("lossRate", 0.05)
                .put("delayMs", "2-12")
                .put("maxOpsPerSeed", 60)
                .put("virtualBudgetMs", 12_000)
                .put("violations", violations)
                .put("convergenceFailures", convergenceFailures)
                .put("wallTimeMs", elapsedMs)
                .put("checker", "per-key Wing/Gong linearizability, INFO writes may or may not take effect")
                .put("runs", runs);
        Path out = Path.of("results", "simulation.json");
        Files.createDirectories(out.getParent());
        Files.writeString(out, Json.writePretty(root));
        System.out.println("violations=" + violations + " convergenceFailures=" + convergenceFailures);
        if (violations > 0 || convergenceFailures > 0) {
            System.exit(1);
        }
    }

    private static Run one(int seed) {
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
        try {
            while (sim.now() < 12_000 && ops < 60) {
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
                    sim.cut(Set.of(sim.ids().get(isolated)), others(sim.ids(), isolated));
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
            LinearizabilityChecker.Result result = new LinearizabilityChecker().check(sim.history());
            return Run.from(sim.history(), ops, true, result.ok(), result.detail());
        } catch (RuntimeException error) {
            return Run.from(sim.history(), ops, false, false, error.getMessage() == null ? error.toString() : error.getMessage());
        }
    }

    private static Set<String> others(List<String> ids, int isolated) {
        HashSet<String> rest = new HashSet<>(ids);
        rest.remove(ids.get(isolated));
        return rest;
    }

    private record Run(int ops, int ok, int info, int fail, boolean converged, boolean linearizable, String detail) {
        static Run from(History history, int ops, boolean converged, boolean linearizable, String detail) {
            int ok = 0;
            int info = 0;
            int fail = 0;
            for (History.Op op : history.ops()) {
                switch (op.status) {
                    case OK -> ok++;
                    case INFO -> info++;
                    case FAIL -> fail++;
                }
            }
            return new Run(ops, ok, info, fail, converged, linearizable, detail == null ? "" : detail);
        }
    }
}
