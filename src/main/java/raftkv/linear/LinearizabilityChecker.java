package raftkv.linear;

import raftkv.linear.History.Kind;
import raftkv.linear.History.Op;
import raftkv.linear.History.Status;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Jepsen-style linearizability check for a map of read/write registers.
 *
 * <p>Operations on different keys do not affect each other's sequential specification, so a history
 * is linearizable if and only if each per-key history is (Herlihy and Wing's P-compositionality).
 * Each key is checked with the Wing and Gong backtracking search. {@link Status#INFO} writes may
 * either take effect or not. {@link Status#FAIL} operations are omitted: the caller only uses FAIL
 * when the operation definitely did not run.
 */
public final class LinearizabilityChecker {
    private static final int STEP_BUDGET = 2_000_000;

    private int steps;
    private final Set<String> failed = new HashSet<>();
    private final Set<String> visiting = new HashSet<>();

    public Result check(History history) {
        Map<String, List<Op>> byKey = new HashMap<>();
        for (Op op : history.ops()) {
            if (op.open) {
                return Result.bad("op still open: " + op);
            }
            if (op.status == Status.FAIL) {
                continue;
            }
            if (op.kind == Kind.READ && op.status != Status.OK) {
                continue;
            }
            if (op.status != Status.OK && op.status != Status.INFO) {
                continue;
            }
            if (op.kind == Kind.READ && op.status == Status.INFO) {
                continue;
            }
            byKey.computeIfAbsent(op.key, key -> new ArrayList<>()).add(op);
        }
        for (var entry : byKey.entrySet()) {
            steps = 0;
            failed.clear();
            List<Op> ops = entry.getValue();
            if (ops.size() > 96) {
                return Result.bad("too many ops on " + entry.getKey() + " (" + ops.size() + ")");
            }
            if (!search(ops, null, all(ops.size()))) {
                return Result.bad("no linearization for key " + entry.getKey() + " (" + ops.size() + " ops)\n" + ops);
            }
        }
        return Result.passed();
    }

    private boolean search(List<Op> ops, String state, BitSet live) {
        if (++steps > STEP_BUDGET) {
            throw new IllegalStateException("linearizability search exceeded " + STEP_BUDGET + " steps");
        }
        if (live.isEmpty()) {
            return true;
        }
        String memo = (state == null ? "\u0000" : state) + "|" + bits(live, ops.size());
        if (failed.contains(memo)) {
            return false;
        }
        if (!visiting.add(memo)) {
            return false;
        }
        try {
            for (int i = live.nextSetBit(0); i >= 0; i = live.nextSetBit(i + 1)) {
                if (!minimal(ops, live, i)) {
                    continue;
                }
                Op op = ops.get(i);
                live.clear(i);
                boolean ok;
                if (op.kind == Kind.READ) {
                    ok = same(state, op.readValue) && search(ops, state, live);
                } else if (op.status == Status.OK) {
                    ok = search(ops, written(op), live);
                } else if (search(ops, written(op), live)) {
                    // Indeterminate write: it may have been applied, or it may have been lost.
                    ok = true;
                } else {
                    ok = search(ops, state, live);
                }
                if (ok) {
                    return true;
                }
                live.set(i);
            }
            failed.add(memo);
            return false;
        } finally {
            visiting.remove(memo);
        }
    }

    private static boolean minimal(List<Op> ops, BitSet live, int index) {
        long start = ops.get(index).start;
        for (int other = live.nextSetBit(0); other >= 0; other = live.nextSetBit(other + 1)) {
            if (other != index && ops.get(other).end <= start) {
                return false;
            }
        }
        return true;
    }

    private static String written(Op op) {
        return op.delete ? null : op.writeValue;
    }

    private static boolean same(String state, String observed) {
        return state == null ? observed == null : state.equals(observed);
    }

    private static BitSet all(int n) {
        BitSet bits = new BitSet(n);
        bits.set(0, n);
        return bits;
    }

    private static String bits(BitSet live, int n) {
        char[] chars = new char[n];
        for (int i = 0; i < n; i++) {
            chars[i] = live.get(i) ? '1' : '0';
        }
        return new String(chars);
    }

    public record Result(boolean ok, String detail) {
        public static Result passed() {
            return new Result(true, "linearizable");
        }

        public static Result bad(String detail) {
            return new Result(false, detail);
        }
    }
}
