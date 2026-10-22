package raftkv.raft;

import raftkv.raft.Types.Command;
import raftkv.raft.Types.LogCell;
import raftkv.raft.Types.LogEntry;
import raftkv.raft.Types.Op;

import java.util.ArrayList;
import java.util.List;

/**
 * In-memory Raft log. Index 0 is the empty prefix: {@code lastIncludedIndex == 0} and
 * {@code lastIncludedTerm == 0} before any snapshot. Real entries start at 1.
 */
public final class RaftLog {
    private int lastIncludedIndex;
    private int lastIncludedTerm;
    private final ArrayList<LogEntry> entries = new ArrayList<>();

    public int lastIncludedIndex() {
        return lastIncludedIndex;
    }

    public int lastIncludedTerm() {
        return lastIncludedTerm;
    }

    public int lastIndex() {
        return lastIncludedIndex + entries.size();
    }

    public int lastTerm() {
        if (entries.isEmpty()) {
            return lastIncludedTerm;
        }
        return entries.get(entries.size() - 1).term();
    }

    public boolean contains(int index) {
        return index >= lastIncludedIndex && index <= lastIndex();
    }

    public int termAt(int index) {
        if (index == lastIncludedIndex) {
            return lastIncludedTerm;
        }
        return entryAt(index).term();
    }

    public LogEntry entryAt(int index) {
        if (index <= lastIncludedIndex || index > lastIndex()) {
            throw new IllegalArgumentException(
                    "index " + index + " outside (" + lastIncludedIndex + "," + lastIndex() + "]");
        }
        return entries.get(index - lastIncludedIndex - 1);
    }

    public void append(LogEntry entry) {
        int expected = lastIndex() + 1;
        if (entry.index() != expected) {
            throw new IllegalArgumentException("expected index " + expected + " but got " + entry.index());
        }
        entries.add(entry);
    }

    /** Delete {@code index} and everything after it. The snapshot prefix cannot be truncated. */
    public void truncateFrom(int index) {
        if (index <= lastIncludedIndex) {
            throw new IllegalArgumentException("cannot truncate compacted index " + index);
        }
        if (index > lastIndex()) {
            return;
        }
        int from = index - lastIncludedIndex - 1;
        entries.subList(from, entries.size()).clear();
    }

    /** Drop the prefix through {@code index} after that prefix has been saved in a snapshot. */
    public void compact(int index, int term) {
        if (index < lastIncludedIndex || index > lastIndex()) {
            throw new IllegalArgumentException("compact index " + index);
        }
        if (index > lastIncludedIndex && termAt(index) != term) {
            throw new IllegalArgumentException("compact term mismatch at " + index);
        }
        int drop = index - lastIncludedIndex;
        if (drop > 0) {
            entries.subList(0, drop).clear();
        }
        lastIncludedIndex = index;
        lastIncludedTerm = term;
    }

    public void resetToSnapshot(int index, int term) {
        entries.clear();
        lastIncludedIndex = index;
        lastIncludedTerm = term;
    }

    public List<LogEntry> slice(int fromInclusive, int toInclusive, int max) {
        if (fromInclusive > toInclusive || max <= 0) {
            return List.of();
        }
        ArrayList<LogEntry> out = new ArrayList<>();
        for (int i = fromInclusive; i <= toInclusive && out.size() < max; i++) {
            if (i > lastIncludedIndex) {
                out.add(entryAt(i));
            }
        }
        return List.copyOf(out);
    }

    public List<LogEntry> all() {
        return List.copyOf(entries);
    }

    public int lastIndexOfTerm(int term) {
        for (int i = lastIndex(); i > lastIncludedIndex; i--) {
            if (termAt(i) == term) {
                return i;
            }
        }
        if (lastIncludedTerm == term) {
            return lastIncludedIndex;
        }
        return -1;
    }

    public int firstIndexOfTerm(int term) {
        if (lastIncludedTerm == term && entries.isEmpty()) {
            return lastIncludedIndex;
        }
        for (int i = lastIncludedIndex + 1; i <= lastIndex(); i++) {
            if (termAt(i) == term) {
                return i;
            }
        }
        if (lastIncludedTerm == term) {
            return lastIncludedIndex;
        }
        return -1;
    }

    public List<LogCell> tail(int limit) {
        int start = Math.max(lastIncludedIndex + 1, lastIndex() - limit + 1);
        ArrayList<LogCell> out = new ArrayList<>();
        for (int i = start; i <= lastIndex(); i++) {
            LogEntry entry = entryAt(i);
            Command command = entry.command();
            Op op = command.op();
            out.add(new LogCell(
                    entry.index(),
                    entry.term(),
                    op,
                    op == Op.NOOP ? "" : command.key(),
                    op == Op.PUT ? command.value() : ""));
        }
        return List.copyOf(out);
    }
}
