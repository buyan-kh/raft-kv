package raftkv.persist;

import raftkv.raft.Types.LogEntry;

import java.util.ArrayList;
import java.util.List;

/** Process-local stand-in for disk. A restarted node reads this; a crash does not clear it. */
public final class MemoryLogStore implements LogStore {
    private int currentTerm;
    private String votedFor;
    private Snapshot snapshot;
    private final ArrayList<LogEntry> entries = new ArrayList<>();

    @Override
    public synchronized Meta loadMeta() {
        return new Meta(currentTerm, votedFor);
    }

    @Override
    public synchronized void storeMeta(int currentTerm, String votedFor) {
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
    }

    @Override
    public synchronized Snapshot loadSnapshot() {
        if (snapshot == null) {
            return null;
        }
        return new Snapshot(
                snapshot.lastIncludedIndex(),
                snapshot.lastIncludedTerm(),
                snapshot.data().clone());
    }

    @Override
    public synchronized void saveSnapshot(Snapshot snapshot) {
        this.snapshot = new Snapshot(
                snapshot.lastIncludedIndex(),
                snapshot.lastIncludedTerm(),
                snapshot.data().clone());
    }

    @Override
    public synchronized List<LogEntry> loadEntries() {
        return List.copyOf(entries);
    }

    @Override
    public synchronized void append(List<LogEntry> newEntries) {
        entries.addAll(newEntries);
    }

    @Override
    public synchronized void rewrite(List<LogEntry> newEntries) {
        entries.clear();
        entries.addAll(newEntries);
    }
}
