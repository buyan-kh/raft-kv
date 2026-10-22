package raftkv.persist;

import raftkv.raft.Types.LogEntry;

import java.util.List;

/** Durable Raft state. Implementations must make each method crash-atomic. */
public interface LogStore {
    record Meta(int currentTerm, String votedFor) {}

    record Snapshot(int lastIncludedIndex, int lastIncludedTerm, byte[] data) {}

    Meta loadMeta();

    void storeMeta(int currentTerm, String votedFor);

    /** @return the latest snapshot, or null when the node has never compacted. */
    Snapshot loadSnapshot();

    void saveSnapshot(Snapshot snapshot);

    List<LogEntry> loadEntries();

    /** Append already-contiguous entries and make them durable before returning. */
    void append(List<LogEntry> entries);

    /** Replace the on-disk entry list. Used after truncation and after compaction. */
    void rewrite(List<LogEntry> entries);
}
