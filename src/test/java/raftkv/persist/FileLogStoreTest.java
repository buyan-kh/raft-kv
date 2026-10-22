package raftkv.persist;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import raftkv.raft.Types.Command;
import raftkv.raft.Types.LogEntry;

import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileLogStoreTest {
    @TempDir
    Path dir;

    @Test
    void metaVoteAndEntriesSurviveReopen() {
        FileLogStore store = FileLogStore.open(dir, true);
        store.storeMeta(4, "n2");
        store.append(List.of(
                new LogEntry(1, 1, Command.noop()),
                new LogEntry(2, 4, Command.put("c", 1, "k", "v"))));
        FileLogStore again = FileLogStore.open(dir, true);
        assertEquals(4, again.loadMeta().currentTerm());
        assertEquals("n2", again.loadMeta().votedFor());
        List<LogEntry> entries = again.loadEntries();
        assertEquals(2, entries.size());
        assertEquals("v", entries.get(1).command().value());
    }

    @Test
    void tornTailIsTruncated() throws Exception {
        FileLogStore store = FileLogStore.open(dir, true);
        store.append(List.of(new LogEntry(1, 1, Command.put("c", 1, "k", "v"))));
        try (RandomAccessFile file = new RandomAccessFile(dir.resolve("wal.log").toFile(), "rw")) {
            file.seek(file.length());
            file.write(new byte[]{9, 9, 9, 9, 1, 2});
        }
        FileLogStore again = FileLogStore.open(dir, true);
        assertEquals(1, again.loadEntries().size());
        assertEquals("v", again.loadEntries().get(0).command().value());
    }

    @Test
    void snapshotDropsCompactedPrefix() {
        FileLogStore store = FileLogStore.open(dir, true);
        store.append(List.of(
                new LogEntry(1, 1, Command.noop()),
                new LogEntry(2, 1, Command.put("c", 1, "k", "v"))));
        store.saveSnapshot(new LogStore.Snapshot(1, 1, new byte[]{1, 2, 3}));
        FileLogStore again = FileLogStore.open(dir, true);
        LogStore.Snapshot snapshot = again.loadSnapshot();
        assertEquals(1, snapshot.lastIncludedIndex());
        assertEquals(1, again.loadEntries().size());
        assertEquals(2, again.loadEntries().get(0).index());
        store.rewrite(List.of());
        assertTrue(FileLogStore.open(dir, true).loadEntries().isEmpty());
        assertNull(FileLogStore.open(dir.resolve("empty"), false).loadMeta().votedFor());
    }
}
