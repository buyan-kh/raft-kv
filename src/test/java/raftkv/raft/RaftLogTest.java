package raftkv.raft;

import org.junit.jupiter.api.Test;
import raftkv.raft.Types.Command;
import raftkv.raft.Types.LogEntry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RaftLogTest {
    @Test
    void appendTruncateAndCompact() {
        RaftLog log = new RaftLog();
        assertEquals(0, log.lastIndex());
        assertEquals(0, log.lastTerm());
        log.append(new LogEntry(1, 1, Command.noop()));
        log.append(new LogEntry(2, 1, Command.put("c", 1, "a", "1")));
        log.append(new LogEntry(3, 2, Command.put("c", 2, "a", "2")));
        log.truncateFrom(3);
        assertEquals(2, log.lastIndex());
        log.append(new LogEntry(3, 3, Command.put("c", 3, "a", "3")));
        log.compact(2, 1);
        assertEquals(2, log.lastIncludedIndex());
        assertEquals(1, log.termAt(2));
        assertEquals(3, log.lastIndex());
        assertEquals(3, log.entryAt(3).term());
        assertEquals(1, log.slice(3, 3, 10).size());
        assertThrows(IllegalArgumentException.class, () -> log.truncateFrom(2));
    }
}
