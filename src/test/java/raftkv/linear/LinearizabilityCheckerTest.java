package raftkv.linear;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LinearizabilityCheckerTest {
    @Test
    void sequentialWritesAndReadsAreLinearizable() {
        History history = new History();
        int w1 = history.beginWrite(0, "k", "1", false);
        history.end(w1, 10, History.Status.OK, null);
        int w2 = history.beginWrite(20, "k", "2", false);
        history.end(w2, 30, History.Status.OK, null);
        int read = history.beginRead(40, "k");
        history.end(read, 50, History.Status.OK, "2");
        assertTrue(new LinearizabilityChecker().check(history).ok());
    }

    @Test
    void staleReadAfterBothWritesIsRejected() {
        History history = new History();
        int w1 = history.beginWrite(0, "k", "1", false);
        history.end(w1, 10, History.Status.OK, null);
        int w2 = history.beginWrite(20, "k", "2", false);
        history.end(w2, 30, History.Status.OK, null);
        int read = history.beginRead(40, "k");
        history.end(read, 50, History.Status.OK, "1");
        assertFalse(new LinearizabilityChecker().check(history).ok());
    }

    @Test
    void overlappingWritesMayBeSeenInEitherOrder() {
        History history = new History();
        int w1 = history.beginWrite(0, "k", "1", false);
        history.end(w1, 10, History.Status.OK, null);
        int w2 = history.beginWrite(5, "k", "2", false);
        history.end(w2, 15, History.Status.OK, null);
        int read = history.beginRead(6, "k");
        history.end(read, 12, History.Status.OK, "1");
        assertTrue(new LinearizabilityChecker().check(history).ok());
    }

    @Test
    void indeterminateWriteCanBeDropped() {
        History history = new History();
        int lost = history.beginWrite(0, "k", "1", false);
        history.end(lost, 10, History.Status.INFO, null);
        int write = history.beginWrite(20, "k", "2", false);
        history.end(write, 30, History.Status.OK, null);
        int read = history.beginRead(40, "k");
        history.end(read, 50, History.Status.OK, "2");
        assertTrue(new LinearizabilityChecker().check(history).ok());
    }

    @Test
    void failedOperationsAreIgnored() {
        History history = new History();
        int failed = history.beginWrite(0, "k", "nope", false);
        history.end(failed, 5, History.Status.FAIL, null);
        int read = history.beginRead(10, "k");
        history.end(read, 12, History.Status.OK, null);
        assertTrue(new LinearizabilityChecker().check(history).ok());
    }

    @Test
    void keysAreIndependent() {
        History history = new History();
        int a = history.beginWrite(0, "a", "1", false);
        history.end(a, 5, History.Status.OK, null);
        int b = history.beginWrite(0, "b", "9", false);
        history.end(b, 5, History.Status.OK, null);
        int ra = history.beginRead(6, "a");
        history.end(ra, 8, History.Status.OK, "1");
        int rb = history.beginRead(6, "b");
        history.end(rb, 8, History.Status.OK, "9");
        assertTrue(new LinearizabilityChecker().check(history).ok());
    }
}
