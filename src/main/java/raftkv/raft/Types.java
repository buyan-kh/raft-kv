package raftkv.raft;

import java.util.List;
import java.util.Random;

/** Shared value types for the Raft core. Kept together so the protocol reads as one vocabulary. */
public final class Types {
    private Types() {}

    public enum Role {
        FOLLOWER,
        PRE_CANDIDATE,
        CANDIDATE,
        LEADER
    }

    public enum Op {
        NOOP,
        PUT,
        DELETE
    }

    public enum ClientStatus {
        /** The state machine applied the command, or the read completed. */
        OK,
        /** This attempt was rejected before it touched the log. */
        NOT_LEADER,
        /** The leader could not confirm a quorum in time to serve a read or accept a write. */
        NO_QUORUM,
        /**
         * The command may already be in the log. The caller must retry the same client sequence
         * number; a new sequence number could apply the write twice.
         */
        UNKNOWN
    }

    /**
     * A client command. {@code clientId} + {@code seq} is the idempotency key.
     * Each client keeps a single in-flight command and retries that same sequence number.
     */
    public record Command(Op op, String clientId, long seq, String key, String value) {
        public Command {
            if (op == null) {
                throw new IllegalArgumentException("op");
            }
            clientId = clientId == null ? "" : clientId;
            key = key == null ? "" : key;
            value = value == null ? "" : value;
        }

        public static Command noop() {
            return new Command(Op.NOOP, "", 0, "", "");
        }

        public static Command put(String clientId, long seq, String key, String value) {
            return new Command(Op.PUT, clientId, seq, key, value);
        }

        public static Command delete(String clientId, long seq, String key) {
            return new Command(Op.DELETE, clientId, seq, key, "");
        }
    }

    public record LogEntry(int index, int term, Command command) {
        public LogEntry {
            if (index < 1) {
                throw new IllegalArgumentException("index " + index);
            }
            if (term < 1) {
                throw new IllegalArgumentException("term " + term);
            }
            if (command == null) {
                throw new IllegalArgumentException("command");
            }
        }
    }

    public record ClientResult(
            ClientStatus status,
            String leaderId,
            int logIndex,
            boolean found,
            String value) {

        public static ClientResult ok(int logIndex, boolean found, String value, String leaderId) {
            return new ClientResult(ClientStatus.OK, leaderId, logIndex, found, value);
        }

        public static ClientResult notLeader(String leaderId) {
            return new ClientResult(ClientStatus.NOT_LEADER, leaderId, 0, false, null);
        }

        public static ClientResult noQuorum(String leaderId) {
            return new ClientResult(ClientStatus.NO_QUORUM, leaderId, 0, false, null);
        }

        public static ClientResult unknown(String leaderId) {
            return new ClientResult(ClientStatus.UNKNOWN, leaderId, 0, false, null);
        }

        public boolean ok() {
            return status == ClientStatus.OK;
        }
    }

    public record RequestVote(
            int term,
            String candidateId,
            int lastLogIndex,
            int lastLogTerm,
            boolean prevote) {}

    /**
     * {@code term} is the receiver's current term. {@code requestTerm} echoes the campaign
     * so a delayed reply from an older pre-vote or election is ignored.
     */
    public record RequestVoteReply(int term, boolean voteGranted, boolean prevote, int requestTerm) {}

    public record AppendEntries(
            int term,
            String leaderId,
            int prevLogIndex,
            int prevLogTerm,
            List<LogEntry> entries,
            int leaderCommit,
            long stamp) {}

    public record AppendEntriesReply(
            int term,
            boolean success,
            int matchIndex,
            int conflictIndex,
            int conflictTerm,
            long stamp) {}

    public record InstallSnapshot(
            int term,
            String leaderId,
            int lastIncludedIndex,
            int lastIncludedTerm,
            byte[] data,
            long stamp) {}

    public record InstallSnapshotReply(int term, boolean success, int matchIndex, long stamp) {}

    public record LogCell(int index, int term, Op op, String key, String value) {}

    public record RaftView(
            String id,
            String role,
            int term,
            int commit,
            int applied,
            int lastIndex,
            int snapshotIndex,
            String votedFor,
            String leaderId,
            List<LogCell> log) {}

    public record Status(
            String id,
            String role,
            int term,
            String leaderId,
            int commitIndex,
            int lastApplied,
            int lastLogIndex,
            int snapshotIndex,
            String votedFor) {}

    /**
     * Timing knobs. Heartbeat must stay well below both the election window and the pre-vote
     * grant silence. {@code grantSilenceMs} may be shorter than this node's own election timer
     * so a slow node can vote for a faster one without starting a rival campaign.
     */
    public record RaftConfig(
            int electionMinMs,
            int electionMaxMs,
            int heartbeatMs,
            int snapshotEvery,
            int maxAppendEntries,
            int grantSilenceMs) {

        public RaftConfig {
            if (electionMinMs < 1 || electionMaxMs < electionMinMs) {
                throw new IllegalArgumentException("election window");
            }
            if (heartbeatMs < 1 || heartbeatMs >= electionMinMs || heartbeatMs >= grantSilenceMs) {
                throw new IllegalArgumentException("heartbeat must be shorter than election and grant silence");
            }
            if (snapshotEvery < 1 || maxAppendEntries < 1 || grantSilenceMs < 1) {
                throw new IllegalArgumentException("positive limits required");
            }
        }

        public static RaftConfig standard() {
            return new RaftConfig(300, 500, 50, 64, 32, 300);
        }

        public static RaftConfig sim(
                int electionMinMs,
                int electionMaxMs,
                int heartbeatMs,
                int snapshotEvery,
                int grantSilenceMs) {
            return new RaftConfig(electionMinMs, electionMaxMs, heartbeatMs, snapshotEvery, 32, grantSilenceMs);
        }
    }

    public interface Clock {
        long millis();
    }

    public static final class SystemClock implements Clock {
        @Override
        public long millis() {
            return System.currentTimeMillis();
        }
    }

    public interface RandomSource {
        int nextInt(int bound);
    }

    public static final class JdkRandom implements RandomSource {
        private final Random random;

        public JdkRandom(long seed) {
            this.random = new Random(seed);
        }

        @Override
        public int nextInt(int bound) {
            if (bound <= 0) {
                return 0;
            }
            return random.nextInt(bound);
        }
    }

    /** Hooks for traces and invariants. All callbacks run on the node thread. */
    public interface Observer {
        Observer NONE = new Observer() {};

        default void role(String id, String role, int term) {}

        default void commit(String id, int index) {}

        default void snapshot(String id, int index) {}

        default void vote(String id, String candidateId, boolean granted, int term, boolean prevote) {}

        default void note(String id, String message) {}
    }

    public interface Transport {
        void sendRequestVote(String to, RequestVote message);

        void sendAppendEntries(String to, AppendEntries message);

        void sendInstallSnapshot(String to, InstallSnapshot message);
    }
}
