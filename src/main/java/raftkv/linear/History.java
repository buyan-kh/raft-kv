package raftkv.linear;

import java.util.ArrayList;
import java.util.List;

/** A real-time history of single-key reads and writes. */
public final class History {
    public enum Kind { READ, WRITE }

    public enum Status {
        /** Completed and definitely took effect. */
        OK,
        /** Definitely did not take effect. */
        FAIL,
        /** The client timed out or the leader stepped down after appending. May have taken effect. */
        INFO
    }

    public static final class Op {
        public final int id;
        public final Kind kind;
        public final String key;
        /** For a write, the value submitted. Null with {@code delete} means removal. */
        public final String writeValue;
        public final boolean delete;
        public final long start;
        public long end;
        public Status status;
        /** Value observed by an OK read. Null means the key was absent. */
        public String readValue;
        public boolean open = true;

        Op(int id, Kind kind, String key, String writeValue, boolean delete, long start) {
            this.id = id;
            this.kind = kind;
            this.key = key;
            this.writeValue = writeValue;
            this.delete = delete;
            this.start = start;
        }

        @Override
        public String toString() {
            String effect = kind == Kind.WRITE
                    ? (delete ? "DEL" : "W(" + writeValue + ")")
                    : "R->" + readValue;
            return "#" + id + " " + effect + " " + key + " [" + start + "," + end + "] " + status;
        }
    }

    private int nextId = 1;
    private final List<Op> ops = new ArrayList<>();

    public int beginWrite(long now, String key, String value, boolean delete) {
        Op op = new Op(nextId++, Kind.WRITE, key, value, delete, now);
        ops.add(op);
        return op.id;
    }

    public int beginRead(long now, String key) {
        Op op = new Op(nextId++, Kind.READ, key, null, false, now);
        ops.add(op);
        return op.id;
    }

    public void end(int id, long now, Status status, String readValue) {
        Op op = find(id);
        if (!op.open) {
            throw new IllegalStateException("op already closed: " + op);
        }
        if (now < op.start) {
            throw new IllegalStateException("op ended before it started: " + op + " now=" + now);
        }
        op.open = false;
        op.end = now;
        op.status = status;
        op.readValue = readValue;
    }

    public List<Op> ops() {
        return List.copyOf(ops);
    }

    public int size() {
        return ops.size();
    }

    private Op find(int id) {
        for (Op op : ops) {
            if (op.id == id) {
                return op;
            }
        }
        throw new IllegalArgumentException("unknown op " + id);
    }
}
