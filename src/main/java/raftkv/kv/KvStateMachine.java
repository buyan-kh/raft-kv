package raftkv.kv;

import raftkv.persist.FileLogStore;
import raftkv.raft.Types.Command;
import raftkv.raft.Types.LogEntry;
import raftkv.raft.Types.Op;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Deterministic key-value machine. Applies each log index at most once and remembers the
 * last result per client so a retried sequence number does not write twice.
 */
public final class KvStateMachine {
    private final TreeMap<String, String> data = new TreeMap<>();
    private final HashMap<String, Long> clientSeq = new HashMap<>();
    private final HashMap<String, Result> clientResult = new HashMap<>();
    private int lastApplied;

    public record Result(boolean duplicate, boolean found, String value) {}

    public int lastApplied() {
        return lastApplied;
    }

    public String get(String key) {
        return data.get(key);
    }

    public boolean contains(String key) {
        return data.containsKey(key);
    }

    public Map<String, String> copy() {
        return Map.copyOf(data);
    }

    /** @return the previous result when this client sequence was already applied. */
    public Result ifDuplicate(Command command) {
        if (command.op() == Op.NOOP || command.clientId().isEmpty()) {
            return null;
        }
        Long seen = clientSeq.get(command.clientId());
        if (seen == null || command.seq() > seen) {
            return null;
        }
        Result result = clientResult.get(command.clientId());
        if (result != null && command.seq() == seen) {
            return new Result(true, result.found(), result.value());
        }
        // Older than the in-flight window. It already applied; do not apply it again.
        return new Result(true, result != null && result.found(), result == null ? null : result.value());
    }

    public Result apply(LogEntry entry) {
        if (entry.index() <= lastApplied) {
            Result dup = ifDuplicate(entry.command());
            return dup == null ? new Result(true, false, null) : dup;
        }
        if (entry.index() != lastApplied + 1) {
            throw new IllegalStateException("apply gap: have " + lastApplied + " got " + entry.index());
        }
        lastApplied = entry.index();
        Command command = entry.command();
        if (command.op() == Op.NOOP) {
            return new Result(false, false, null);
        }
        Result dup = ifDuplicate(command);
        if (dup != null) {
            return dup;
        }
        boolean found;
        String value;
        if (command.op() == Op.PUT) {
            data.put(command.key(), command.value());
            found = true;
            value = command.value();
        } else if (command.op() == Op.DELETE) {
            found = data.remove(command.key()) != null;
            value = null;
        } else {
            throw new IllegalStateException("unexpected op " + command.op());
        }
        Result result = new Result(false, found, value);
        clientSeq.put(command.clientId(), command.seq());
        clientResult.put(command.clientId(), result);
        return result;
    }

    public byte[] snapshot() {
        try {
            ByteArrayOutputStream raw = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(raw);
            out.writeInt(0x4B565331); // KVS1
            out.writeInt(data.size());
            for (var entry : data.entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }
            out.writeInt(clientSeq.size());
            for (var entry : clientSeq.entrySet()) {
                Result result = clientResult.get(entry.getKey());
                out.writeUTF(entry.getKey());
                out.writeLong(entry.getValue());
                out.writeBoolean(result != null && result.found());
                out.writeUTF(result == null || result.value() == null ? "" : result.value());
                out.writeBoolean(result != null && result.value() != null);
            }
            out.flush();
            return raw.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void restore(byte[] bytes, int appliedIndex) {
        data.clear();
        clientSeq.clear();
        clientResult.clear();
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            int magic = in.readInt();
            if (magic != 0x4B565331) {
                throw new IllegalStateException("bad state machine snapshot");
            }
            int n = in.readInt();
            for (int i = 0; i < n; i++) {
                data.put(in.readUTF(), in.readUTF());
            }
            int sessions = in.readInt();
            for (int i = 0; i < sessions; i++) {
                String clientId = in.readUTF();
                long seq = in.readLong();
                boolean found = in.readBoolean();
                String value = in.readUTF();
                boolean hasValue = in.readBoolean();
                clientSeq.put(clientId, seq);
                clientResult.put(clientId, new Result(true, found, hasValue ? value : null));
            }
            lastApplied = appliedIndex;
        } catch (IOException e) {
            throw new IllegalStateException("restore state machine", e);
        }
    }

    /** Exposed so the WAL tests can round-trip commands without standing up a node. */
    public static byte[] encode(Command command) {
        return FileLogStore.encodeCommand(command);
    }
}
