package raftkv.persist;

import raftkv.raft.Types.Command;
import raftkv.raft.Types.LogEntry;
import raftkv.raft.Types.Op;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Directory layout:
 * <pre>
 *   meta.bin       term + votedFor, replaced atomically
 *   snapshot.bin   last included index/term and state-machine bytes
 *   wal.log        length-prefixed frames; a torn tail is truncated on open
 * </pre>
 * Snapshot files are replaced before the WAL is rewritten, so a crash cannot
 * drop a compacted prefix that is not yet in a snapshot.
 */
public final class FileLogStore implements LogStore {
    private static final int META_MAGIC = 0x524D4554; // RMET
    private static final int SNAP_MAGIC = 0x52534E50; // RSNP
    private static final int WAL_MAGIC = 0x524C4F47; // RLOG

    private final Path dir;
    private final boolean fsync;
    private final Path metaPath;
    private final Path snapPath;
    private final Path walPath;

    private FileLogStore(Path dir, boolean fsync) {
        this.dir = dir;
        this.fsync = fsync;
        this.metaPath = dir.resolve("meta.bin");
        this.snapPath = dir.resolve("snapshot.bin");
        this.walPath = dir.resolve("wal.log");
    }

    public static FileLogStore open(Path dir, boolean fsync) {
        try {
            Files.createDirectories(dir);
            FileLogStore store = new FileLogStore(dir, fsync);
            if (!Files.exists(store.walPath)) {
                Files.createFile(store.walPath);
            }
            store.truncateTornTail();
            return store;
        } catch (IOException e) {
            throw new IllegalStateException("open log store " + dir, e);
        }
    }

    @Override
    public Meta loadMeta() {
        if (!Files.exists(metaPath)) {
            return new Meta(0, null);
        }
        try {
            byte[] bytes = Files.readAllBytes(metaPath);
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            int magic = in.readInt();
            if (magic != META_MAGIC) {
                throw new IllegalStateException("bad meta magic");
            }
            int term = in.readInt();
            String voted = in.readUTF();
            int crc = in.readInt();
            if (crc != crc(bytes, bytes.length - Integer.BYTES)) {
                throw new IllegalStateException("meta checksum mismatch");
            }
            return new Meta(term, voted.isEmpty() ? null : voted);
        } catch (IOException e) {
            throw new IllegalStateException("read meta", e);
        }
    }

    @Override
    public void storeMeta(int currentTerm, String votedFor) {
        try {
            ByteArrayOutputStream raw = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(raw);
            out.writeInt(META_MAGIC);
            out.writeInt(currentTerm);
            out.writeUTF(votedFor == null ? "" : votedFor);
            out.flush();
            byte[] body = raw.toByteArray();
            out.writeInt(crc(body, body.length));
            out.flush();
            atomicWrite(metaPath, raw.toByteArray());
        } catch (IOException e) {
            throw new IllegalStateException("write meta", e);
        }
    }

    @Override
    public Snapshot loadSnapshot() {
        if (!Files.exists(snapPath)) {
            return null;
        }
        try {
            byte[] bytes = Files.readAllBytes(snapPath);
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            if (in.readInt() != SNAP_MAGIC) {
                throw new IllegalStateException("bad snapshot magic");
            }
            int index = in.readInt();
            int term = in.readInt();
            int len = in.readInt();
            if (len < 0 || len > bytes.length) {
                throw new IllegalStateException("bad snapshot length");
            }
            byte[] data = in.readNBytes(len);
            if (data.length != len) {
                throw new IllegalStateException("short snapshot");
            }
            int crc = in.readInt();
            if (crc != crc(bytes, bytes.length - Integer.BYTES)) {
                throw new IllegalStateException("snapshot checksum mismatch");
            }
            return new Snapshot(index, term, data);
        } catch (IOException e) {
            throw new IllegalStateException("read snapshot", e);
        }
    }

    @Override
    public void saveSnapshot(Snapshot snapshot) {
        try {
            ByteArrayOutputStream raw = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(raw);
            out.writeInt(SNAP_MAGIC);
            out.writeInt(snapshot.lastIncludedIndex());
            out.writeInt(snapshot.lastIncludedTerm());
            byte[] data = snapshot.data();
            out.writeInt(data.length);
            out.write(data);
            out.flush();
            byte[] body = raw.toByteArray();
            out.writeInt(crc(body, body.length));
            out.flush();
            atomicWrite(snapPath, raw.toByteArray());
        } catch (IOException e) {
            throw new IllegalStateException("write snapshot", e);
        }
    }

    @Override
    public List<LogEntry> loadEntries() {
        truncateTornTail();
        try (DataInputStream in = new DataInputStream(Files.newInputStream(walPath))) {
            ArrayList<LogEntry> entries = new ArrayList<>();
            while (true) {
                int magic;
                try {
                    magic = in.readInt();
                } catch (EOFException eof) {
                    break;
                }
                if (magic != WAL_MAGIC) {
                    break;
                }
                int index = in.readInt();
                int term = in.readInt();
                int len = in.readInt();
                byte[] payload = in.readNBytes(len);
                int crc = in.readInt();
                byte[] framed = frame(index, term, payload);
                int expect = crc(framed, framed.length);
                if (crc != expect || payload.length != len) {
                    break;
                }
                entries.add(new LogEntry(index, term, decodeCommand(payload)));
            }
            Snapshot snapshot = loadSnapshot();
            if (snapshot != null) {
                entries.removeIf(entry -> entry.index() <= snapshot.lastIncludedIndex());
            }
            return List.copyOf(entries);
        } catch (IOException e) {
            throw new IllegalStateException("read wal", e);
        }
    }

    @Override
    public void append(List<LogEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        try (RandomAccessFile file = new RandomAccessFile(walPath.toFile(), "rw")) {
            file.seek(file.length());
            for (LogEntry entry : entries) {
                byte[] payload = encodeCommand(entry.command());
                byte[] framed = frame(entry.index(), entry.term(), payload);
                file.writeInt(WAL_MAGIC);
                file.writeInt(entry.index());
                file.writeInt(entry.term());
                file.writeInt(payload.length);
                file.write(payload);
                file.writeInt(crc(framed, framed.length));
            }
            if (fsync) {
                file.getFD().sync();
            }
        } catch (IOException e) {
            throw new IllegalStateException("append wal", e);
        }
    }

    @Override
    public void rewrite(List<LogEntry> entries) {
        Path tmp = dir.resolve("wal.log.tmp");
        try (RandomAccessFile file = new RandomAccessFile(tmp.toFile(), "rw")) {
            file.setLength(0);
            for (LogEntry entry : entries) {
                byte[] payload = encodeCommand(entry.command());
                byte[] framed = frame(entry.index(), entry.term(), payload);
                file.writeInt(WAL_MAGIC);
                file.writeInt(entry.index());
                file.writeInt(entry.term());
                file.writeInt(payload.length);
                file.write(payload);
                file.writeInt(crc(framed, framed.length));
            }
            if (fsync) {
                file.getFD().sync();
            }
        } catch (IOException e) {
            throw new IllegalStateException("rewrite wal", e);
        }
        moveAtomically(tmp, walPath);
    }

    private void truncateTornTail() {
        try (RandomAccessFile file = new RandomAccessFile(walPath.toFile(), "rw")) {
            long good = 0;
            while (file.getFilePointer() + 16 <= file.length()) {
                long mark = file.getFilePointer();
                int magic;
                try {
                    magic = file.readInt();
                } catch (EOFException eof) {
                    break;
                }
                if (magic != WAL_MAGIC) {
                    break;
                }
                if (file.getFilePointer() + 12 > file.length()) {
                    break;
                }
                int index = file.readInt();
                int term = file.readInt();
                int len = file.readInt();
                if (len < 0 || file.getFilePointer() + len + 4L > file.length()) {
                    break;
                }
                byte[] payload = new byte[len];
                file.readFully(payload);
                int crc = file.readInt();
                byte[] framed = frame(index, term, payload);
                if (crc != crc(framed, framed.length)) {
                    break;
                }
                good = file.getFilePointer();
                if (mark >= good) {
                    break;
                }
            }
            if (good < file.length()) {
                file.setLength(good);
                if (fsync) {
                    file.getFD().sync();
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("repair wal", e);
        }
    }

    private void atomicWrite(Path target, byte[] bytes) throws IOException {
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        Files.write(tmp, bytes);
        if (fsync) {
            try (RandomAccessFile file = new RandomAccessFile(tmp.toFile(), "rw")) {
                file.getFD().sync();
            }
        }
        moveAtomically(tmp, target);
    }

    private void moveAtomically(Path tmp, Path target) {
        try {
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException ex) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new IllegalStateException("replace " + target, e);
        }
    }

    private static byte[] frame(int index, int term, byte[] payload) {
        ByteArrayOutputStream raw = new ByteArrayOutputStream(16 + payload.length);
        DataOutputStream out = new DataOutputStream(raw);
        try {
            out.writeInt(index);
            out.writeInt(term);
            out.writeInt(payload.length);
            out.write(payload);
            out.flush();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return raw.toByteArray();
    }

    public static byte[] encodeCommand(Command command) {
        try {
            ByteArrayOutputStream raw = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(raw);
            out.writeByte(command.op().ordinal());
            out.writeUTF(command.clientId());
            out.writeLong(command.seq());
            out.writeUTF(command.key());
            out.writeUTF(command.value());
            out.flush();
            return raw.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Command decodeCommand(byte[] payload) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
            Op op = Op.values()[in.readUnsignedByte()];
            String clientId = in.readUTF();
            long seq = in.readLong();
            String key = in.readUTF();
            String value = in.readUTF();
            return new Command(op, clientId, seq, key, value);
        } catch (IOException e) {
            throw new IllegalStateException("bad command", e);
        }
    }

    private static int crc(byte[] bytes, int length) {
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, length);
        return (int) crc.getValue();
    }
}
