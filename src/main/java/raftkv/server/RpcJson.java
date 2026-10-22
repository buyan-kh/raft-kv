package raftkv.server;

import raftkv.json.Json;
import raftkv.json.Json.Obj;
import raftkv.raft.Types.AppendEntries;
import raftkv.raft.Types.AppendEntriesReply;
import raftkv.raft.Types.ClientResult;
import raftkv.raft.Types.Command;
import raftkv.raft.Types.InstallSnapshot;
import raftkv.raft.Types.InstallSnapshotReply;
import raftkv.raft.Types.LogEntry;
import raftkv.raft.Types.Op;
import raftkv.raft.Types.RequestVote;
import raftkv.raft.Types.RequestVoteReply;
import raftkv.raft.Types.Status;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/** JSON mapping for peer RPCs and the client API. */
public final class RpcJson {
    private RpcJson() {}

    public static String requestVote(RequestVote message) {
        return Json.write(new Obj()
                .put("term", message.term())
                .put("candidateId", message.candidateId())
                .put("lastLogIndex", message.lastLogIndex())
                .put("lastLogTerm", message.lastLogTerm())
                .put("prevote", message.prevote()));
    }

    public static RequestVote requestVote(String json) {
        Obj obj = Json.parseObj(json);
        return new RequestVote(obj.i("term"), obj.str("candidateId"), obj.i("lastLogIndex"), obj.i("lastLogTerm"), obj.b("prevote"));
    }

    public static String requestVoteReply(RequestVoteReply message) {
        return Json.write(new Obj()
                .put("term", message.term())
                .put("voteGranted", message.voteGranted())
                .put("prevote", message.prevote())
                .put("requestTerm", message.requestTerm()));
    }

    public static RequestVoteReply requestVoteReply(String json) {
        Obj obj = Json.parseObj(json);
        return new RequestVoteReply(obj.i("term"), obj.b("voteGranted"), obj.b("prevote"), obj.i("requestTerm"));
    }

    public static String appendEntries(AppendEntries message) {
        Json.Arr entries = new Json.Arr();
        for (LogEntry entry : message.entries()) {
            entries.add(entry(entry));
        }
        return Json.write(new Obj()
                .put("term", message.term())
                .put("leaderId", message.leaderId())
                .put("prevLogIndex", message.prevLogIndex())
                .put("prevLogTerm", message.prevLogTerm())
                .put("leaderCommit", message.leaderCommit())
                .put("stamp", message.stamp())
                .put("entries", entries));
    }

    public static AppendEntries appendEntries(String json) {
        Obj obj = Json.parseObj(json);
        ArrayList<LogEntry> entries = new ArrayList<>();
        Json.Arr raw = obj.arr("entries");
        for (int i = 0; i < raw.size(); i++) {
            entries.add(entry(raw.obj(i)));
        }
        return new AppendEntries(
                obj.i("term"),
                obj.str("leaderId"),
                obj.i("prevLogIndex"),
                obj.i("prevLogTerm"),
                List.copyOf(entries),
                obj.i("leaderCommit"),
                obj.l("stamp"));
    }

    public static String appendEntriesReply(AppendEntriesReply message) {
        return Json.write(new Obj()
                .put("term", message.term())
                .put("success", message.success())
                .put("matchIndex", message.matchIndex())
                .put("conflictIndex", message.conflictIndex())
                .put("conflictTerm", message.conflictTerm())
                .put("stamp", message.stamp()));
    }

    public static AppendEntriesReply appendEntriesReply(String json) {
        Obj obj = Json.parseObj(json);
        return new AppendEntriesReply(
                obj.i("term"), obj.b("success"), obj.i("matchIndex"), obj.i("conflictIndex"), obj.i("conflictTerm"), obj.l("stamp"));
    }

    public static String installSnapshot(InstallSnapshot message) {
        return Json.write(new Obj()
                .put("term", message.term())
                .put("leaderId", message.leaderId())
                .put("lastIncludedIndex", message.lastIncludedIndex())
                .put("lastIncludedTerm", message.lastIncludedTerm())
                .put("stamp", message.stamp())
                .put("data", Base64.getEncoder().encodeToString(message.data())));
    }

    public static InstallSnapshot installSnapshot(String json) {
        Obj obj = Json.parseObj(json);
        return new InstallSnapshot(
                obj.i("term"),
                obj.str("leaderId"),
                obj.i("lastIncludedIndex"),
                obj.i("lastIncludedTerm"),
                Base64.getDecoder().decode(obj.str("data")),
                obj.l("stamp"));
    }

    public static String installSnapshotReply(InstallSnapshotReply message) {
        return Json.write(new Obj()
                .put("term", message.term())
                .put("success", message.success())
                .put("matchIndex", message.matchIndex())
                .put("stamp", message.stamp()));
    }

    public static InstallSnapshotReply installSnapshotReply(String json) {
        Obj obj = Json.parseObj(json);
        return new InstallSnapshotReply(obj.i("term"), obj.b("success"), obj.i("matchIndex"), obj.l("stamp"));
    }

    public static String clientResult(ClientResult result, String self) {
        Obj obj = new Obj()
                .put("status", result.status().name().toLowerCase())
                .put("leaderId", result.leaderId())
                .put("logIndex", result.logIndex())
                .put("found", result.found())
                .put("node", self);
        if (result.value() != null) {
            obj.put("value", result.value());
        }
        return Json.write(obj);
    }

    public static String status(Status status) {
        return Json.write(new Obj()
                .put("id", status.id())
                .put("role", status.role())
                .put("term", status.term())
                .put("leaderId", status.leaderId())
                .put("commitIndex", status.commitIndex())
                .put("lastApplied", status.lastApplied())
                .put("lastLogIndex", status.lastLogIndex())
                .put("snapshotIndex", status.snapshotIndex())
                .put("votedFor", status.votedFor()));
    }

    public static Command command(Op op, String json, String key) {
        Obj obj = Json.parseObj(json);
        long seq = obj.l("seq");
        String clientId = obj.str("clientId");
        String value = obj.has("value") ? obj.str("value") : "";
        return new Command(op, clientId, seq, key, value == null ? "" : value);
    }

    private static Obj entry(LogEntry entry) {
        Command command = entry.command();
        return new Obj()
                .put("index", entry.index())
                .put("term", entry.term())
                .put("op", command.op().name())
                .put("clientId", command.clientId())
                .put("seq", command.seq())
                .put("key", command.key())
                .put("value", command.value());
    }

    private static LogEntry entry(Obj obj) {
        Command command = new Command(
                Op.valueOf(obj.str("op")),
                obj.str("clientId"),
                obj.l("seq"),
                obj.str("key"),
                obj.str("value"));
        return new LogEntry(obj.i("index"), obj.i("term"), command);
    }
}
