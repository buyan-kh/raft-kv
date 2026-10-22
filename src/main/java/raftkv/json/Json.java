package raftkv.json;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A small JSON codec for the peer RPC, the client API, and trace files.
 * The accepted subset is objects, arrays, strings, numbers, booleans, and null.
 */
public final class Json {
    private Json() {}

    public sealed interface Val permits Obj, Arr, Str, Num, Bool, NullVal {}

    public static final class Obj implements Val {
        private final LinkedHashMap<String, Val> map = new LinkedHashMap<>();

        public Obj put(String key, Val value) {
            map.put(key, value == null ? new NullVal() : value);
            return this;
        }

        public Obj put(String key, String value) {
            return put(key, value == null ? new NullVal() : new Str(value));
        }

        public Obj put(String key, long value) {
            return put(key, Num.of(value));
        }

        public Obj put(String key, int value) {
            return put(key, Num.of(value));
        }

        public Obj put(String key, double value) {
            return put(key, Num.of(value));
        }

        public Obj put(String key, boolean value) {
            return put(key, new Bool(value));
        }

        public boolean has(String key) {
            return map.containsKey(key) && !(map.get(key) instanceof NullVal);
        }

        public Val get(String key) {
            return map.get(key);
        }

        public Obj obj(String key) {
            Val v = map.get(key);
            if (v instanceof Obj o) {
                return o;
            }
            throw new IllegalArgumentException("expected object at " + key);
        }

        public Arr arr(String key) {
            Val v = map.get(key);
            if (v instanceof Arr a) {
                return a;
            }
            throw new IllegalArgumentException("expected array at " + key);
        }

        public String str(String key) {
            Val v = map.get(key);
            if (v instanceof Str s) {
                return s.value();
            }
            if (v == null || v instanceof NullVal) {
                return null;
            }
            throw new IllegalArgumentException("expected string at " + key);
        }

        public int i(String key) {
            return num(key).intValue();
        }

        public long l(String key) {
            return num(key).longValue();
        }

        public double d(String key) {
            return num(key).doubleValue();
        }

        public boolean b(String key) {
            Val v = map.get(key);
            if (v instanceof Bool bool) {
                return bool.value();
            }
            throw new IllegalArgumentException("expected bool at " + key);
        }

        public Map<String, Val> fields() {
            return map;
        }

        private Num num(String key) {
            Val v = map.get(key);
            if (v instanceof Num n) {
                return n;
            }
            throw new IllegalArgumentException("expected number at " + key);
        }
    }

    public static final class Arr implements Val {
        private final List<Val> list = new ArrayList<>();

        public Arr add(Val value) {
            list.add(value == null ? new NullVal() : value);
            return this;
        }

        public Arr add(String value) {
            return add(value == null ? new NullVal() : new Str(value));
        }

        public Arr add(long value) {
            return add(Num.of(value));
        }

        public Arr add(int value) {
            return add(Num.of(value));
        }

        public int size() {
            return list.size();
        }

        public Val get(int index) {
            return list.get(index);
        }

        public Obj obj(int index) {
            Val v = list.get(index);
            if (v instanceof Obj o) {
                return o;
            }
            throw new IllegalArgumentException("expected object at " + index);
        }

        public List<Val> values() {
            return list;
        }
    }

    public record Str(String value) implements Val {}

    public static final class Num implements Val {
        private final double value;
        private final boolean integral;

        private Num(double value, boolean integral) {
            this.value = value;
            this.integral = integral;
        }

        public static Num of(long value) {
            return new Num(value, true);
        }

        public static Num of(double value) {
            return new Num(value, false);
        }

        public double doubleValue() {
            return value;
        }

        public long longValue() {
            return (long) value;
        }

        public int intValue() {
            return (int) value;
        }

        public boolean integral() {
            return integral;
        }
    }

    public record Bool(boolean value) implements Val {}

    public static final class NullVal implements Val {}

    public static String write(Val value) {
        StringBuilder out = new StringBuilder();
        write(value, out, 0, false);
        return out.toString();
    }

    public static String writePretty(Val value) {
        StringBuilder out = new StringBuilder();
        write(value, out, 0, true);
        out.append('\n');
        return out.toString();
    }

    private static void write(Val value, StringBuilder out, int indent, boolean pretty) {
        switch (value) {
            case null -> out.append("null");
            case NullVal ignored -> out.append("null");
            case Bool b -> out.append(b.value() ? "true" : "false");
            case Num n -> {
                if (n.integral) {
                    out.append(n.longValue());
                } else {
                    out.append(n.doubleValue());
                }
            }
            case Str s -> writeString(s.value(), out);
            case Arr a -> {
                out.append('[');
                List<Val> items = a.values();
                for (int i = 0; i < items.size(); i++) {
                    if (pretty) {
                        out.append('\n').append("  ".repeat(indent + 1));
                    }
                    write(items.get(i), out, indent + 1, pretty);
                    if (i + 1 < items.size()) {
                        out.append(',');
                    }
                }
                if (pretty && !items.isEmpty()) {
                    out.append('\n').append("  ".repeat(indent));
                }
                out.append(']');
            }
            case Obj o -> {
                out.append('{');
                int i = 0;
                int n = o.fields().size();
                for (var e : o.fields().entrySet()) {
                    if (pretty) {
                        out.append('\n').append("  ".repeat(indent + 1));
                    }
                    writeString(e.getKey(), out);
                    out.append(pretty ? ": " : ":");
                    write(e.getValue(), out, indent + 1, pretty);
                    if (++i < n) {
                        out.append(',');
                    }
                }
                if (pretty && n > 0) {
                    out.append('\n').append("  ".repeat(indent));
                }
                out.append('}');
            }
        }
    }

    private static void writeString(String value, StringBuilder out) {
        out.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        out.append('"');
    }

    public static Val parse(String text) {
        Parser parser = new Parser(text);
        Val value = parser.parseValue();
        parser.skipWs();
        if (!parser.eof()) {
            throw parser.error("trailing input");
        }
        return value;
    }

    public static Obj parseObj(String text) {
        Val value = parse(text);
        if (value instanceof Obj obj) {
            return obj;
        }
        throw new IllegalArgumentException("expected JSON object");
    }

    private static final class Parser {
        private final String text;
        private int i;

        private Parser(String text) {
            this.text = text;
        }

        private Val parseValue() {
            skipWs();
            if (eof()) {
                throw error("unexpected end");
            }
            char c = text.charAt(i);
            return switch (c) {
                case '{' -> parseObject();
                case '[' -> parseArray();
                case '"' -> new Str(parseString());
                case 't' -> {
                    expect("true");
                    yield new Bool(true);
                }
                case 'f' -> {
                    expect("false");
                    yield new Bool(false);
                }
                case 'n' -> {
                    expect("null");
                    yield new NullVal();
                }
                default -> {
                    if (c == '-' || (c >= '0' && c <= '9')) {
                        yield parseNumber();
                    }
                    throw error("unexpected character");
                }
            };
        }

        private Obj parseObject() {
            expect("{");
            Obj obj = new Obj();
            skipWs();
            if (peek('}')) {
                i++;
                return obj;
            }
            while (true) {
                skipWs();
                if (eof() || text.charAt(i) != '"') {
                    throw error("expected object key");
                }
                String key = parseString();
                skipWs();
                expect(":");
                obj.put(key, parseValue());
                skipWs();
                if (peek('}')) {
                    i++;
                    return obj;
                }
                expect(",");
            }
        }

        private Arr parseArray() {
            expect("[");
            Arr arr = new Arr();
            skipWs();
            if (peek(']')) {
                i++;
                return arr;
            }
            while (true) {
                arr.add(parseValue());
                skipWs();
                if (peek(']')) {
                    i++;
                    return arr;
                }
                expect(",");
            }
        }

        private String parseString() {
            expect("\"");
            StringBuilder sb = new StringBuilder();
            while (!eof()) {
                char c = text.charAt(i++);
                if (c == '"') {
                    return sb.toString();
                }
                if (c == '\\') {
                    if (eof()) {
                        throw error("bad escape");
                    }
                    char e = text.charAt(i++);
                    switch (e) {
                        case '"', '\\', '/' -> sb.append(e);
                        case 'b' -> sb.append('\b');
                        case 'f' -> sb.append('\f');
                        case 'n' -> sb.append('\n');
                        case 'r' -> sb.append('\r');
                        case 't' -> sb.append('\t');
                        case 'u' -> {
                            if (i + 4 > text.length()) {
                                throw error("bad unicode escape");
                            }
                            int cp = Integer.parseInt(text.substring(i, i + 4), 16);
                            sb.append((char) cp);
                            i += 4;
                        }
                        default -> throw error("bad escape");
                    }
                } else if (c < 0x20) {
                    throw error("raw control character in string");
                } else {
                    sb.append(c);
                }
            }
            throw error("unterminated string");
        }

        private Num parseNumber() {
            int start = i;
            if (peek('-')) {
                i++;
            }
            if (eof() || !digit(text.charAt(i))) {
                throw error("bad number");
            }
            if (text.charAt(i) == '0') {
                i++;
            } else {
                while (!eof() && digit(text.charAt(i))) {
                    i++;
                }
            }
            boolean integral = true;
            if (!eof() && text.charAt(i) == '.') {
                integral = false;
                i++;
                if (eof() || !digit(text.charAt(i))) {
                    throw error("bad number");
                }
                while (!eof() && digit(text.charAt(i))) {
                    i++;
                }
            }
            if (!eof() && (text.charAt(i) == 'e' || text.charAt(i) == 'E')) {
                integral = false;
                i++;
                if (!eof() && (text.charAt(i) == '+' || text.charAt(i) == '-')) {
                    i++;
                }
                if (eof() || !digit(text.charAt(i))) {
                    throw error("bad number");
                }
                while (!eof() && digit(text.charAt(i))) {
                    i++;
                }
            }
            String token = text.substring(start, i);
            if (integral) {
                try {
                    return Num.of(Long.parseLong(token));
                } catch (NumberFormatException ex) {
                    return Num.of(Double.parseDouble(token));
                }
            }
            return Num.of(Double.parseDouble(token));
        }

        private void skipWs() {
            while (!eof()) {
                char c = text.charAt(i);
                if (c == ' ' || c == '\n' || c == '\r' || c == '\t') {
                    i++;
                } else {
                    return;
                }
            }
        }

        private void expect(String literal) {
            if (!text.startsWith(literal, i)) {
                throw error("expected " + literal);
            }
            i += literal.length();
        }

        private boolean peek(char c) {
            return !eof() && text.charAt(i) == c;
        }

        private boolean eof() {
            return i >= text.length();
        }

        private static boolean digit(char c) {
            return c >= '0' && c <= '9';
        }

        private IllegalArgumentException error(String message) {
            return new IllegalArgumentException(message + " at " + i);
        }
    }
}
