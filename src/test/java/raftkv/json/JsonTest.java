package raftkv.json;

import org.junit.jupiter.api.Test;
import raftkv.json.Json.Arr;
import raftkv.json.Json.Obj;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonTest {
    @Test
    void roundTrip() {
        Obj obj = new Obj()
                .put("name", "n1")
                .put("term", 3)
                .put("ok", true)
                .put("ratio", 1.5)
                .put("leader", (String) null);
        Arr log = new Arr().add(1).add("x");
        obj.put("log", log);
        Obj parsed = Json.parseObj(Json.write(obj));
        assertEquals("n1", parsed.str("name"));
        assertEquals(3, parsed.i("term"));
        assertTrue(parsed.b("ok"));
        assertEquals(1.5, parsed.d("ratio"));
        assertNull(parsed.str("leader"));
        assertFalse(parsed.has("leader"));
        assertEquals(2, parsed.arr("log").size());
        assertEquals("x", ((Json.Str) parsed.arr("log").get(1)).value());
    }

    @Test
    void escapes() {
        Obj parsed = Json.parseObj(Json.write(new Obj().put("s", "a\"b\\c\n")));
        assertEquals("a\"b\\c\n", parsed.str("s"));
    }
}
