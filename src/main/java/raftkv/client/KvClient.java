package raftkv.client;

import raftkv.json.Json;
import raftkv.json.Json.Obj;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** HTTP client. Retries a write with the same sequence number so a lost response cannot double-apply. */
public final class KvClient {
    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofMillis(400))
            .build();
    private final Map<String, String> byId;
    private final List<String> urls;
    private final String clientId;
    private String current;
    private int cursor;
    private long seq;

    public KvClient(Map<String, String> baseUrls) {
        if (baseUrls.isEmpty()) {
            throw new IllegalArgumentException("no servers");
        }
        this.byId = Map.copyOf(baseUrls);
        this.urls = new ArrayList<>(baseUrls.values());
        this.current = urls.get(0);
        this.clientId = "c-" + Long.toHexString(System.nanoTime());
    }

    public void put(String key, String value, Duration timeout) {
        mutate("PUT", key, value, timeout);
    }

    public void delete(String key, Duration timeout) {
        mutate("DELETE", key, null, timeout);
    }

    /** @return the value, or null when the key is absent */
    public String get(String key, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        RuntimeException last = null;
        while (System.nanoTime() < deadline) {
            try {
                Obj body = exchange("GET", current + "/v1/kv/" + encode(key), null);
                String status = body.str("status");
                if ("ok".equals(status)) {
                    remember(body);
                    return body.has("value") ? body.str("value") : null;
                }
                follow(body);
            } catch (RuntimeException error) {
                last = error;
                advance();
            }
        }
        throw last == null ? new IllegalStateException("get timed out") : last;
    }

    public Obj status(String baseUrl, Duration timeout) {
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/status"))
                    .timeout(timeout)
                    .GET()
                    .build();
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            return Json.parseObj(response.body());
        } catch (Exception e) {
            throw new IllegalStateException("status " + baseUrl, e);
        }
    }

    private void mutate(String method, String key, String value, Duration timeout) {
        long operation = ++seq;
        long deadline = System.nanoTime() + timeout.toNanos();
        Obj payload = new Obj().put("clientId", clientId).put("seq", operation);
        if (value != null) {
            payload.put("value", value);
        }
        String json = Json.write(payload);
        RuntimeException last = null;
        while (System.nanoTime() < deadline) {
            try {
                Obj body = exchange(method, current + "/v1/kv/" + encode(key), json);
                if ("ok".equals(body.str("status"))) {
                    remember(body);
                    return;
                }
                follow(body);
            } catch (RuntimeException error) {
                last = error;
                advance();
            }
        }
        throw last == null ? new IllegalStateException(method + " timed out") : last;
    }

    private Obj exchange(String method, String url, String json) {
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofMillis(1_500));
            if ("GET".equals(method)) {
                builder.GET();
            } else {
                builder.header("content-type", "application/json");
                builder.method(method, HttpRequest.BodyPublishers.ofString(json));
            }
            HttpResponse<String> response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.body() == null || response.body().isBlank()) {
                throw new IllegalStateException("empty response from " + url);
            }
            return Json.parseObj(response.body());
        } catch (IllegalStateException error) {
            throw error;
        } catch (Exception error) {
            throw new IllegalStateException("request failed: " + url, error);
        }
    }

    private void follow(Obj body) {
        String leader = body.str("leaderId");
        if (leader != null && byId.containsKey(leader)) {
            current = byId.get(leader);
            return;
        }
        advance();
    }

    private void remember(Obj body) {
        String leader = body.str("leaderId");
        if (leader != null && byId.containsKey(leader)) {
            current = byId.get(leader);
        }
    }

    private void advance() {
        cursor = (cursor + 1) % urls.size();
        current = urls.get(cursor);
    }

    private static String encode(String key) {
        return URLEncoder.encode(key, StandardCharsets.UTF_8);
    }
}
