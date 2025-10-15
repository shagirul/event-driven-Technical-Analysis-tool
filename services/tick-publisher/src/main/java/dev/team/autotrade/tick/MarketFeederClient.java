package dev.team.autotrade.tick;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.protobuf.util.JsonFormat;
import com.upstox.marketdatafeederv3udapi.rpc.proto.MarketDataFeed;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;

public class MarketFeederClient {

    // --- CONFIG: hardcode your access token for quick testing ---
    private static final String ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI0RkNFSDYiLCJqdGkiOiI2OGVmNGFjMDk2MDkzMTY0NDZlYjFiY2MiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc2MDUxMjcwNCwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzYwNTY1NjAwfQ.6wtCB470FQ_cjbIzLYccJWe4rrzqISNfWcSHH-jDTZk";
    // Instrument keys to subscribe (exact casing/spaces matter)
    private static final String[] INSTRUMENT_KEYS = {
            "NSE_EQ|INE299N01021"

    };
    // Subscribe mode: ltpc | full | option_greeks | full_d30 (Plus)
    private static final String MODE = "ltpc";

    private static final String AUTHORIZE_V3 =
            "https://api.upstox.com/v3/feed/market-data-feed/authorize";

    private static final Gson GSON = new Gson();

    public static void main(String[] args) throws Exception {
        // Simple reconnect loop: re-authorize each time (authorized_redirect_uri is single-use)
        int backoff = 1;
        final int maxBackoff = 30;

        while (true) {
            try {
                URI wss = getAuthorizedWssUrl(ACCESS_TOKEN);
                runWebSocketOnce(wss); // blocks until closed
                // If we reach here, the socket closed normally; fall through to reconnect with backoff
            } catch (Exception e) {
                System.err.println("[WS] error: " + e.getMessage());
                e.printStackTrace(System.err);
            }
            System.out.println("[WS] reconnecting in " + backoff + "s …");
            Thread.sleep(backoff * 1000L);
            backoff = Math.min(backoff * 2, maxBackoff);
        }
    }

    /** Calls Authorize v3 and returns the single-use wss:// URL. */
    private static URI getAuthorizedWssUrl(String accessToken) throws Exception {
        HttpClient http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        HttpRequest req = HttpRequest.newBuilder(URI.create(AUTHORIZE_V3))
                .header("Accept", "application/json")
                .header("Authorization", "Bearer " + accessToken)
                .GET()
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IllegalStateException("Authorize V3 failed: HTTP " + resp.statusCode() + " body=" + resp.body());
        }
        WebSocketResponse body = GSON.fromJson(resp.body(), WebSocketResponse.class);
        String wss = null;
        if (body != null && body.data != null) {
            // tolerate both snake_case and camelCase
            wss = (body.data.authorized_redirect_uri != null)
                    ? body.data.authorized_redirect_uri
                    : body.data.authorizedRedirectUri;
        }
        if (wss == null || wss.isBlank()) {
            throw new IllegalStateException("Authorize V3 success but missing authorized_redirect_uri");
        }
        System.out.println("[AUTH] authorized WSS: " + wss);
        return URI.create(wss);
    }

    /** Opens a WS connection to the given URL and blocks until it closes. */
    private static void runWebSocketOnce(URI serverUri) throws Exception {
        WebSocketClient client = new WebSocketClient(serverUri) {
            @Override public void onOpen(ServerHandshake handshake) {
                System.out.println("[WS] OPEN");
                sendBinarySubscribe(this);
            }

            @Override public void onMessage(String message) {
                // Providers sometimes send human-readable acks/errors as text frames
                System.out.println("[WS][text] " + (message.length() > 600 ? message.substring(0, 600) + "…" : message));
            }

            @Override public void onMessage(ByteBuffer buf) {
                try {
                    byte[] arr = new byte[buf.remaining()];
                    buf.get(arr);
                    MarketDataFeed.FeedResponse fr = MarketDataFeed.FeedResponse.parseFrom(arr);
                    String json = JsonFormat.printer().print(fr);
                    System.out.println(json);
                } catch (Exception e) {
                    System.err.println("[WS] failed to parse protobuf: " + e);
                }
            }

            @Override public void onClose(int code, String reason, boolean remote) {
                System.out.printf("[WS] CLOSE code=%d remote=%s reason=%s%n", code, remote, reason);
            }

            @Override public void onError(Exception ex) {
                System.err.println("[WS] ERROR: " + ex);
            }
        };

        // TLS for wss://
        SSLContext ssl = SSLContext.getInstance("TLS");
        ssl.init(null, null, new SecureRandom());
        SSLSocketFactory factory = ssl.getSocketFactory();
        client.setSocketFactory(factory);

        // Optional: if you want the library to send pings, set a non-zero timeout
        // client.setConnectionLostTimeout(30);

        // Connect and block this thread until the socket is open/closed
        boolean opened = client.connectBlocking();
        if (!opened) throw new IllegalStateException("Unable to open WebSocket connection");
        // Wait while open
        while (client.isOpen()) {
            Thread.sleep(500);
        }
        // Return to caller so the outer loop can re-authorize and reconnect
    }

    /** Sends a JSON subscribe as **binary** (UTF-8 bytes). */
    private static void sendBinarySubscribe(WebSocketClient client) {
        JsonObject req = new JsonObject();
        req.addProperty("guid", UUID.randomUUID().toString().replace("-", ""));
        req.addProperty("method", "sub");

        JsonObject data = new JsonObject();
        data.addProperty("mode", MODE);

        JsonArray keys = new JsonArray();
        Arrays.stream(INSTRUMENT_KEYS).forEach(keys::add);
        data.add("instrumentKeys", keys);

        req.add("data", data);

        byte[] bin = req.toString().getBytes(StandardCharsets.UTF_8);
        System.out.println("[WS] SUB (binary): " + req);
        client.send(bin);
    }

    // Minimal DTO for authorize response
    private static class WebSocketResponse {
        String status;
        Data data;
        static class Data {
            String authorized_redirect_uri;
            String authorizedRedirectUri;
        }
    }
}
