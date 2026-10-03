package com.hft.discovery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.rest.PacedLimiter;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/** Публичные запросы подбора тикеров: таймауты, свой мягкий лимит на биржу, пауза после 429/418/403. */
final class Http {
    static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    private final String exchange;
    private final PacedLimiter limiter;

    Http(String exchange, double requestsPerSec) {
        this.exchange = exchange;
        this.limiter = new PacedLimiter(requestsPerSec, 60_000);
    }

    JsonNode get(String url) throws Exception {
        return send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(15)).header("Accept", "application/json").GET().build());
    }

    JsonNode post(String url, String body) throws Exception {
        return send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(15))
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build());
    }

    private JsonNode send(HttpRequest r) throws Exception {
        limiter.acquire();
        HttpResponse<String> resp = CLIENT.send(r, HttpResponse.BodyHandlers.ofString());
        int code = resp.statusCode();
        if (code == 429 || code == 418 || code == 403) {
            limiter.blockFor(120_000);
            throw new IllegalStateException(exchange + ": HTTP " + code + " — пауза 2 мин");
        }
        if (code / 100 != 2) throw new IllegalStateException(exchange + ": HTTP " + code + " " + abbreviate(resp.body()));
        return JSON.readTree(resp.body());
    }

    static String abbreviate(String s) { return s == null ? "" : s.length() > 200 ? s.substring(0, 200) + "…" : s; }
}
