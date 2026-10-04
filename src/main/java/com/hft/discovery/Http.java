package com.hft.discovery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.rest.PacedLimiter;
import com.hft.rest.RateBudget;
import com.hft.rest.RateLimits;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Публичные запросы подбора тикеров: таймауты, свой мягкий лимит, пауза после 429/418/403.
 * Кроме своего лимита запрос берёт разрешение у общего бюджета биржи ({@link RateBudget}) —
 * подбор не может съесть лимит, нужный торговле.
 */
final class Http {
    static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    private final String exchange;
    private final PacedLimiter limiter;
    private final RateBudget budget;

    Http(String exchange, double requestsPerSec) {
        this.exchange = exchange;
        this.limiter = new PacedLimiter(requestsPerSec, 60_000);
        this.budget = RateBudget.of(exchange);
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
        budget.acquire(RateBudget.Kind.PUBLIC, RateLimits.weight(exchange, r.method(), r.uri().getPath(), r.uri().getRawQuery()), 60_000);
        HttpResponse<String> resp = CLIENT.send(r, HttpResponse.BodyHandlers.ofString());
        int code = resp.statusCode();
        budget.onResponse(code, resp.headers());
        if (code == 429 || code == 418 || code == 403) {
            limiter.blockFor(120_000);
            throw new IllegalStateException(exchange + ": HTTP " + code + " — пауза 2 мин");
        }
        if (code / 100 != 2) throw new IllegalStateException(exchange + ": HTTP " + code + " " + abbreviate(resp.body()));
        return JSON.readTree(resp.body());
    }

    static String abbreviate(String s) { return s == null ? "" : s.length() > 200 ? s.substring(0, 200) + "…" : s; }
}
