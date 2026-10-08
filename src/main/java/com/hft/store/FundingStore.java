package com.hft.store;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Ставки финансирования (funding) перпов одной биржи: текущая ставка, время следующего списания,
 * интервал и mark-цена. Пишет опрос биржи, читают стратегия funding-арбитража и бумажный движок.
 */
public final class FundingStore {

    /**
     * Ставка символа.
     *
     * @param rate ставка за один период (0.0001 = 0.01%)
     * @param intervalHours длина периода, ч (8 у Binance/Bybit/OKX, 1 у Hyperliquid)
     * @param nextFundingMs когда следующее списание (0 — неизвестно)
     * @param markPrice mark-цена
     * @param updatedMs когда получено
     */
    public record Funding(double rate, double intervalHours, long nextFundingMs, double markPrice, long updatedMs) {
        /** Ставка, приведённая к 8 часам — чтобы сравнивать биржи с разным периодом. */
        public double ratePer8h() { return intervalHours > 0 ? rate * 8 / intervalHours : rate; }
        /** Годовая доходность ставки, % (3 × 365 восьмичасовых периодов). */
        public double aprPercent() { return ratePer8h() * 3 * 365 * 100; }
    }

    /** Символ -> ставка. */
    private final Map<String, Funding> rates = new ConcurrentHashMap<>();

    /** Записать ставку; возвращает прежнюю (или null). */
    public Funding put(String symbol, Funding f) { return rates.put(symbol, f); }

    /** Ставка символа или null. */
    public Funding get(String symbol) { return rates.get(symbol); }

    /** Все ставки (копия, по символу). */
    public Map<String, Funding> snapshot() { return new TreeMap<>(rates); }
}
