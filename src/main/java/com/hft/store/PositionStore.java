package com.hft.store;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Позиции по бессрочным контрактам (перпам) одной биржи: объём со знаком (плюс — лонг, минус — шорт)
 * и средняя цена входа.
 *
 * Обновляется так же, как баланс: локально после каждого исполнения (чтобы следующий ордер
 * видел позицию сразу), из приватного потока биржи и периодической сверкой по REST.
 */
public final class PositionStore {

    /** Позиция символа: объём со знаком, средняя цена входа, время обновления. */
    public record Position(double qty, double entryPrice, long updatedMs) {
        /** Позиции нет. */
        public static final Position FLAT = new Position(0, 0, 0);
        /** Нет позиции (с точностью до пыли). */
        public boolean isFlat() { return Math.abs(qty) < 1e-12; }
        /** Стоимость позиции по цене price (без знака). */
        public double notional(double price) { return Math.abs(qty) * price; }
        /** Нереализованный результат по цене price. */
        public double unrealized(double price) { return isFlat() ? 0 : (price - entryPrice) * qty; }
    }

    /** Символ -> позиция. */
    private final Map<String, Position> positions = new ConcurrentHashMap<>();
    /** Реализованный результат с запуска (без комиссий, они в цене исполнения) и полученный funding. */
    private double realized, funding;

    /** Позиция символа (FLAT, если нет). */
    public Position get(String symbol) { return positions.getOrDefault(symbol, Position.FLAT); }

    /** Объём со знаком. */
    public double qty(String symbol) { return get(symbol).qty(); }

    /** Задать позицию целиком (из потока биржи или сверки). */
    public void set(String symbol, double qty, double entryPrice) {
        if (Math.abs(qty) < 1e-12) positions.remove(symbol);
        else positions.put(symbol, new Position(qty, entryPrice, System.currentTimeMillis()));
    }

    /**
     * Учесть исполнение: buy=true — покупка. Возвращает реализованный результат
     * (если сделка уменьшила или перевернула позицию), иначе 0.
     */
    public synchronized double apply(String symbol, boolean buy, double qty, double price) {
        if (qty <= 0 || price <= 0) return 0;
        Position p = get(symbol);
        double signed = buy ? qty : -qty;
        double q0 = p.qty(), q1 = q0 + signed;
        double pnl = 0;
        double entry;
        if (q0 == 0 || Math.signum(q0) == Math.signum(signed)) {
            entry = (Math.abs(q0) * p.entryPrice() + qty * price) / Math.abs(q1);   // добавили к позиции
        } else {
            double closed = Math.min(Math.abs(q0), qty);
            pnl = (price - p.entryPrice()) * closed * Math.signum(q0);
            entry = Math.abs(q1) < 1e-12 ? 0 : Math.signum(q1) == Math.signum(q0) ? p.entryPrice() : price;   // перевернулись — новая цена
        }
        set(symbol, q1, entry);
        realized += pnl;
        return pnl;
    }

    /**
     * Списание/начисление funding по позиции: лонг платит при положительной ставке, шорт получает.
     * Возвращает сумму для баланса (плюс — получили).
     */
    public synchronized double accrueFunding(String symbol, double rate, double markPrice) {
        double q = qty(symbol);
        if (q == 0 || !(markPrice > 0)) return 0;
        double payment = -q * markPrice * rate;
        funding += payment;
        return payment;
    }

    /** Все открытые позиции (копия, по символу). */
    public Map<String, Position> snapshot() { return new TreeMap<>(positions); }

    /** Есть открытые позиции. */
    public boolean any() { return !positions.isEmpty(); }

    /** Сводка для админки. */
    public synchronized Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("realized", realized);
        m.put("funding", funding);
        m.put("open", positions.size());
        return m;
    }
}
