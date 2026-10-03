package com.hft.util;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Потокобезопасная карта, которая помнит не больше maxSize последних добавленных записей:
 * при переполнении вытесняется самая старая. Для состояний ордеров и подобного — без предела
 * такие карты росли бы всё время работы процесса.
 */
public final class BoundedMap {

    private BoundedMap() {}

    public static <K, V> Map<K, V> create(int maxSize) {
        return Collections.synchronizedMap(new LinkedHashMap<>(256, 0.75f, false) {
            @Override protected boolean removeEldestEntry(Map.Entry<K, V> e) { return size() > maxSize; }
        });
    }
}
