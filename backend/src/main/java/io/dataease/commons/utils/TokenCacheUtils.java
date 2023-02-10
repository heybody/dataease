package io.dataease.commons.utils;

import io.dataease.listener.util.CacheUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

public class TokenCacheUtils {

    private static final String KEY = "sys_token_store";
    private static final String DELAY_KEY = "sys_token_store_delay";

    public static void add(String token, Long userId) {
        CacheUtils.put(KEY, token, userId, null, null);
    }

    public static void remove(String token) {
        CacheUtils.remove(KEY, token);
    }

    public static boolean validate(String token) {
        Object sys_token_store = CacheUtils.get(KEY, token);
        return ObjectUtils.isNotEmpty(sys_token_store) && StringUtils.isNotBlank(sys_token_store.toString());
    }

    public static boolean validate(String token, Long userId) {
        Object sys_token_store = CacheUtils.get(KEY, token);
        return ObjectUtils.isNotEmpty(sys_token_store) && StringUtils.isNotBlank(sys_token_store.toString()) && userId == Long.parseLong(sys_token_store.toString());
    }

    public static void addWithTtl(String token, Long userId) {
        CacheUtils.put(DELAY_KEY, token, userId, 3, 5);
    }

    public static boolean validateDelay(String token) {
        Object tokenObj = CacheUtils.get(DELAY_KEY, token);
        return ObjectUtils.isNotEmpty(tokenObj) && StringUtils.isNotBlank(tokenObj.toString());
    }
}
