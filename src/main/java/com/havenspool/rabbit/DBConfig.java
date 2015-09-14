package com.havenspool.rabbit;

import java.util.Map;

/**
 * Created by havens on 15-8-12.
 */
public final class DBConfig {
    public final static String DEFAULT_KEY_FIELD = "system";
    public CacheConfig cacheConfig;
    public Class<?> afterCacheClass;
    public Map<String, DataSourceConf> dataSources;
}