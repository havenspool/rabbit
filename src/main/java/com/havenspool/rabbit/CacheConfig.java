package com.havenspool.rabbit;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by havens on 15-8-12.
 */
public class CacheConfig {
    public String cachePrefix;
    public long syncTime;
    public List<Host> hosts;
    public String cache;
    public String password;
    public boolean cluster;
    public boolean memory_cache;

    public static class Host {
        public String host;
        public int port;
    }

    public static final CacheConfig DEFAULT;

    static {
        DEFAULT = new CacheConfig();
        DEFAULT.cachePrefix = "c_";
        DEFAULT.syncTime = 900000; // 15min
        DEFAULT.cache = "default";
        DEFAULT.memory_cache = true;
        DEFAULT.cluster = false;
        DEFAULT.hosts = new ArrayList<Host>();
    }
}
