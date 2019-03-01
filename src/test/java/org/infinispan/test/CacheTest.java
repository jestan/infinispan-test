package org.infinispan.test;

import org.infinispan.Cache;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.rocksdb.configuration.CompressionType;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.junit.Test;

import java.io.IOException;

import static java.io.File.separator;

public class CacheTest {

    private static final EmbeddedCacheManager CACHE_MANAGER;
    private static final TransactionManagerLookup TX_MANAGER_LOOKUP;

    static {
        System.setProperty("com.arjuna.ats.arjuna.common.propertiesFile", "jta.xml");
        System.setProperty("com.arjuna.ats.arjuna.objectstore.objectStoreDir", System.getProperty("java.io.tmpdir") + separator + "test_data" + separator + "transaction");
        System.setProperty("com.arjuna.ats.arjuna.objectstore.localOSRoot", "defaultStore");

        GlobalConfiguration globalCfg = new GlobalConfigurationBuilder()
                .globalState()
                .configurationStorage(ConfigurationStorage.OVERLAY)
                .persistentLocation(System.getProperty("java.io.tmpdir") + separator + "test_data" + separator + "config")
                .enable()
                .globalJmxStatistics()
                .enable()
                .build();

        DefaultCacheManager cacheMgr = new DefaultCacheManager(globalCfg);

        cacheMgr.defineConfiguration(
                "test_config",
                new ConfigurationBuilder()
                        .clustering()
                        .cacheMode(CacheMode.LOCAL)
                        .build()
        );

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                cacheMgr.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

        CACHE_MANAGER = cacheMgr;

        TX_MANAGER_LOOKUP = new JBossStandaloneJTAManagerLookup();
    }

    public static Cache<String, Long> getCache() {
        Cache<String, Long> counterCache;

        if (CACHE_MANAGER.cacheExists("counter_cache")) {
            counterCache = CACHE_MANAGER.getCache("counter_cache", false);
        } else {
            synchronized (CacheTest.class) {
                Configuration template = new ConfigurationBuilder()
                        .clustering()
                        .cacheMode(CacheMode.LOCAL)
                        .persistence()
                        .addStore(RocksDBStoreConfigurationBuilder.class)
                        .location(System.getProperty("java.io.tmpdir") + separator + "test_data" + separator + "active_")
                        .expiredLocation(System.getProperty("java.io.tmpdir") + separator + "test_data" + separator + "expired_")
                        .compressionType(CompressionType.NONE)
                        .blockSize(4096)
                        .cacheSize(33554432)
                        .maxBatchSize(1024)
                        .memory()
                        .storageType(StorageType.OBJECT)
                        .evictionType(EvictionType.COUNT)
                        .evictionStrategy(EvictionStrategy.REMOVE)
                        .size(1000_000)
                        .locking()
                        .isolationLevel(IsolationLevel.REPEATABLE_READ)
                        .transaction()
                        .lockingMode(LockingMode.OPTIMISTIC)
                        .transactionMode(TransactionMode.TRANSACTIONAL)
                        .transactionProtocol(TransactionProtocol.DEFAULT)
                        .completedTxTimeout(180_000)
                        .reaperWakeUpInterval(30_000)
                        .cacheStopTimeout(150_000)
                        .transactionManagerLookup(TX_MANAGER_LOOKUP)
                        .recovery()
                        .enabled(false)
                        .build();


                CACHE_MANAGER.defineConfiguration("counter_cache", template);

                counterCache = CACHE_MANAGER.getCache("counter_cache");
            }
        }

        return counterCache;
    }


    @Test
    public void incrementAndGetWith1() throws Exception {
        Cache<String, Long> cache = getCache();

        cache.putIfAbsent("user_counter", 0L);

        while (true) {
            Long userCount = cache.computeIfPresent("user_counter", (key, value) -> value + 1);

            System.out.println("userCount value = " + userCount);

            Thread.sleep(100);
        }
    }

    @Test
    public void incrementAndGetWith2() throws Exception {
        Cache<String, Long> cache = getCache();

        cache.putIfAbsent("user_counter", 0L);

        while (true) {

            Long userCount1 = cache.get("user_counter");

            cache.put("user_counter", userCount1 + 1);

            Long userCount2 = cache.get("user_counter");

            System.out.println("userCount value = " + userCount2);

            Thread.sleep(100);
        }
    }
}
