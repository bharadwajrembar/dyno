package com.netflix.dyno.recipes.lock;

import com.netflix.discovery.EurekaClient;
import com.netflix.dyno.connectionpool.ConnectionPool;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.contrib.ArchaiusConnectionPoolConfiguration;
import com.netflix.dyno.contrib.DynoCPMonitor;
import com.netflix.dyno.contrib.DynoOPMonitor;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.dyno.jedis.DynoJedisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DynoLockClient {

    private static final Logger logger = LoggerFactory.getLogger(DynoJedisClient.class);

    private final String appName;
    private final String clusterName;
    private final TokenMapSupplier tokenMapSupplier;
    private final HostSupplier hostSupplier;
    private final ConnectionPoolConfiguration cpConfig;
    private final ConnectionPool pool;
    private final VotingHostsSelector votingHostsSelector;
    private final ExecutorService service;
    private final int quorum;
    private final double CLOCK_DRIFT = 0.01;
    private TimeUnit timeoutUnit;
    private long timeout;
    private final ConcurrentHashMap<String, String> resourceKeyMap = new ConcurrentHashMap<>();

    public DynoLockClient(String appName, String clusterName, ConnectionPool pool, HostSupplier hostSupplier, TokenMapSupplier tokenMapSupplier,
                          ConnectionPoolConfiguration cpConfig, VotingHostsSelector votingHostsSelector, long timeout, TimeUnit unit) {
        this.appName = appName;
        this.clusterName = clusterName;
        this.tokenMapSupplier = tokenMapSupplier;
        this.hostSupplier = hostSupplier;
        this.cpConfig = cpConfig;
        this.pool = pool;
        this.votingHostsSelector = votingHostsSelector;
        // Threads for locking and unlocking
        this.service = Executors.newFixedThreadPool(votingHostsSelector.getVotingSize() * 2);
        this.quorum = votingHostsSelector.getVotingSize() / 2 + 1;
        this.timeout = timeout;
        this.timeoutUnit = unit;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> cleanup()));
    }

    public void setTimeoutUnit(TimeUnit timeoutUnit) {
        this.timeoutUnit = timeoutUnit;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    private static String getRandomString() {
        return UUID.randomUUID().toString();
    }

    public List<String> getLockedResources() {
        return new ArrayList<>(resourceKeyMap.values());
    }

    public void releaseLock(String resource) {
        if(!checkResourceExists(resource)){
            logger.info("No lock held on {}", resource);
            return;
        }
        CountDownLatch latch = new CountDownLatch(votingHostsSelector.getVotingSize());
        votingHostsSelector.getVotingHosts().getEntireList().stream()
                .map(host -> new CheckAndRunHost(host, pool, "del", resource, resourceKeyMap.get(resource)))
                .forEach(ulH -> CompletableFuture.supplyAsync(ulH, service)
                        .thenAccept(result -> latch.countDown())
                );
        boolean latchValue = false;
        try {
            latchValue = latch.await(timeout, timeoutUnit);
        } catch (InterruptedException e) {
            logger.info("Interrupted while releasing the lock for resource {}", resource);
        }
        if(latchValue) {
            logger.info("Released lock on {}", resource);
        } else {
            logger.info("Timed out before we could release the lock");
        }
        resourceKeyMap.remove(resource);
    }

    private long runLockHost(String resource, long ttlMS, boolean extend) {
        long startTime = Instant.now().toEpochMilli();
        long drift = Math.round(ttlMS * CLOCK_DRIFT) + 2;
        LockResource lockResource = new LockResource(resource, ttlMS);
        CountDownLatch latch = new CountDownLatch(quorum);
        if(extend) {
            votingHostsSelector.getVotingHosts().getEntireList().stream()
                    .map(host -> new ExtendHost(host, pool, lockResource, latch, resourceKeyMap.get(resource)))
                    .forEach(lH -> CompletableFuture.supplyAsync(lH, service));
        } else {
            votingHostsSelector.getVotingHosts().getEntireList().stream()
                    .map(host -> new LockHost(host, pool, lockResource, latch, resourceKeyMap.get(resource)))
                    .forEach(lH -> CompletableFuture.supplyAsync(lH, service));
        }
        awaitLatch(latch, resource);
        long validity = 0L;
        if (lockResource.getLocked() >= quorum) {
            long timeElapsed = Instant.now().toEpochMilli() - startTime;
            validity = ttlMS - timeElapsed - drift;
        } else {
            releaseLock(resource);
        }
        return validity;
    }

    public long acquireLock(String resource, long ttlMS) {
        resourceKeyMap.putIfAbsent(resource, getRandomString());
        return runLockHost(resource, ttlMS, false);
    }

    boolean checkResourceExists(String resource) {
        if(!resourceKeyMap.containsKey(resource)) {
            logger.info("No lock held on {}", resource);
            return false;
        } else {
            return true;
        }
    }

    private boolean awaitLatch(CountDownLatch latch, String resource) {
        try {
            return latch.await(timeout, timeoutUnit);
        } catch (InterruptedException e) {
            logger.info("Interrupted while checking the lock for resource {}", resource);
            return false;
        }
    }

    public long checkLock(String resource) {
        if(!checkResourceExists(resource)) {
            return 0;
        } else {
            long startTime = Instant.now().toEpochMilli();
            CopyOnWriteArrayList<Long> resultTtls = new CopyOnWriteArrayList<>();
            CountDownLatch latch = new CountDownLatch(votingHostsSelector.getVotingSize());
            votingHostsSelector.getVotingHosts().getEntireList().stream()
                    .map(host -> new CheckAndRunHost(host, pool, "pttl", resource, resourceKeyMap.get(resource)))
                    .forEach(checkAndRunHost -> CompletableFuture.supplyAsync(checkAndRunHost, service)
                            .thenAccept(r -> {
                                String result = r.getResult().toString();
                                if(result.equals("0") || result.equals("-2")) {
                                    logger.info("Lock not present on host");
                                } else {
                                    resultTtls.add(Long.valueOf(result));
                                    latch.countDown();
                                }
                            })
                    );
            boolean latchValue = awaitLatch(latch, resource);
            if(latchValue) {
                long timeElapsed = Instant.now().toEpochMilli() - startTime;
                logger.info("Checked lock on {}", resource);
                return Collections.min(resultTtls) - timeElapsed;
            } else {
                logger.info("Timed out before we could check the lock");
                return 0;
            }
        }
    }

    public long extendLock(String resource, long ttlMS) {
        if(!checkResourceExists(resource)) {
            logger.info("Could not extend lock since its already released");
            return 0;
        } else {
            return runLockHost(resource, ttlMS, true);
        }
    }

    public void cleanup() {
        resourceKeyMap.keySet().stream().forEach(this::releaseLock);
    }

    public void logLocks() {
        resourceKeyMap.entrySet().stream()
                .forEach(e -> logger.info("Resource: {}, Key: {}", e.getKey(), e.getValue()));
    }

    public static class Builder {
        private String appName;
        private String clusterName;
        private TokenMapSupplier tokenMapSupplier;
        private HostSupplier hostSupplier;
        private ConnectionPoolConfigurationImpl cpConfig;
        private EurekaClient eurekaClient;
        private long timeout;
        private TimeUnit timeoutUnit;

        public Builder() {
        }

        public Builder withTimeout(long timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder withTimeoutUnit(TimeUnit unit) {
            this.timeoutUnit = unit;
            return this;
        }

        public Builder withEurekaClient(EurekaClient eurekaClient) {
            this.eurekaClient = eurekaClient;
            return this;
        }

        public Builder withApplicationName(String applicationName) {
            appName = applicationName;
            return this;
        }

        public Builder withDynomiteClusterName(String cluster) {
            clusterName = cluster;
            return this;
        }

        public Builder withHostSupplier(HostSupplier hSupplier) {
            hostSupplier = hSupplier;
            return this;
        }

        public Builder withTokenMapSupplier(TokenMapSupplier tokenMapSupplier) {
            this.tokenMapSupplier = tokenMapSupplier;
            return this;
        }

        public Builder withConnectionPoolConfiguration(ConnectionPoolConfigurationImpl cpConfig) {
            this.cpConfig = cpConfig;
            return this;
        }

        public DynoLockClient build() {
            assert (appName != null);
            assert (clusterName != null);

            if (cpConfig == null) {
                cpConfig = new ArchaiusConnectionPoolConfiguration(appName);
                logger.info("Dyno Client runtime properties: " + cpConfig.toString());
            }

            // We do not want to fallback to other azs which is the normal opertion for the connection pool
            cpConfig.setFallbackEnabled(false);
            cpConfig.setConnectToDatastore(true);

            return buildDynoLockClient();
        }

        private DynoLockClient buildDynoLockClient() {
            DynoOPMonitor opMonitor = new DynoOPMonitor(appName);
            ConnectionPoolMonitor cpMonitor = new DynoCPMonitor(appName);

            DynoJedisUtils.updateConnectionPoolConfig(cpConfig, hostSupplier, tokenMapSupplier, eurekaClient,
                    clusterName);
            if (tokenMapSupplier == null)
                tokenMapSupplier = cpConfig.getTokenSupplier();
            final ConnectionPool<Jedis> pool = DynoJedisUtils.createConnectionPool(appName, opMonitor, cpMonitor,
                    cpConfig, null);

            return new DynoLockClient(appName, clusterName, pool, hostSupplier, tokenMapSupplier, cpConfig,
                    new VotingHostsFromTokenRange(hostSupplier, tokenMapSupplier, cpConfig.getLockVotingSize()),
                    timeout, timeoutUnit);
        }
    }
}
