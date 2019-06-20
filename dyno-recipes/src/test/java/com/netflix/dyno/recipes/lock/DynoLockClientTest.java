package com.netflix.dyno.recipes.lock;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import com.netflix.dyno.recipes.util.Tuple;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public abstract class DynoLockClientTest {

    Host host;
    TokenMapSupplierImpl tokenMapSupplier;
    DynoLockClient dynoLockClient;
    String resource = "testResource";

    @Test
    public void testExtendLockAndCheckResourceExists() {
        long v = dynoLockClient.acquireLock(resource, 500);
        Assert.assertTrue("Acquire lock did not succeed in time", v > 0);
        Assert.assertEquals(1, dynoLockClient.getLockedResources().size());
        Assert.assertTrue(dynoLockClient.checkResourceExists(resource));
        long ev = dynoLockClient.extendLock(resource, 1000);
        Assert.assertTrue("Extend lock did not extend the lock", ev > 500);
        dynoLockClient.releaseLock(resource);
        Assert.assertEquals(0, dynoLockClient.getLockedResources().size());
    }

    @Test
    public void testReleaseLock() {
        long v = dynoLockClient.acquireLock(resource, 100);
        Assert.assertTrue("Acquire lock did not succeed in time", v > 0);
        dynoLockClient.releaseLock(resource);
        v = dynoLockClient.checkLock(resource);
        Assert.assertTrue("Release lock failed",v == 0);
    }

    @Test
    public void testExtendLockFailsIfTooLate() throws InterruptedException {
        long v = dynoLockClient.acquireLock(resource, 100);
        Assert.assertTrue("Acquire lock did not succeed in time", v > 0);
        Assert.assertEquals(1, dynoLockClient.getLockedResources().size());
        Thread.sleep(100);
        long ev = dynoLockClient.extendLock(resource, 1000);
        Assert.assertTrue("Extend lock did not extend the lock", ev == 0);
        Assert.assertEquals(0, dynoLockClient.getLockedResources().size());
    }

    @Test
    public void testCheckLock() {
        long v = dynoLockClient.acquireLock(resource, 5000);
        Assert.assertTrue("Acquire lock did not succeed in time", v > 0);
        Assert.assertEquals(1, dynoLockClient.getLockedResources().size());
        v = dynoLockClient.checkLock(resource);
        Assert.assertTrue("Check lock failed for acquired lock",v > 0);
        dynoLockClient.releaseLock(resource);
        Assert.assertTrue("Check lock failed for acquired lock", dynoLockClient.checkLock(resource) == 0);
    }

    @Test
    public void testLockClient() {
        long v = dynoLockClient.acquireLock(resource, 1000);
        Assert.assertTrue("Acquire lock did not succeed in time", v > 0);
        Assert.assertEquals(1, dynoLockClient.getLockedResources().size());
        dynoLockClient.releaseLock(resource);
        Assert.assertEquals(0, dynoLockClient.getLockedResources().size());
    }

    @Test
    public void testLockClientConcurrent() {
        ScheduledExecutorService ses = Executors.newScheduledThreadPool(3);
        List<Long> ttls = Arrays.asList(new Long[]{100L, 50L, 25L});
        Collections.shuffle(ttls);
        ConcurrentLinkedDeque<Long> ttlQueue = new ConcurrentLinkedDeque<>(ttls);
        List<Long> resultList = Collections.synchronizedList(new ArrayList());
        Supplier<Tuple<Long, Long>> acquireLock = () -> {
            long ttl = ttlQueue.poll();
            long value = dynoLockClient.acquireLock(resource, ttl);
            resultList.add(value);
            return new Tuple<>(ttl, value);
        };
        IntStream.range(0, ttls.size()).mapToObj(i -> CompletableFuture.supplyAsync(acquireLock)
                .thenAccept(t -> Assert.assertTrue(t._2() < t._1()))).forEach(f -> {
            try {
                f.get();
            } catch (InterruptedException e) {
                Assert.fail("Interrupted during the test");
            } catch (ExecutionException e) {
                e.printStackTrace();
                Assert.fail();
            }
        });
        boolean lock = false;
        for(Long r: resultList) {
            if(r > 0) {
                if(lock) {
                    Assert.fail("Lock did not work as expected");
                }
                lock = true;
            }
        }
    }

    static class TokenMapSupplierImpl implements TokenMapSupplier {

        private final HostToken localHostToken;

        TokenMapSupplierImpl(Host host) {
            this.localHostToken = new HostToken(100000L, host);
        }

        @Override
        public List<HostToken> getTokens(Set<Host> activeHosts) {
            return Collections.singletonList(localHostToken);
        }

        @Override
        public HostToken getTokenForHost(Host host, Set<Host> activeHosts) {
            return localHostToken;
        }

    }
}