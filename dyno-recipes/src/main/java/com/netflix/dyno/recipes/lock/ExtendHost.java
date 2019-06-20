package com.netflix.dyno.recipes.lock;

import com.netflix.dyno.connectionpool.ConnectionContext;
import com.netflix.dyno.connectionpool.ConnectionPool;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.jedis.OpName;
import com.netflix.dyno.jedis.operation.BaseKeyOperation;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.util.concurrent.CountDownLatch;

public class ExtendHost extends CommandHost<LockResource> {

    private static final String cmdScript = " if redis.call(\"get\",KEYS[1]) == ARGV[1] then\n" +
            "        return redis.call(\"set\",KEYS[1], ARGV[1], \"px\", ARGV[2])" +
            "    else\n" +
            "        return 0\n" +
            "    end";
    private final LockResource lockResource;
    private final String value;
    private final SetParams params;
    private final String randomKey;
    private final CountDownLatch latch;

    public ExtendHost(Host host, ConnectionPool pool, LockResource lockResource, CountDownLatch latch, String randomKey) {
        super(host, pool);
        this.lockResource = lockResource;
        this.value = lockResource.getResource();
        this.params = SetParams.setParams().px(lockResource.getTtlMs());
        this.randomKey = randomKey;
        this.latch = latch;
    }

    @Override
    public OperationResult<LockResource> get() {
        return getConnection().execute(new BaseKeyOperation<Object>(randomKey, OpName.EVAL) {
            @Override
            public LockResource execute(Jedis client, ConnectionContext state) {
                if(randomKey == null) {
                    throw new IllegalStateException("Cannot extend lock with null value for key");
                }
                String result = client.eval(cmdScript, 1, value, randomKey, String.valueOf(lockResource.getTtlMs()))
                        .toString();
                if(result.equals("OK")) {
                    lockResource.incrementLocked();
                    latch.countDown();
                }
                return lockResource;
            }
        });
    }
}

