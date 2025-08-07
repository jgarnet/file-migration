package org.example.lock;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.util.UUID;
import java.util.function.Supplier;

public class Lock {
    private final Supplier<Jedis> jedisSupplier;

    public Lock(Supplier<Jedis> jedisSupplier) {
        this.jedisSupplier = jedisSupplier;
    }

    public boolean acquireLock(String key, UUID lockId, int ttl) {
        try (Jedis jedis = this.jedisSupplier.get()) {
            SetParams params = new SetParams();
            params.nx();
            params.ex(ttl);
            String result = jedis.set(key, lockId.toString(), params);
            return "OK".equals(result);
        }
    }

    public void releaseLock(String key, UUID lockId) {
        try (Jedis jedis = this.jedisSupplier.get()) {
            // thread-safe deletion of lock
            String lua =
                    "if redis.call('get', KEYS[1]) == ARGV[1] then " +
                            "   return redis.call('del', KEYS[1]) " +
                            " else " +
                            "   return 0 " +
                            "end";
            jedis.eval(lua, 1, key, lockId.toString());
        }
    }

}