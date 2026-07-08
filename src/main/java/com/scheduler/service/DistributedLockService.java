package com.scheduler.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DistributedLockService {

    private final StringRedisTemplate redis;
    private final DefaultRedisScript<Long> releaseLockScript;

    /**
     * Attempts to acquire a distributed lock for the given key.
     *
     * Uses SET key value NX PX ttlMs — a single atomic Redis command.
     * NX = only set if key does Not eXist.
     * PX = TTL in milliseconds (auto-expires the lock if the holder crashes).
     *
     * The TTL is your safety net: if the holder crashes without releasing,
     * the lock expires automatically within `ttl` and other nodes can proceed.
     * Set TTL >= the maximum expected execution time of any job.
     *
     * @return true if the lock was acquired, false if another node holds it
     */
    public boolean acquire(String lockKey, String lockValue, Duration ttl) {
        Boolean acquired = redis.opsForValue()
                .setIfAbsent(lockKey, lockValue, ttl);

        if (Boolean.TRUE.equals(acquired)) {
            log.debug("[LOCK] Acquired: key={}, value={}", lockKey, lockValue);
            return true;
        }

        log.debug("[LOCK] Already held: key={}", lockKey);
        return false;
    }

    /**
     * Releases the lock ONLY if the current node still owns it.
     * Delegates to the Lua script for atomicity.
     *
     * Returns true if we successfully released our lock.
     * Returns false if the lock had already expired or was taken by another node —
     * this is not an error, just informational.
     */
    public boolean release(String lockKey, String lockValue) {
        Long result = redis.execute(
                releaseLockScript,
                List.of(lockKey),
                lockValue
        );

        boolean released = Long.valueOf(1L).equals(result);
        if (released) {
            log.debug("[LOCK] Released: key={}", lockKey);
        } else {
            log.warn("[LOCK] Could not release key={} — already expired or owned by another node", lockKey);
        }
        return released;
    }

    /**
     * Generates a unique lock value for one acquisition.
     * Embedding the nodeId makes it easy to inspect Redis and see which node holds a lock.
     * Format: "<nodeId>:<uuid>"
     */
    public static String generateLockValue(String nodeId) {
        return nodeId + ":" + UUID.randomUUID();
    }

    public boolean isLocked(String lockKey) {
        return redis.hasKey(lockKey);
    }
}
