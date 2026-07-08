package com.scheduler.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

@Configuration
public class RedisConfig {

    @Bean
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory factory) {
        return new StringRedisTemplate(factory);
    }

    /**
     * Lua script for atomic lock release.
     *
     * The naive approach (GET then DEL) has a race condition:
     *   1. Thread A: GET → value matches, it's mine
     *   2. Lock TTL expires
     *   3. Thread B: acquires the lock (new value)
     *   4. Thread A: DEL → just deleted Thread B's lock ← catastrophic bug
     *
     * This Lua script runs atomically on the Redis server — no other command
     * can execute between the GET and DEL. Thread A either deletes its own lock
     * or returns 0 (lock already expired/taken by someone else).
     *
     * KEYS[1] = lock key
     * ARGV[1] = the lock value this node set when acquiring
     */
    @Bean
    public DefaultRedisScript<Long> releaseLockScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText("""
                if redis.call('get', KEYS[1]) == ARGV[1] then
                    return redis.call('del', KEYS[1])
                else
                    return 0
                end
                """);
        script.setResultType(Long.class);
        return script;
    }
}
