package com.scheduler.service;

import com.scheduler.config.NodeIdentityService;
import com.scheduler.service.SchedulerMetricsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Slf4j
@Service
@RequiredArgsConstructor
public class LeaderElectionService {

    static final String LEADER_KEY = "scheduler:leader";

    private final StringRedisTemplate redis;
    private final NodeIdentityService nodeIdentity;
    private final SchedulerMetricsService metrics;   // Phase 4

    @Value("${leader.ttl-seconds:15}")
    private long leaderTtlSeconds;

    private volatile boolean currentlyLeader = false;

    /**
     * Runs every leader.refresh-interval-ms (default 5s).
     *
     * Two cases:
     *   - Not the leader: attempt SETNX. If it succeeds, we become the leader.
     *   - Currently the leader: extend the TTL (refresh lease). If the key is
     *     gone (we lost it somehow), step down gracefully.
     *
     * fixedDelayString ensures the next refresh starts only after the current
     * one completes — prevents pile-up if Redis is slow.
     */
    @Scheduled(fixedDelayString = "${leader.refresh-interval-ms:5000}")
    public void refreshLeadership() {
        String nodeId = nodeIdentity.getNodeId();

        if (currentlyLeader) {
            extendLease(nodeId);
        } else {
            attemptElection(nodeId);
        }
    }

    /**
     * SETNX: SET if Not eXists.
     * Only one node wins this across the entire cluster — Redis guarantees atomicity.
     * All others get false and remain followers.
     */
    private void attemptElection(String nodeId) {
        Boolean won = redis.opsForValue()
                .setIfAbsent(LEADER_KEY, nodeId, Duration.ofSeconds(leaderTtlSeconds));

        if (Boolean.TRUE.equals(won)) {
            currentlyLeader = true;
            metrics.recordLeaderElectionWon();   // Phase 4
            log.info("[LEADER] {} became the scheduler leader", nodeId);
        }
    }

    /**
     * Extends the TTL of the leader key so it doesn't expire while we're alive.
     * If the key is gone (split-brain, manual deletion, Redis restart), we step down
     * and let the next election cycle pick a new leader.
     */
    private void extendLease(String nodeId) {
        // Verify we still own the key before extending
        String currentLeader = redis.opsForValue().get(LEADER_KEY);

        if (nodeId.equals(currentLeader)) {
            redis.expire(LEADER_KEY, Duration.ofSeconds(leaderTtlSeconds));
            log.debug("[LEADER] {} refreshed leader lease", nodeId);
        } else {
            // We thought we were leader but the key is gone or held by someone else.
            // Step down immediately — better to have no leader momentarily than two.
            currentlyLeader = false;
            log.warn("[LEADER] {} lost leadership (current leader: {}). Stepping down.",
                    nodeId, currentLeader);
        }
    }

    /**
     * Called on every scheduler tick. Deliberately checks Redis, not just the
     * local flag — provides defense against split-brain where two nodes both
     * think they're leader due to a lag in the local flag update.
     */
    public boolean isLeader() {
        if (!currentlyLeader) return false;

        // Double-check against Redis on each tick
        String currentLeader = redis.opsForValue().get(LEADER_KEY);
        boolean confirmed = nodeIdentity.getNodeId().equals(currentLeader);

        if (!confirmed && currentlyLeader) {
            currentlyLeader = false;
            log.warn("[LEADER] Leadership check failed — stepping down");
        }

        return confirmed;
    }

    public String getCurrentLeader() {
        return redis.opsForValue().get(LEADER_KEY);
    }
}
