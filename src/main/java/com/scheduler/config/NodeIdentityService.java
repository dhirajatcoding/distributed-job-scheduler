package com.scheduler.config;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.UUID;

@Slf4j
@Component
public class NodeIdentityService {

    /**
     * Stable for the lifetime of this JVM process.
     * Format: "<hostname>-<8-char-uuid>" — readable in logs, unique across nodes.
     * Example: "scheduler-node-1-a3f2bc91"
     */
    @Getter
    private String nodeId;

    @PostConstruct
    public void init() {
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            hostname = "unknown-host";
        }
        // Short UUID suffix ensures uniqueness even if two containers share a hostname
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        this.nodeId = hostname + "-" + suffix;
        log.info("[NODE] Identity established: {}", nodeId);
    }
}
