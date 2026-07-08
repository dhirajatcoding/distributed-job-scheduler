package com.scheduler.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
@EnableAsync
@EnableScheduling
public class AsyncConfig {

    @Value("${worker.thread-pool-size:10}")
    private int threadPoolSize;

    @Value("${worker.thread-name-prefix:job-worker-}")
    private String threadNamePrefix;

    /**
     * Named "jobExecutorPool" — WorkerPoolService's @Async annotation references
     * this name explicitly. This ensures job execution threads are completely isolated
     * from Spring's default async executor used elsewhere in the application.
     *
     * Queue capacity = 0 means no internal queue. If all threads are busy when a
     * new job arrives, it is rejected immediately rather than silently queued.
     * The scheduler's next poll cycle will pick it up again.
     */
    @Bean(name = "jobExecutorPool")
    public Executor jobExecutorPool() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(threadPoolSize);
        executor.setMaxPoolSize(threadPoolSize);
        executor.setQueueCapacity(0);
        executor.setThreadNamePrefix(threadNamePrefix);
        executor.setRejectedExecutionHandler((runnable, pool) -> {
            // Log and drop — the scheduler will pick the job up in the next poll cycle
            System.err.println("[WORKER POOL] All threads busy. Job rejected — will retry on next poll.");
        });
        executor.initialize();
        return executor;
    }
}
