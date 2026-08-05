package com.example.kafkarestorejob.restoreengine.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Configures asynchronous execution for background restore jobs.
 */
@Configuration
@EnableAsync
public class AsyncConfiguration {

    @Bean(name = "restoreJobExecutor")
    /**
     * Creates the task executor used to run restore jobs asynchronously.
     *
     * @return the restore job task executor
     */
    public ThreadPoolTaskExecutor restoreJobExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setThreadNamePrefix("restore-job-");
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(4);
        executor.setQueueCapacity(20);
        executor.initialize();
        return executor;
    }
}
