package com.example.kafkarestorejob.restoreengine.zookeeper;

import com.example.kafkarestorejob.restoreengine.config.EngineZookeeperProperties;
import com.example.kafkarestorejob.restoreengine.kafka.RestoreExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

/**
 * Placeholder repository that would update ZooKeeper command-processor state after a successful
 * restore.
 */
@Repository
public class ZooKeeperCommandProcessorStateRepository {

    private static final Logger log = LoggerFactory.getLogger(ZooKeeperCommandProcessorStateRepository.class);

    private final EngineZookeeperProperties properties;

    /**
     * Creates the repository with the bound ZooKeeper properties.
     *
     * @param properties the ZooKeeper properties
     */
    public ZooKeeperCommandProcessorStateRepository(EngineZookeeperProperties properties) {
        this.properties = properties;
    }

    /**
     * Logs the command-processor state update that would occur after a restore completes.
     *
     * @param result the restore execution result
     */
    public void markRestoreCompleted(RestoreExecutionResult result) {
        log.info(
                "ZooKeeper state update placeholder after restore completion: connectString={} path={} key={} restoreType={}",
                properties.getConnectString(),
                properties.getCommandProcessorPath(),
                properties.getCommandProcessorKey(),
                result.restoreType()
        );
    }
}
