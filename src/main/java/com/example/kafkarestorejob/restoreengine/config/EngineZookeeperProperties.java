package com.example.kafkarestorejob.restoreengine.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Binds ZooKeeper settings used by the command-processor state repository.
 */
@Validated
@ConfigurationProperties(prefix = "engine.zookeeper")
public class EngineZookeeperProperties {

    @NotBlank
    private String connectString;

    @NotNull
    private Duration sessionTimeout;

    @NotNull
    private Duration connectionTimeout;

    @NotBlank
    private String commandProcessorPath;

    @NotBlank
    private String commandProcessorKey;

    /**
     * Returns the ZooKeeper connection string.
     *
     * @return the connection string
     */
    public String getConnectString() {
        return connectString;
    }

    /**
     * Sets the ZooKeeper connection string.
     *
     * @param connectString the connection string
     */
    public void setConnectString(String connectString) {
        this.connectString = connectString;
    }

    /**
     * Returns the ZooKeeper session timeout.
     *
     * @return the session timeout
     */
    public Duration getSessionTimeout() {
        return sessionTimeout;
    }

    /**
     * Sets the ZooKeeper session timeout.
     *
     * @param sessionTimeout the session timeout
     */
    public void setSessionTimeout(Duration sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    /**
     * Returns the ZooKeeper connection timeout.
     *
     * @return the connection timeout
     */
    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * Sets the ZooKeeper connection timeout.
     *
     * @param connectionTimeout the connection timeout
     */
    public void setConnectionTimeout(Duration connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    /**
     * Returns the ZooKeeper path that stores the command-processor state.
     *
     * @return the ZooKeeper path
     */
    public String getCommandProcessorPath() {
        return commandProcessorPath;
    }

    /**
     * Sets the ZooKeeper path that stores the command-processor state.
     *
     * @param commandProcessorPath the ZooKeeper path
     */
    public void setCommandProcessorPath(String commandProcessorPath) {
        this.commandProcessorPath = commandProcessorPath;
    }

    /**
     * Returns the key within the command-processor state payload that stores the desired value.
     *
     * @return the command-processor key
     */
    public String getCommandProcessorKey() {
        return commandProcessorKey;
    }

    /**
     * Sets the key within the command-processor state payload that stores the desired value.
     *
     * @param commandProcessorKey the command-processor key
     */
    public void setCommandProcessorKey(String commandProcessorKey) {
        this.commandProcessorKey = commandProcessorKey;
    }
}
