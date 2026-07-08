package com.example.kafkarestorejob.restoreengine.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

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

    public String getConnectString() {
        return connectString;
    }

    public void setConnectString(String connectString) {
        this.connectString = connectString;
    }

    public Duration getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(Duration sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(Duration connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public String getCommandProcessorPath() {
        return commandProcessorPath;
    }

    public void setCommandProcessorPath(String commandProcessorPath) {
        this.commandProcessorPath = commandProcessorPath;
    }

    public String getCommandProcessorKey() {
        return commandProcessorKey;
    }

    public void setCommandProcessorKey(String commandProcessorKey) {
        this.commandProcessorKey = commandProcessorKey;
    }
}
