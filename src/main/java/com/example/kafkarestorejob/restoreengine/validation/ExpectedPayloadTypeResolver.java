package com.example.kafkarestorejob.restoreengine.validation;

public interface ExpectedPayloadTypeResolver {

    Class<?> resolve(String configuredMessageType);
}
