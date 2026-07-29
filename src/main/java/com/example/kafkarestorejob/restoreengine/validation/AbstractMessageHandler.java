package com.example.kafkarestorejob.restoreengine.validation;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public abstract class AbstractMessageHandler<T> implements MessageHandler {

    @Override
    public final void validate(
            ConsumerRecord<String, byte[]> sourceRecord,
            RestoreRecordValidationContext context
    ) {
        T payload = parse(sourceRecord.value());
        if (!validateStructure(payload)) {
            throw structureFailure(context, payloadDescription(payload));
        }
        if (hasBinary()) {
            validateBinary(sourceRecord, context, payload);
        }
    }

    @Override
    public boolean hasBinary() {
        return false;
    }

    protected abstract T parse(byte[] payloadBytes);

    protected abstract boolean validateStructure(T payload);

    protected void validateBinary(
            ConsumerRecord<String, byte[]> sourceRecord,
            RestoreRecordValidationContext context,
            T payload
    ) {
        throw new UnsupportedOperationException("Binary validation is not supported");
    }

    protected String payloadDescription(T payload) {
        return String.valueOf(payload);
    }

    protected final RestorePayloadValidationException parsingFailure(Exception exception) {
        return new RestorePayloadValidationException(
                "Failed to parse " + getType() + " payload",
                exception
        );
    }

    protected final MessageTypeMismatchException structureFailure(
            RestoreRecordValidationContext context,
            String actualDescription
    ) {
        return new MessageTypeMismatchException(
                "Unexpected payload structure for restoreType=" + context.restoreType()
                        + " sourceTopic=" + context.sourceTopic()
                        + " partition=" + context.partition()
                        + " offset=" + context.offset()
                        + " expectedMessageType=" + getType()
                        + " actual=" + actualDescription
        );
    }
}
