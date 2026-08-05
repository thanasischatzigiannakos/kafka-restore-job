package com.example.kafkarestorejob.restoreengine.validation;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Shared message-handler implementation that parses the payload once and validates it before any
 * target write occurs.
 *
 * @param <T> the parsed payload type
 */
public abstract class AbstractMessageHandler<T> implements MessageHandler {

    /**
     * Parses the payload once, checks its expected logical structure, and then runs binary
     * validation when the concrete handler supports binaries.
     *
     * @param sourceRecord the source record being validated
     * @param context the restore context used for diagnostics
     */
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

    /**
     * Indicates that plain handlers do not validate external binaries by default.
     *
     * @return {@code false}
     */
    @Override
    public boolean hasBinary() {
        return false;
    }

    /**
     * Parses the serialized payload into the handler's logical message type.
     *
     * @param payloadBytes the serialized payload bytes
     * @return the parsed payload
     */
    protected abstract T parse(byte[] payloadBytes);

    /**
     * Verifies that the parsed payload matches the handler's expected logical structure.
     *
     * @param payload the parsed payload
     * @return {@code true} when the payload matches the expected structure
     */
    protected abstract boolean validateStructure(T payload);

    /**
     * Performs binary validation for binary-capable handlers.
     *
     * @param sourceRecord the source record being validated
     * @param context the restore context used for diagnostics
     * @param payload the parsed payload
     */
    protected void validateBinary(
            ConsumerRecord<String, byte[]> sourceRecord,
            RestoreRecordValidationContext context,
            T payload
    ) {
        throw new UnsupportedOperationException("Binary validation is not supported");
    }

    /**
     * Returns a concise payload description used in mismatch diagnostics.
     *
     * @param payload the parsed payload
     * @return a short textual description of the actual payload
     */
    protected String payloadDescription(T payload) {
        return String.valueOf(payload);
    }

    /**
     * Wraps the original parsing failure in the restore-specific exception type.
     *
     * @param exception the original parsing exception
     * @return the wrapped parsing failure
     */
    protected final RestorePayloadValidationException parsingFailure(Exception exception) {
        return new RestorePayloadValidationException(
                "Failed to parse " + getType() + " payload",
                exception
        );
    }

    /**
     * Builds the type-mismatch exception thrown when a payload parses but does not match the
     * handler's expected logical structure.
     *
     * @param context the restore context used for diagnostics
     * @param actualDescription a short description of the actual payload
     * @return the mismatch exception
     */
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
