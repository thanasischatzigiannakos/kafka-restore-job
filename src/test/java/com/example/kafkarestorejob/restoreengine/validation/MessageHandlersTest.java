package com.example.kafkarestorejob.restoreengine.validation;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.example.kafkarestorejob.restoreengine.verification.S3FileExistenceVerifier;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class MessageHandlersTest {

    @Test
    void notificationHandlerAcceptsMatchingPayloadWithoutS3Validation() {
        NotificationMessageHandler handler = new NotificationMessageHandler();
        handler.validate(
                new ConsumerRecord<>(
                        "topic",
                        0,
                        1L,
                        "key",
                        "{\"entityType\":\"notification\",\"messageId\":\"1\"}".getBytes(StandardCharsets.UTF_8)
                ),
                context("notification")
        );
    }

    @Test
    void notificationHandlerRejectsWrongType() {
        NotificationMessageHandler handler = new NotificationMessageHandler();

        assertThrows(MessageTypeMismatchException.class, () -> handler.validate(
                new ConsumerRecord<>(
                        "topic",
                        0,
                        1L,
                        "key",
                        "{\"entityType\":\"application\"}".getBytes(StandardCharsets.UTF_8)
                ),
                context("notification")
        ));
    }

    @Test
    void notificationHandlerWrapsParsingFailures() {
        NotificationMessageHandler handler = new NotificationMessageHandler();

        RestorePayloadValidationException exception = assertThrows(
                RestorePayloadValidationException.class,
                () -> handler.validate(
                        new ConsumerRecord<>(
                                "topic",
                                0,
                                1L,
                                "key",
                                "{".getBytes(StandardCharsets.UTF_8)
                        ),
                        context("notification")
                )
        );

        org.junit.jupiter.api.Assertions.assertNotNull(exception.getCause());
    }

    @Test
    void applicationHandlerValidatesAllKnownBinaryReferences() {
        S3FileExistenceVerifier verifier = mock(S3FileExistenceVerifier.class);
        ApplicationMessageHandler handler = new ApplicationMessageHandler(verifier);

        handler.validate(
                new ConsumerRecord<>(
                        "topic",
                        0,
                        1L,
                        "key",
                        """
                        {
                          "entityType":"application",
                          "writtenDocument":{"objectKey":"written","checksum":"abc"},
                          "signedForm":{"objectKey":"signed","checksum":"def"},
                          "signedLocallyForm":{"objectKey":"local","checksum":"ghi"},
                          "translatedFiles":[{"objectKey":"translated-0","checksum":"jkl"}],
                          "applicant":{"uploadedFiles":[{"objectKey":"app-0","checksum":"mno"},{"objectKey":"app-1"}]}
                        }
                        """.getBytes(StandardCharsets.UTF_8)
                ),
                context("application")
        );

        verify(verifier, times(6)).verifyExists(any(), eq("application"), any());
    }

    @Test
    void applicationHandlerRejectsWrongTypeBeforeS3Validation() {
        S3FileExistenceVerifier verifier = mock(S3FileExistenceVerifier.class);
        ApplicationMessageHandler handler = new ApplicationMessageHandler(verifier);

        assertThrows(MessageTypeMismatchException.class, () -> handler.validate(
                new ConsumerRecord<>(
                        "topic",
                        0,
                        1L,
                        "key",
                        "{\"entityType\":\"notification\"}".getBytes(StandardCharsets.UTF_8)
                ),
                context("application")
        ));

        verifyNoInteractions(verifier);
    }

    @Test
    void applicationHandlerWrapsParsingFailures() {
        S3FileExistenceVerifier verifier = mock(S3FileExistenceVerifier.class);
        ApplicationMessageHandler handler = new ApplicationMessageHandler(verifier);

        RestorePayloadValidationException exception = assertThrows(
                RestorePayloadValidationException.class,
                () -> handler.validate(
                        new ConsumerRecord<>(
                                "topic",
                                0,
                                1L,
                                "key",
                                "{".getBytes(StandardCharsets.UTF_8)
                        ),
                        context("application")
                )
        );

        org.junit.jupiter.api.Assertions.assertNotNull(exception.getCause());
        verifyNoInteractions(verifier);
    }

    @Test
    void abuseHandlerSkipsBlankObjectKeys() {
        S3FileExistenceVerifier verifier = mock(S3FileExistenceVerifier.class);
        AbuseMessageHandler handler = new AbuseMessageHandler(verifier);

        handler.validate(
                new ConsumerRecord<>(
                        "topic",
                        0,
                        1L,
                        "key",
                        """
                        {
                          "messageId":"abuse-1",
                          "uploadedFile":{"objectKey":" "},
                          "attachments":[{"objectKey":"att-1","checksum":"sum-1"},{}]
                        }
                        """.getBytes(StandardCharsets.UTF_8)
                ),
                context("abuse")
        );

        verify(verifier, times(1)).verifyExists(any(), eq("abuse"), any());
        verifyNoMoreInteractions(verifier);
    }

    @Test
    void abuseHandlerWrapsParsingFailures() {
        S3FileExistenceVerifier verifier = mock(S3FileExistenceVerifier.class);
        AbuseMessageHandler handler = new AbuseMessageHandler(verifier);

        RestorePayloadValidationException exception = assertThrows(
                RestorePayloadValidationException.class,
                () -> handler.validate(
                        new ConsumerRecord<>(
                                "topic",
                                0,
                                1L,
                                "key",
                                "{".getBytes(StandardCharsets.UTF_8)
                        ),
                        context("abuse")
                )
        );

        org.junit.jupiter.api.Assertions.assertNotNull(exception.getCause());
        verifyNoInteractions(verifier);
    }

    private RestoreRecordValidationContext context(String restoreType) {
        return new RestoreRecordValidationContext(UUID.randomUUID(), restoreType, "topic", 0, 1L);
    }
}
