package com.example.kafkarestorejob.restoreengine.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.example.kafkarestorejob.restoreengine.serialization.model.AbuseRestoreMessage;
import com.example.kafkarestorejob.restoreengine.serialization.model.ApplicationRestoreMessage;
import com.example.kafkarestorejob.restoreengine.serialization.model.NotificationRestoreMessage;
import com.example.kafkarestorejob.restoreengine.verification.S3FileExistenceVerifier;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class MessageHandlersTest {

    @Test
    void handlersExposeExpectedTypesAndBinaryCapabilities() {
        ApplicationMessageHandler applicationHandler =
                new TestApplicationMessageHandler(
                        mock(S3FileExistenceVerifier.class),
                        applicationMessage()
                );
        AbuseMessageHandler abuseHandler =
                new TestAbuseMessageHandler(
                        mock(S3FileExistenceVerifier.class),
                        abuseMessage("abuse-1")
                );
        NotificationMessageHandler notificationHandler =
                new TestNotificationMessageHandler(notificationMessage("notification"));

        assertEquals("application", applicationHandler.getType());
        assertEquals("abuse", abuseHandler.getType());
        assertEquals("notification", notificationHandler.getType());
        assertTrue(applicationHandler.hasBinary());
        assertTrue(abuseHandler.hasBinary());
        assertFalse(notificationHandler.hasBinary());
    }

    @Test
    void notificationHandlerAcceptsMatchingPayloadWithoutS3Validation() {
        NotificationMessageHandler handler =
                new TestNotificationMessageHandler(notificationMessage("notification"));

        handler.validate(sourceRecord(), context("notification"));
    }

    @Test
    void notificationHandlerRejectsWrongType() {
        NotificationMessageHandler handler =
                new TestNotificationMessageHandler(notificationMessage("application"));

        assertThrows(
                MessageTypeMismatchException.class,
                () -> handler.validate(sourceRecord(), context("notification"))
        );
    }

    @Test
    void notificationHandlerWrapsParsingFailures() {
        NotificationMessageHandler handler =
                new TestNotificationMessageHandler(new IllegalArgumentException("parse failed"));

        RestorePayloadValidationException exception = assertThrows(
                RestorePayloadValidationException.class,
                () -> handler.validate(sourceRecord(), context("notification"))
        );

        assertNotNull(exception.getCause());
    }

    @Test
    void applicationHandlerValidatesAllKnownBinaryReferences() {
        S3FileExistenceVerifier verifier = mock(S3FileExistenceVerifier.class);
        ApplicationMessageHandler handler =
                new TestApplicationMessageHandler(verifier, applicationMessage());

        handler.validate(sourceRecord(), context("application"));

        verify(verifier, times(6)).verifyExists(any(), eq("application"), any());
    }

    @Test
    void applicationHandlerRejectsWrongTypeBeforeS3Validation() {
        S3FileExistenceVerifier verifier = mock(S3FileExistenceVerifier.class);
        ApplicationMessageHandler handler =
                new TestApplicationMessageHandler(verifier, applicationMessage("notification"));

        assertThrows(
                MessageTypeMismatchException.class,
                () -> handler.validate(sourceRecord(), context("application"))
        );

        verifyNoInteractions(verifier);
    }

    @Test
    void applicationHandlerWrapsParsingFailures() {
        S3FileExistenceVerifier verifier = mock(S3FileExistenceVerifier.class);
        ApplicationMessageHandler handler =
                new TestApplicationMessageHandler(verifier, new IllegalArgumentException("parse failed"));

        RestorePayloadValidationException exception = assertThrows(
                RestorePayloadValidationException.class,
                () -> handler.validate(sourceRecord(), context("application"))
        );

        assertNotNull(exception.getCause());
        verifyNoInteractions(verifier);
    }

    @Test
    void applicationHandlerSkipsS3WhenNoKnownBinaryReferencesExist() {
        S3FileExistenceVerifier verifier = mock(S3FileExistenceVerifier.class);
        ApplicationMessageHandler handler =
                new TestApplicationMessageHandler(verifier, applicationMessageWithoutReferences());

        handler.validate(sourceRecord(), context("application"));

        verifyNoInteractions(verifier);
    }

    @Test
    void abuseHandlerSkipsBlankObjectKeys() {
        S3FileExistenceVerifier verifier = mock(S3FileExistenceVerifier.class);
        AbuseMessageHandler handler =
                new TestAbuseMessageHandler(verifier, abuseMessageWithOneReference());

        handler.validate(sourceRecord(), context("abuse"));

        verify(verifier, times(1)).verifyExists(any(), eq("abuse"), any());
        verifyNoMoreInteractions(verifier);
    }

    @Test
    void abuseHandlerRejectsMissingMessageIdBeforeS3Validation() {
        S3FileExistenceVerifier verifier = mock(S3FileExistenceVerifier.class);
        AbuseMessageHandler handler =
                new TestAbuseMessageHandler(verifier, abuseMessage(" "));

        assertThrows(
                MessageTypeMismatchException.class,
                () -> handler.validate(sourceRecord(), context("abuse"))
        );

        verifyNoInteractions(verifier);
    }

    @Test
    void abuseHandlerSkipsS3WhenNoBinaryReferencesExist() {
        S3FileExistenceVerifier verifier = mock(S3FileExistenceVerifier.class);
        AbuseMessageHandler handler =
                new TestAbuseMessageHandler(verifier, abuseMessageWithoutReferences());

        handler.validate(sourceRecord(), context("abuse"));

        verifyNoInteractions(verifier);
    }

    @Test
    void abuseHandlerWrapsParsingFailures() {
        S3FileExistenceVerifier verifier = mock(S3FileExistenceVerifier.class);
        AbuseMessageHandler handler =
                new TestAbuseMessageHandler(verifier, new IllegalArgumentException("parse failed"));

        RestorePayloadValidationException exception = assertThrows(
                RestorePayloadValidationException.class,
                () -> handler.validate(sourceRecord(), context("abuse"))
        );

        assertNotNull(exception.getCause());
        verifyNoInteractions(verifier);
    }

    private ConsumerRecord<String, byte[]> sourceRecord() {
        return new ConsumerRecord<>(
                "topic",
                0,
                1L,
                "key",
                "unused".getBytes(StandardCharsets.UTF_8)
        );
    }

    private RestoreRecordValidationContext context(String restoreType) {
        return new RestoreRecordValidationContext(UUID.randomUUID(), restoreType, "topic", 0, 1L);
    }

    private NotificationRestoreMessage notificationMessage(String entityType) {
        NotificationRestoreMessage message = new NotificationRestoreMessage();
        message.setEntityType(entityType);
        message.setMessageId("notification-1");
        return message;
    }

    private ApplicationRestoreMessage applicationMessage() {
        return applicationMessage("application");
    }

    private ApplicationRestoreMessage applicationMessage(String entityType) {
        ApplicationRestoreMessage message = new ApplicationRestoreMessage();
        message.setEntityType(entityType);

        ApplicationRestoreMessage.WrittenDocument writtenDocument =
                new ApplicationRestoreMessage.WrittenDocument();
        writtenDocument.setObjectKey("written");
        message.setWrittenDocument(writtenDocument);

        ApplicationRestoreMessage.SignedForm signedForm =
                new ApplicationRestoreMessage.SignedForm();
        signedForm.setObjectKey("signed");
        message.setSignedForm(signedForm);

        ApplicationRestoreMessage.SignedLocallyForm signedLocallyForm =
                new ApplicationRestoreMessage.SignedLocallyForm();
        signedLocallyForm.setObjectKey("local");
        message.setSignedLocallyForm(signedLocallyForm);

        ApplicationRestoreMessage.TranslatedFile translatedFile =
                new ApplicationRestoreMessage.TranslatedFile();
        translatedFile.setObjectKey("translated-0");
        message.setTranslatedFiles(List.of(translatedFile));

        ApplicationRestoreMessage.Applicant applicant = new ApplicationRestoreMessage.Applicant();
        ApplicationRestoreMessage.ApplicantUploadedFile uploadedFileZero =
                new ApplicationRestoreMessage.ApplicantUploadedFile();
        uploadedFileZero.setObjectKey("app-0");
        ApplicationRestoreMessage.ApplicantUploadedFile uploadedFileOne =
                new ApplicationRestoreMessage.ApplicantUploadedFile();
        uploadedFileOne.setObjectKey("app-1");
        applicant.setUploadedFiles(List.of(uploadedFileZero, uploadedFileOne));
        message.setApplicant(applicant);

        return message;
    }

    private ApplicationRestoreMessage applicationMessageWithoutReferences() {
        ApplicationRestoreMessage message = new ApplicationRestoreMessage();
        message.setEntityType("application");

        ApplicationRestoreMessage.WrittenDocument writtenDocument =
                new ApplicationRestoreMessage.WrittenDocument();
        writtenDocument.setObjectKey(" ");
        message.setWrittenDocument(writtenDocument);

        ApplicationRestoreMessage.TranslatedFile translatedFile =
                new ApplicationRestoreMessage.TranslatedFile();
        ApplicationRestoreMessage.Applicant applicant = new ApplicationRestoreMessage.Applicant();
        ApplicationRestoreMessage.ApplicantUploadedFile uploadedFile =
                new ApplicationRestoreMessage.ApplicantUploadedFile();
        uploadedFile.setObjectKey("");
        applicant.setUploadedFiles(List.of(uploadedFile));
        message.setApplicant(applicant);
        message.setTranslatedFiles(List.of(translatedFile));
        return message;
    }

    private AbuseRestoreMessage abuseMessage(String messageId) {
        AbuseRestoreMessage message = new AbuseRestoreMessage();
        message.setMessageId(messageId);
        return message;
    }

    private AbuseRestoreMessage abuseMessageWithOneReference() {
        AbuseRestoreMessage message = abuseMessage("abuse-1");

        AbuseRestoreMessage.UploadedFile uploadedFile = new AbuseRestoreMessage.UploadedFile();
        uploadedFile.setObjectKey(" ");
        message.setUploadedFile(uploadedFile);

        AbuseRestoreMessage.UploadedFile attachment = new AbuseRestoreMessage.UploadedFile();
        attachment.setObjectKey("att-1");
        AbuseRestoreMessage.UploadedFile emptyAttachment = new AbuseRestoreMessage.UploadedFile();
        message.setAttachments(List.of(attachment, emptyAttachment));
        return message;
    }

    private AbuseRestoreMessage abuseMessageWithoutReferences() {
        AbuseRestoreMessage message = abuseMessage("abuse-1");

        AbuseRestoreMessage.UploadedFile uploadedFile = new AbuseRestoreMessage.UploadedFile();
        uploadedFile.setObjectKey(" ");
        message.setUploadedFile(uploadedFile);

        AbuseRestoreMessage.UploadedFile emptyAttachment = new AbuseRestoreMessage.UploadedFile();
        emptyAttachment.setObjectKey("   ");
        message.setAttachments(List.of(new AbuseRestoreMessage.UploadedFile(), emptyAttachment));
        return message;
    }

    private static final class TestNotificationMessageHandler extends NotificationMessageHandler {

        private final NotificationRestoreMessage parsed;
        private final RuntimeException failure;

        private TestNotificationMessageHandler(NotificationRestoreMessage parsed) {
            this.parsed = parsed;
            this.failure = null;
        }

        private TestNotificationMessageHandler(RuntimeException failure) {
            this.parsed = null;
            this.failure = failure;
        }

        @Override
        protected NotificationRestoreMessage parse(byte[] payloadBytes) {
            if (failure != null) {
                throw parsingFailure(failure);
            }
            return parsed;
        }
    }

    private static final class TestApplicationMessageHandler extends ApplicationMessageHandler {

        private final ApplicationRestoreMessage parsed;
        private final RuntimeException failure;

        private TestApplicationMessageHandler(
                S3FileExistenceVerifier verifier,
                ApplicationRestoreMessage parsed
        ) {
            super(verifier);
            this.parsed = parsed;
            this.failure = null;
        }

        private TestApplicationMessageHandler(
                S3FileExistenceVerifier verifier,
                RuntimeException failure
        ) {
            super(verifier);
            this.parsed = null;
            this.failure = failure;
        }

        @Override
        protected ApplicationRestoreMessage parse(byte[] payloadBytes) {
            if (failure != null) {
                throw parsingFailure(failure);
            }
            return parsed;
        }
    }

    private static final class TestAbuseMessageHandler extends AbuseMessageHandler {

        private final AbuseRestoreMessage parsed;
        private final RuntimeException failure;

        private TestAbuseMessageHandler(
                S3FileExistenceVerifier verifier,
                AbuseRestoreMessage parsed
        ) {
            super(verifier);
            this.parsed = parsed;
            this.failure = null;
        }

        private TestAbuseMessageHandler(
                S3FileExistenceVerifier verifier,
                RuntimeException failure
        ) {
            super(verifier);
            this.parsed = null;
            this.failure = failure;
        }

        @Override
        protected AbuseRestoreMessage parse(byte[] payloadBytes) {
            if (failure != null) {
                throw parsingFailure(failure);
            }
            return parsed;
        }
    }
}
