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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class MessageHandlersTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void handlersExposeExpectedTypesAndBinaryCapabilities() {
        ApplicationMessageHandler applicationHandler =
                new ApplicationMessageHandler(mock(S3FileExistenceVerifier.class));
        AbuseMessageHandler abuseHandler =
                new AbuseMessageHandler(mock(S3FileExistenceVerifier.class));
        NotificationMessageHandler notificationHandler =
                new NotificationMessageHandler();

        assertEquals("application", applicationHandler.getType());
        assertEquals("abuse", abuseHandler.getType());
        assertEquals("notification", notificationHandler.getType());
        assertTrue(applicationHandler.hasBinary());
        assertTrue(abuseHandler.hasBinary());
        assertFalse(notificationHandler.hasBinary());
    }

    @Test
    void notificationHandlerAcceptsMatchingPayloadWithoutS3Validation() throws Exception {
        NotificationMessageHandler handler = new NotificationMessageHandler();

        handler.validate(
                sourceRecord(jsonBytes(notificationMessage("notification"))),
                context("notification")
        );
    }

    @Test
    void notificationHandlerRejectsWrongType() throws Exception {
        NotificationMessageHandler handler = new NotificationMessageHandler();

        assertThrows(
                MessageTypeMismatchException.class,
                () -> handler.validate(
                        sourceRecord(jsonBytes(notificationMessage("application"))),
                        context("notification")
                )
        );
    }

    @Test
    void notificationHandlerWrapsParsingFailures() {
        NotificationMessageHandler handler = new NotificationMessageHandler();

        RestorePayloadValidationException exception = assertThrows(
                RestorePayloadValidationException.class,
                () -> handler.validate(
                        sourceRecord("{".getBytes(StandardCharsets.UTF_8)),
                        context("notification")
                )
        );

        assertNotNull(exception.getCause());
    }

    @Test
    void applicationHandlerValidatesAllKnownBinaryReferences() throws Exception {
        S3FileExistenceVerifier verifier = mock(S3FileExistenceVerifier.class);
        ApplicationMessageHandler handler = new ApplicationMessageHandler(verifier);

        handler.validate(
                sourceRecord(jsonBytes(applicationMessage())),
                context("application")
        );

        verify(verifier, times(6)).verifyExists(any(), eq("application"), any());
    }

    @Test
    void applicationHandlerRejectsWrongTypeBeforeS3Validation() throws Exception {
        S3FileExistenceVerifier verifier = mock(S3FileExistenceVerifier.class);
        ApplicationMessageHandler handler = new ApplicationMessageHandler(verifier);

        assertThrows(
                MessageTypeMismatchException.class,
                () -> handler.validate(
                        sourceRecord(jsonBytes(applicationMessage("notification"))),
                        context("application")
                )
        );

        verifyNoInteractions(verifier);
    }

    @Test
    void applicationHandlerWrapsParsingFailures() {
        S3FileExistenceVerifier verifier = mock(S3FileExistenceVerifier.class);
        ApplicationMessageHandler handler = new ApplicationMessageHandler(verifier);

        RestorePayloadValidationException exception = assertThrows(
                RestorePayloadValidationException.class,
                () -> handler.validate(
                        sourceRecord("{".getBytes(StandardCharsets.UTF_8)),
                        context("application")
                )
        );

        assertNotNull(exception.getCause());
        verifyNoInteractions(verifier);
    }

    @Test
    void applicationHandlerSkipsS3WhenNoKnownBinaryReferencesExist() throws Exception {
        S3FileExistenceVerifier verifier = mock(S3FileExistenceVerifier.class);
        ApplicationMessageHandler handler = new ApplicationMessageHandler(verifier);

        handler.validate(
                sourceRecord(jsonBytes(applicationMessageWithoutReferences())),
                context("application")
        );

        verifyNoInteractions(verifier);
    }

    @Test
    void abuseHandlerSkipsBlankObjectKeys() throws Exception {
        S3FileExistenceVerifier verifier = mock(S3FileExistenceVerifier.class);
        AbuseMessageHandler handler = new AbuseMessageHandler(verifier);

        handler.validate(
                sourceRecord(jsonBytes(abuseMessageWithOneReference())),
                context("abuse")
        );

        verify(verifier, times(1)).verifyExists(any(), eq("abuse"), any());
        verifyNoMoreInteractions(verifier);
    }

    @Test
    void abuseHandlerRejectsMissingMessageIdBeforeS3Validation() throws Exception {
        S3FileExistenceVerifier verifier = mock(S3FileExistenceVerifier.class);
        AbuseMessageHandler handler = new AbuseMessageHandler(verifier);

        assertThrows(
                MessageTypeMismatchException.class,
                () -> handler.validate(
                        sourceRecord(jsonBytes(abuseMessage(" "))),
                        context("abuse")
                )
        );

        verifyNoInteractions(verifier);
    }

    @Test
    void abuseHandlerSkipsS3WhenNoBinaryReferencesExist() throws Exception {
        S3FileExistenceVerifier verifier = mock(S3FileExistenceVerifier.class);
        AbuseMessageHandler handler = new AbuseMessageHandler(verifier);

        handler.validate(
                sourceRecord(jsonBytes(abuseMessageWithoutReferences())),
                context("abuse")
        );

        verifyNoInteractions(verifier);
    }

    @Test
    void abuseHandlerWrapsParsingFailures() {
        S3FileExistenceVerifier verifier = mock(S3FileExistenceVerifier.class);
        AbuseMessageHandler handler = new AbuseMessageHandler(verifier);

        RestorePayloadValidationException exception = assertThrows(
                RestorePayloadValidationException.class,
                () -> handler.validate(
                        sourceRecord("{".getBytes(StandardCharsets.UTF_8)),
                        context("abuse")
                )
        );

        assertNotNull(exception.getCause());
        verifyNoInteractions(verifier);
    }

    private ConsumerRecord<String, byte[]> sourceRecord(byte[] payloadBytes) {
        return new ConsumerRecord<>(
                "topic",
                0,
                1L,
                "key",
                payloadBytes
        );
    }

    private RestoreRecordValidationContext context(String restoreType) {
        return new RestoreRecordValidationContext(UUID.randomUUID(), restoreType, "topic", 0, 1L);
    }

    private byte[] jsonBytes(Object value) throws Exception {
        return OBJECT_MAPPER.writeValueAsBytes(value);
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
}
