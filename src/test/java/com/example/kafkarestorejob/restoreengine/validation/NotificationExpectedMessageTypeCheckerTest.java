package com.example.kafkarestorejob.restoreengine.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.example.kafkarestorejob.restoreengine.serialization.NotificationRestoreMessageUnpacker;
import com.example.kafkarestorejob.restoreengine.serialization.model.NotificationRestoreMessage;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NotificationExpectedMessageTypeCheckerTest {

    @Mock
    private NotificationRestoreMessageUnpacker unpacker;

    private NotificationExpectedMessageTypeChecker checker;
    private RestoreRecordValidationContext context;

    @BeforeEach
    void setUp() {
        checker = new NotificationExpectedMessageTypeChecker(unpacker);
        context = new RestoreRecordValidationContext(
                UUID.randomUUID(),
                "notification",
                "notification-source-topic",
                0,
                15L
        );
    }

    @Test
    void acceptsExpectedNotificationEntityType() {
        NotificationRestoreMessage payload = new NotificationRestoreMessage();
        payload.setEntityType("notification");
        when(unpacker.unpack(any())).thenReturn(payload);

        assertDoesNotThrow(() -> checker.validate(context, "{}".getBytes()));
    }

    @Test
    void rejectsUnexpectedEntityType() {
        NotificationRestoreMessage payload = new NotificationRestoreMessage();
        payload.setEntityType("application");
        when(unpacker.unpack(any())).thenReturn(payload);

        assertThrows(
                RestorePayloadValidationException.class,
                () -> checker.validate(context, "{}".getBytes())
        );
    }
}
