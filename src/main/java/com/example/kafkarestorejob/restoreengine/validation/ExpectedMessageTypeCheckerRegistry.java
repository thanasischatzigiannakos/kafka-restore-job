package com.example.kafkarestorejob.restoreengine.validation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public class ExpectedMessageTypeCheckerRegistry {

    private final Map<String, ExpectedMessageTypeChecker> checkersByMessageType;

    public ExpectedMessageTypeCheckerRegistry(List<ExpectedMessageTypeChecker> checkers) {
        Map<String, ExpectedMessageTypeChecker> registry = new LinkedHashMap<>();
        for (ExpectedMessageTypeChecker checker : checkers) {
            ExpectedMessageTypeChecker previous = registry.put(checker.messageType(), checker);
            if (previous != null) {
                throw new IllegalArgumentException(
                        "Duplicate message type checker configured for message type: "
                                + checker.messageType()
                );
            }
        }
        this.checkersByMessageType = Map.copyOf(registry);
    }

    public ExpectedMessageTypeChecker requireChecker(String messageType) {
        ExpectedMessageTypeChecker checker = checkersByMessageType.get(messageType);
        if (checker == null) {
            throw new IllegalArgumentException(
                    "No message type checker configured for message type: " + messageType
            );
        }
        return checker;
    }

    public Optional<ExpectedMessageTypeChecker> findChecker(String messageType) {
        return Optional.ofNullable(checkersByMessageType.get(messageType));
    }
}
