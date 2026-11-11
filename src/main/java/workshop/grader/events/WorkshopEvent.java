package workshop.grader.events;

import java.time.Instant;

/**
 * One post on the live feed, as JSON on {@code workshop.events}. The template's
 * {@code Submit} reads {@code type}, {@code attemptId} and {@code message}.
 */
public record WorkshopEvent(
        Type type,
        String team,
        Integer level,
        String attemptId,
        String message,
        Integer points,
        Instant at) {

    public enum Type {
        TEAM_JOINED,
        ATTEMPT_STARTED,
        CHECK_PASSED,
        CHECK_FAILED,
        SOLVED,
        FAILED,
        ANNOUNCEMENT
    }
}
