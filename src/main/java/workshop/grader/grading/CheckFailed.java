package workshop.grader.grading;

/** Thrown when a check fails. Its message is shown to the team and on the feed. */
public class CheckFailed extends RuntimeException {

    public CheckFailed(String message) {
        super(message);
    }
}
