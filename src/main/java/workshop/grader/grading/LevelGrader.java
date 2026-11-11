package workshop.grader.grading;

/**
 * Grades one level. {@link #grade(AttemptContext)} writes input, reads the team's output and
 * calls {@link AttemptContext#check} for each rule. Returning normally means solved.
 */
public interface LevelGrader {

    int level();

    void grade(AttemptContext ctx) throws Exception;
}
