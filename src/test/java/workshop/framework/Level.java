package workshop.framework;

/**
 * One workshop level. Your class {@code workshop.levelNN.LevelNN} implements this.
 *
 * <p>{@link #run(Workshop)} is called once. For consumer and Streams levels, keep working
 * until {@link Workshop#isRunning()} returns false, then close your clients and return.
 */
public interface Level {

    void run(Workshop ws) throws Exception;
}
