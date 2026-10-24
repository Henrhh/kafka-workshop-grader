package workshop.framework;

/** Finds the {@code workshop.levelNN.LevelNN} class for a level number. */
public final class Levels {

    public static final int COUNT = 10;

    private Levels() {
    }

    public static int parse(String arg) {
        if (arg == null || arg.isBlank()) {
            throw new IllegalArgumentException("Say which level, for example: ./gradlew submit -Plevel=1");
        }
        int level;
        try {
            level = Integer.parseInt(arg.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Level must be a number, but was: " + arg);
        }
        if (level < 1 || level > COUNT) {
            throw new IllegalArgumentException("Level must be between 1 and " + COUNT + ", but was: " + level);
        }
        return level;
    }

    public static Level create(int level) {
        String className = "workshop.level%02d.Level%02d".formatted(level, level);
        try {
            return (Level) Class.forName(className).getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Could not create " + className, e);
        }
    }
}
