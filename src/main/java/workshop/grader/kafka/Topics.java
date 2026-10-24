package workshop.grader.kafka;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.kafka.common.config.TopicConfig;

/**
 * Topic names and layouts. Keep in sync with {@code TopicSpec} in the template repo, which
 * its local tests use.
 */
public final class Topics {

    public static final String EVENTS = "workshop.events";
    public static final int LEVELS = 10;

    /** One topic of a level. {@code logAppendTime} makes the broker stamp records, so client clocks do not matter. */
    public record Spec(String name, int partitions, boolean compacted, boolean logAppendTime) {

        static Spec of(String name, int partitions) {
            return new Spec(name, partitions, false, false);
        }

        public Map<String, String> configs() {
            if (compacted) {
                return Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
            }
            if (logAppendTime) {
                return Map.of(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "LogAppendTime");
            }
            return Map.of();
        }
    }

    private Topics() {
    }

    public static List<Spec> forLevel(int level) {
        return switch (level) {
            case 1 -> List.of(new Spec("out", 1, false, true));
            case 2 -> List.of(Spec.of("in", 3), Spec.of("out", 1));
            case 3 -> List.of(Spec.of("in", 3), Spec.of("out", 6));
            case 4 -> List.of(Spec.of("in", 3), Spec.of("out", 3), Spec.of("dlq", 1));
            case 5 -> List.of(Spec.of("in", 6), Spec.of("out", 3));
            case 6 -> List.of(Spec.of("in", 6), Spec.of("out", 6));
            case 7 -> List.of(Spec.of("in", 3), Spec.of("out", 3));
            case 8 -> List.of(Spec.of("in", 3), Spec.of("checkout", 3), Spec.of("product", 3), Spec.of("other", 3));
            case 9 -> List.of(Spec.of("in", 1), Spec.of("out", 1));
            case 10 -> List.of(Spec.of("in", 3), new Spec("customers", 3, true, false), Spec.of("out", 3));
            default -> throw new IllegalArgumentException("No level " + level);
        };
    }

    public static List<Integer> levels() {
        return IntStream.rangeClosed(1, LEVELS).boxed().toList();
    }

    public static String prefix(String team) {
        return "team-" + team + ".";
    }

    public static String levelTopic(String team, int level, String name) {
        return "team-%s.level%02d.%s".formatted(team, level, name);
    }

    public static String groupId(String team, int level) {
        return "team-%s.level%02d".formatted(team, level);
    }

    public static String submitTopic(String team) {
        return "team-" + team + ".submit";
    }

    public static String principal(String team) {
        return "team-" + team;
    }
}
