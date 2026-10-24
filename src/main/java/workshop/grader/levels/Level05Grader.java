package workshop.grader.levels;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;
import workshop.grader.grading.AttemptContext;
import workshop.grader.grading.LevelGrader;
import workshop.grader.grading.OutputReader;

/** Level 5: three consumers in one group, every record exactly once, offsets committed. */
@Component
class Level05Grader implements LevelGrader {

    private static final List<String> WORDS = List.of("resize image", "send email", "render pdf", "sync account",
            "index document", "charge card", "thumbnail video", "export report");

    @Override
    public int level() {
        return 5;
    }

    @Override
    public void grade(AttemptContext ctx) throws Exception {
        ctx.purge("in");
        ctx.awaitGroup(3, Duration.ofSeconds(60));
        try (OutputReader reader = ctx.readFromNow("out")) {
            Map<String, Integer> expected = new HashMap<>();
            round(ctx, reader, expected, 0, 600, "first");
            round(ctx, reader, expected, 600, 300, "second");
        }
    }

    private void round(AttemptContext ctx, OutputReader reader, Map<String, Integer> expected, int from, int count,
            String name) throws Exception {
        String out = ctx.topic("out");
        for (int i = from; i < from + count; i++) {
            String value = "job-%04d:%s".formatted(i, WORDS.get(ctx.random().nextInt(WORDS.size())));
            expected.merge(value.toUpperCase(Locale.ROOT), 1, Integer::sum);
            ctx.send("in", "job-%04d".formatted(i), value);
        }
        ctx.flush();
        ctx.pass("Sent the %s batch of %d jobs".formatted(name, count));

        int total = from + count;
        reader.pollUntil(r -> r.count(out) >= total, Duration.ofSeconds(30), Duration.ofSeconds(3));
        Map<String, Integer> actual = new HashMap<>();
        for (var record : reader.records(out)) {
            actual.merge(record.value(), 1, Integer::sum);
        }
        for (var entry : actual.entrySet()) {
            ctx.require(expected.containsKey(entry.getKey()),
                    "Unexpected value on %s: %s. Values should be the input in upper case.".formatted(out,
                            Json.shorten(entry.getKey())));
            ctx.require(entry.getValue() == 1,
                    "%s was processed %d times. Did a rebalance replay records you had not committed?"
                            .formatted(entry.getKey(), entry.getValue()));
        }
        ctx.check(actual.size() == total, "All %d jobs processed exactly once".formatted(total),
                "Only %d of %d jobs arrived on %s".formatted(actual.size(), total, out));

        Map<TopicPartition, Long> end = new HashMap<>(ctx.endOffsets("in"));
        end.values().removeIf(offset -> offset == 0);
        Instant deadline = Instant.now().plusSeconds(15);
        Map<TopicPartition, Long> committed = ctx.committedOffsets();
        while (!committedUpTo(committed, end) && Instant.now().isBefore(deadline)) {
            Thread.sleep(500);
            committed = ctx.committedOffsets();
        }
        Map<TopicPartition, Long> finalCommitted = committed;
        long behind = end.entrySet().stream()
                .mapToLong(e -> Math.max(0, e.getValue() - finalCommitted.getOrDefault(e.getKey(), 0L)))
                .sum();
        ctx.check(behind <= 0, "Committed offsets reached the end of every partition",
                "Your group's committed offsets are %d records behind. Commit after processing each batch."
                        .formatted(behind));
    }

    private static boolean committedUpTo(Map<TopicPartition, Long> committed, Map<TopicPartition, Long> end) {
        return end.entrySet().stream().allMatch(e -> committed.getOrDefault(e.getKey(), 0L) >= e.getValue());
    }
}
