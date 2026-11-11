package workshop.grader.levels;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;
import workshop.grader.grading.AttemptContext;
import workshop.grader.grading.LevelGrader;
import workshop.grader.grading.OutputReader;

/** Level 7: duplicates go in, each payment comes out exactly once. */
@Component
class Level07Grader implements LevelGrader {

    private static final List<String> MERCHANTS = List.of("coffee-corner", "book-nook", "bike-shop", "grocer",
            "cinema", "bakery", "pharmacy");

    @Override
    public int level() {
        return 7;
    }

    @Override
    public void grade(AttemptContext ctx) throws Exception {
        ctx.purge("in");
        ctx.awaitGroup(1, Duration.ofSeconds(45));
        String out = ctx.topic("out");
        try (OutputReader reader = ctx.readFromNow("out")) {
            int count = 300;
            Map<String, String> payments = new HashMap<>();
            List<String> later = new ArrayList<>();
            int duplicates = 0;
            for (int i = 0; i < count; i++) {
                String eventId = "p-%04d".formatted(i);
                String value = "{\"eventId\":\"%s\",\"merchant\":\"%s\",\"amount\":%d}".formatted(eventId,
                        MERCHANTS.get(ctx.random().nextInt(MERCHANTS.size())), 5 + ctx.random().nextInt(500));
                payments.put(eventId, value);
                ctx.send("in", eventId, value);
                if (ctx.random().nextInt(100) < 25) {
                    if (ctx.random().nextBoolean()) {
                        ctx.send("in", eventId, value);
                    } else {
                        later.add(eventId);
                    }
                    duplicates++;
                }
            }
            ctx.flush();
            ctx.pass("Sent %d payments, %d of them with a duplicate".formatted(count, duplicates));
            Thread.sleep(3000);
            for (String eventId : later) {
                ctx.send("in", eventId, payments.get(eventId));
            }
            ctx.flush();
            ctx.pass("Sent %d late duplicates".formatted(later.size()));

            reader.pollUntil(r -> r.count(out) >= count, Duration.ofSeconds(30), Duration.ofSeconds(3));
            Map<String, Integer> seen = new HashMap<>();
            for (var record : reader.records(out)) {
                String eventId = record.key();
                ctx.require(payments.containsKey(eventId) && payments.get(eventId).equals(record.value()),
                        "Unexpected record on %s with key %s: %s".formatted(out, eventId, Json.shorten(record.value())));
                ctx.require(seen.merge(eventId, 1, Integer::sum) == 1,
                        "Payment %s came out more than once".formatted(eventId));
            }
            ctx.check(seen.size() == count, "All %d payments came out exactly once".formatted(count),
                    "Only %d of %d payments came out on %s".formatted(seen.size(), count, out));
        }
    }
}
