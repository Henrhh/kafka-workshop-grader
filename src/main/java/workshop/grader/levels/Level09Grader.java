package workshop.grader.levels;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import workshop.grader.grading.AttemptContext;
import workshop.grader.grading.LevelGrader;
import workshop.grader.grading.OutputReader;
import workshop.grader.grading.OutputReader.Received;

/** Level 9: counts per user in 1 minute event-time windows with a 30 second grace period. */
@Component
class Level09Grader implements LevelGrader {

    static final long WINDOW = 60_000;
    static final long GRACE = 30_000;
    /** Late events land at least this far from the grace boundary, so clock details cannot flip the result. */
    private static final long MARGIN = 3_000;
    private static final DateTimeFormatter CLOCK = DateTimeFormatter.ofPattern("HH:mm").withZone(ZoneOffset.UTC);

    record Action(String userId, long timestamp) {
    }

    /** The actions to send, in order, and what a correct application ends up counting. */
    record Plan(List<Action> actions, Map<String, Long> counts, int lateCounted, int dropped) {
    }

    @Override
    public int level() {
        return 9;
    }

    @Override
    public void grade(AttemptContext ctx) throws Exception {
        ctx.purge("in");
        ctx.purgeInternalTopics();
        ctx.awaitGroup(1, Duration.ofSeconds(60));
        String out = ctx.topic("out");
        try (OutputReader reader = ctx.readFromNow("out")) {
            long start = (System.currentTimeMillis() / WINDOW - 15) * WINDOW;
            Plan plan = plan(ctx.random(), start);
            for (Action action : plan.actions()) {
                ctx.send("in", action.userId(), "{\"userId\":\"%s\",\"action\":\"click\"}".formatted(action.userId()),
                        action.timestamp());
            }
            ctx.flush();
            ctx.pass("Sent %d actions over 12 minutes of event time, %d late but within grace and %d too late"
                    .formatted(plan.actions().size(), plan.lateCounted(), plan.dropped()));

            reader.pollUntil(r -> lastCounts(ctx, r.records(out)).equals(plan.counts()), Duration.ofSeconds(45),
                    Duration.ofSeconds(2));
            Map<String, Long> actual = lastCounts(ctx, reader.records(out));
            ctx.check(!actual.isEmpty(), "Received counts on " + out, "No counts arrived on " + out);
            ctx.require(!actual.containsKey(null),
                    "Some records on %s are not like {\"userId\":...,\"windowStart\":...,\"count\":...}".formatted(out));
            for (var entry : plan.counts().entrySet()) {
                Long got = actual.get(entry.getKey());
                String where = describe(entry.getKey());
                ctx.require(got != null, "No count for " + where);
                ctx.require(got <= entry.getValue(), "%s: you counted %d, expected %d. Too-late events must be dropped."
                        .formatted(where, got, entry.getValue()));
                ctx.require(got >= entry.getValue(), "%s: you counted %d, expected %d. Are late events within the grace period counted?"
                        .formatted(where, got, entry.getValue()));
            }
            for (String key : actual.keySet()) {
                ctx.require(plan.counts().containsKey(key),
                        "There should be no count for %s: it only had events that arrived too late".formatted(describe(key)));
            }
            ctx.pass("All %d window counts are right".formatted(plan.counts().size()));
            ctx.pass("Late events within grace were counted, too-late ones dropped");
        }
    }

    /**
     * Builds the actions and computes the counts the way Kafka Streams does: a record is
     * added to its window while {@code windowEnd > streamTime - grace}, where stream time is
     * the highest timestamp seen so far, including the record itself.
     */
    static Plan plan(Random random, long start) {
        List<Action> actions = new ArrayList<>();
        Map<String, Long> counts = new HashMap<>();
        long streamTime = Long.MIN_VALUE;
        long cursor = start + random.nextInt(2000);
        int lateCounted = 0;
        int dropped = 0;
        while (actions.size() < 250) {
            String user = "u-%02d".formatted(random.nextInt(8));
            int kind = random.nextInt(100);
            long ts;
            if (kind < 85 || streamTime == Long.MIN_VALUE) {
                cursor += 1000 + random.nextInt(4000);
                ts = cursor;
            } else if (kind < 93) {
                ts = streamTime - (3000 + random.nextInt(20_000));
            } else {
                ts = streamTime - (100_000 + random.nextInt(120_000));
            }
            if (ts < start) {
                continue;
            }
            long windowStart = ts - Math.floorMod(ts, WINDOW);
            long windowEnd = windowStart + WINDOW;
            long newStreamTime = Math.max(streamTime, ts);
            long closesAt = windowEnd + GRACE;
            boolean late = ts < newStreamTime;
            if (late && Math.abs(closesAt - newStreamTime) < MARGIN) {
                continue;
            }
            streamTime = newStreamTime;
            actions.add(new Action(user, ts));
            if (windowEnd > streamTime - GRACE) {
                counts.merge(user + "@" + windowStart, 1L, Long::sum);
                if (late) {
                    lateCounted++;
                }
            } else {
                dropped++;
            }
        }
        return new Plan(actions, counts, lateCounted, dropped);
    }

    /** The last count per {@code user@windowStart}. A null key marks an unreadable record. */
    private static Map<String, Long> lastCounts(AttemptContext ctx, List<Received> records) {
        Map<String, Long> counts = new HashMap<>();
        for (Received record : records) {
            JsonNode node = Json.parse(ctx.json(), record.value()).orElse(null);
            if (node == null || !node.path("windowStart").isNumber() || !node.path("count").isNumber()) {
                counts.put(null, -1L);
                continue;
            }
            counts.put(node.path("userId").asString() + "@" + node.path("windowStart").asLong(),
                    node.path("count").asLong());
        }
        return counts;
    }

    private static String describe(String key) {
        int at = key.lastIndexOf('@');
        Instant windowStart = Instant.ofEpochMilli(Long.parseLong(key.substring(at + 1)));
        return "%s in the window starting %s UTC (%d)".formatted(key.substring(0, at), CLOCK.format(windowStart),
                windowStart.toEpochMilli());
    }
}
