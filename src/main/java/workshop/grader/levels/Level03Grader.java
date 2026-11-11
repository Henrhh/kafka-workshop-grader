package workshop.grader.levels;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import workshop.grader.grading.AttemptContext;
import workshop.grader.grading.LevelGrader;
import workshop.grader.grading.OutputReader;

/** Level 3: re-key unkeyed user events by userId. */
@Component
class Level03Grader implements LevelGrader {

    private static final List<String> ACTIONS = List.of("login", "view", "search", "add-to-cart", "logout");
    private static final List<String> COUNTRIES = List.of("NO", "SE", "DK", "FI", "DE", "NL", "GB");

    @Override
    public int level() {
        return 3;
    }

    @Override
    public void grade(AttemptContext ctx) throws Exception {
        ctx.purge("in");
        ctx.awaitGroup(1, Duration.ofSeconds(45));
        String out = ctx.topic("out");
        try (OutputReader reader = ctx.readFromNow("out")) {
            int users = 30 + ctx.random().nextInt(20);
            int count = 300;
            Map<String, String> sent = new HashMap<>();
            for (int i = 0; i < count; i++) {
                String eventId = "e-%04d".formatted(i);
                String value = ctx.json().createObjectNode()
                        .put("eventId", eventId)
                        .put("userId", "u-%02d".formatted(ctx.random().nextInt(users)))
                        .put("action", ACTIONS.get(ctx.random().nextInt(ACTIONS.size())))
                        .put("country", COUNTRIES.get(ctx.random().nextInt(COUNTRIES.size())))
                        .toString();
                sent.put(eventId, value);
                ctx.send("in", null, value);
            }
            ctx.flush();
            ctx.pass("Sent %d events without keys to %s".formatted(count, ctx.topic("in")));

            reader.pollUntil(r -> r.count(out) >= count, Duration.ofSeconds(30), Duration.ofSeconds(2));
            var records = reader.records(out);
            ctx.check(records.size() >= count, "Received %d events on %s".formatted(records.size(), out),
                    "Received %d of %d events on %s".formatted(records.size(), count, out));

            Set<String> seen = new HashSet<>();
            Map<String, Integer> partitionOfUser = new HashMap<>();
            for (var record : records) {
                JsonNode event = Json.parse(ctx.json(), record.value()).orElse(null);
                String eventId = event == null ? "" : event.path("eventId").asString();
                ctx.require(sent.containsKey(eventId) && sent.get(eventId).equals(record.value()), "This value is not one the grader sent, or it was changed: " + Json.shorten(record.value()));
                ctx.require(seen.add(eventId), "Event %s arrived more than once".formatted(eventId));
                String userId = event.path("userId").asString();
                ctx.require(userId.equals(record.key()), "Event %s has key %s, but its userId is %s".formatted(eventId, record.key(), userId));
                Integer partition = partitionOfUser.putIfAbsent(userId, record.record().partition());
                ctx.require(partition == null || partition == record.record().partition(), "Events of %s are spread over partitions %d and %d".formatted(
                                userId, partition, record.record().partition()));
            }
            ctx.pass("Every event arrived once, unchanged");
            ctx.pass("Every event is keyed by its userId, one partition per user");
        }
    }
}
