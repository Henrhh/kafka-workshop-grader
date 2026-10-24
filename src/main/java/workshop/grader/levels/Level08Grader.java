package workshop.grader.levels;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import workshop.grader.grading.AttemptContext;
import workshop.grader.grading.LevelGrader;
import workshop.grader.grading.OutputReader;

/** Level 8: Kafka Streams filter, mapValues and split. */
@Component
class Level08Grader implements LevelGrader {

    private static final List<String> PAGES = List.of("/checkout", "/checkout/pay", "/checkout/confirm",
            "/product/kafka-mug", "/product/hoodie", "/products-archive", "/", "/about", "/blog/streams",
            "/help/checkout", "/search");
    private static final List<String> BROWSERS = List.of("Mozilla/5.0 (Macintosh)", "Mozilla/5.0 (Windows NT 10.0)",
            "Mozilla/5.0 (iPhone)", "curl/8.5.0");
    private static final List<String> BOTS = List.of("Googlebot/2.1", "bingbot/2.0", "AhrefsBot/7.0",
            "SuperBOT 3000", "Mozilla/5.0 (compatible; YandexBot/3.0)");

    private record Expected(String topic, String userId, String page, double seconds) {
    }

    @Override
    public int level() {
        return 8;
    }

    @Override
    public void grade(AttemptContext ctx) throws Exception {
        ctx.purge("in");
        ctx.awaitGroup(1, Duration.ofSeconds(60));
        List<String> outputs = List.of("checkout", "product", "other");
        try (OutputReader reader = ctx.readFromNow("checkout", "product", "other")) {
            int count = 300;
            Map<String, Expected> expected = new HashMap<>();
            int bots = 0;
            for (int i = 0; i < count; i++) {
                String clickId = "c-%04d".formatted(i);
                String userId = "u-%02d".formatted(ctx.random().nextInt(40));
                String page = PAGES.get(ctx.random().nextInt(PAGES.size()));
                long durationMs = 100 + ctx.random().nextInt(30_000);
                boolean bot = ctx.random().nextInt(100) < 15;
                String agent = bot ? BOTS.get(ctx.random().nextInt(BOTS.size()))
                        : BROWSERS.get(ctx.random().nextInt(BROWSERS.size()));
                if (bot) {
                    bots++;
                } else {
                    String topic = page.startsWith("/checkout") ? "checkout"
                            : page.startsWith("/product") ? "product" : "other";
                    expected.put(clickId, new Expected(ctx.topic(topic), userId, page, durationMs / 1000.0));
                }
                ctx.send("in", userId, ctx.json().createObjectNode()
                        .put("clickId", clickId)
                        .put("userId", userId)
                        .put("page", page)
                        .put("durationMs", durationMs)
                        .put("userAgent", agent)
                        .toString());
            }
            ctx.flush();
            ctx.pass("Sent %d clicks, %d of them from bots".formatted(count, bots));

            int wanted = expected.size();
            reader.pollUntil(r -> outputs.stream().mapToInt(t -> r.count(ctx.topic(t))).sum() >= wanted,
                    Duration.ofSeconds(45), Duration.ofSeconds(3));

            Map<String, Integer> seen = new HashMap<>();
            for (String name : outputs) {
                String topic = ctx.topic(name);
                for (var record : reader.records(topic)) {
                    JsonNode click = Json.parse(ctx.json(), record.value()).orElse(null);
                    ctx.require(click != null && click.isObject(),
                            "This record on %s is not a JSON object: %s".formatted(topic, Json.shorten(record.value())));
                    String clickId = click.path("clickId").asString();
                    Expected want = expected.get(clickId);
                    ctx.require(want != null, "Click %s on %s should have been filtered out as a bot"
                            .formatted(clickId, topic));
                    ctx.require(seen.merge(clickId, 1, Integer::sum) == 1, "Click %s came out twice".formatted(clickId));
                    ctx.require(want.topic().equals(topic), "Click %s for page %s went to %s, expected %s"
                            .formatted(clickId, want.page(), topic, want.topic()));
                    ctx.require(want.userId().equals(record.key()) && want.userId().equals(click.path("userId").asString())
                                    && want.page().equals(click.path("page").asString()),
                            "Click %s should be keyed by userId and keep userId and page: %s".formatted(clickId,
                                    Json.shorten(record.value())));
                    ctx.require(click.path("seconds").isNumber()
                                    && Math.abs(click.path("seconds").asDouble() - want.seconds()) < 0.0005,
                            String.format(Locale.ROOT, "Click %s should have seconds = %.3f, but had %s", clickId,
                                    want.seconds(), click.path("seconds")));
                }
            }
            ctx.check(seen.size() == wanted, "No bots got through",
                    "Only %d of %d human clicks came out".formatted(seen.size(), wanted));
            ctx.pass("Every click was reshaped and routed to the right topic");
        }
    }
}
