package workshop.grader.levels;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
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

/** Level 4: valid orders get a total, everything else goes to the dead letter topic. */
@Component
class Level04Grader implements LevelGrader {

    private static final List<String> SKUS = List.of("KAFKA-MUG", "STICKER", "HOODIE", "T-SHIRT", "NOTEBOOK", "CAP");

    @Override
    public int level() {
        return 4;
    }

    @Override
    public void grade(AttemptContext ctx) throws Exception {
        ctx.purge("in");
        ctx.awaitGroup(1, Duration.ofSeconds(45));
        String out = ctx.topic("out");
        String dlq = ctx.topic("dlq");
        Random random = ctx.random();
        try (OutputReader reader = ctx.readFromNow("out", "dlq")) {
            Map<String, BigDecimal> validTotals = new HashMap<>();
            List<String> invalid = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                String orderId = "o-%04d".formatted(i);
                String sku = SKUS.get(random.nextInt(SKUS.size()));
                int quantity = 1 + random.nextInt(10);
                BigDecimal unitPrice = BigDecimal.valueOf(50 + random.nextInt(9950), 2);
                String value;
                if (random.nextInt(100) < 20) {
                    value = broken(random, orderId, sku, quantity, unitPrice);
                    invalid.add(value);
                } else {
                    value = "{\"orderId\":\"%s\",\"sku\":\"%s\",\"quantity\":%d,\"unitPrice\":%s}"
                            .formatted(orderId, sku, quantity, unitPrice.toPlainString());
                    validTotals.put(orderId, unitPrice.multiply(BigDecimal.valueOf(quantity))
                            .setScale(2, RoundingMode.HALF_UP));
                }
                ctx.send("in", orderId, value);
            }
            ctx.flush();
            ctx.pass("Sent 200 orders to %s, some of them broken".formatted(ctx.topic("in")));

            reader.pollUntil(r -> r.count(out) >= validTotals.size() && r.count(dlq) >= invalid.size(),
                    Duration.ofSeconds(30), Duration.ofSeconds(2));

            var orders = reader.records(out);
            ctx.check(orders.size() == validTotals.size(), "%d valid orders on %s".formatted(orders.size(), out),
                    "Expected %d valid orders on %s but got %d".formatted(validTotals.size(), out, orders.size()));
            Map<String, Integer> seen = new HashMap<>();
            for (var record : orders) {
                JsonNode order = Json.parse(ctx.json(), record.value()).orElse(null);
                ctx.require(order != null && order.isObject(), "This record on %s is not a JSON object: %s".formatted(out, Json.shorten(record.value())));
                String orderId = order.path("orderId").asString();
                ctx.require(validTotals.containsKey(orderId), "Order %s on %s should have gone to the dead letter topic".formatted(orderId, out));
                ctx.require(seen.merge(orderId, 1, Integer::sum) == 1, "Order %s arrived twice".formatted(orderId));
                ctx.require(orderId.equals(record.key()), "Order %s has key %s, expected its orderId".formatted(orderId, record.key()));
                JsonNode total = order.path("total");
                ctx.require(total.isNumber() && total.decimalValue().subtract(validTotals.get(orderId)).abs()
                                .compareTo(new BigDecimal("0.005")) <= 0, "Order %s has total %s, expected %s".formatted(orderId, total, validTotals.get(orderId)));
            }
            ctx.pass("Every valid order has the right total and key");

            List<String> dead = reader.records(dlq).stream().map(r -> r.value()).toList();
            List<String> missing = new ArrayList<>(invalid);
            dead.forEach(missing::remove);
            ctx.require(missing.isEmpty(), "%d broken orders are missing from %s, for example: %s".formatted(missing.size(), dlq,
                            missing.isEmpty() ? "" : Json.shorten(missing.getFirst())));
            ctx.check(dead.size() == invalid.size(), "All %d broken orders reached %s unchanged".formatted(
                    invalid.size(), dlq), "Expected %d records on %s but got %d".formatted(invalid.size(), dlq,
                    dead.size()));
            ctx.check(ctx.groupAlive(), "Your consumer survived the bad data",
                    "Your consumer group has no members any more. Did one bad record crash it?");
        }
    }

    private static String broken(Random random, String orderId, String sku, int quantity, BigDecimal unitPrice) {
        return switch (random.nextInt(8)) {
            case 0 -> "%s;%s;%d;%s".formatted(orderId, sku, quantity, unitPrice);
            case 1 -> "{\"orderId\":\"%s\",\"sku\":\"%s\",\"quantity\":%d".formatted(orderId, sku, quantity);
            case 2 -> "{\"orderId\":\"%s\",\"sku\":\"%s\",\"unitPrice\":%s}".formatted(orderId, sku, unitPrice);
            case 3 -> "{\"orderId\":\"%s\",\"sku\":\"%s\",\"quantity\":0,\"unitPrice\":%s}".formatted(orderId, sku,
                    unitPrice);
            case 4 -> "{\"orderId\":\"%s\",\"sku\":\"%s\",\"quantity\":-%d,\"unitPrice\":%s}".formatted(orderId, sku,
                    quantity, unitPrice);
            case 5 -> "{\"orderId\":\"%s\",\"sku\":\"%s\",\"quantity\":\"%d\",\"unitPrice\":%s}".formatted(orderId,
                    sku, quantity, unitPrice);
            case 6 -> "{\"orderId\":\"%s\",\"sku\":\"%s\",\"quantity\":%d,\"unitPrice\":-%s}".formatted(orderId, sku,
                    quantity, unitPrice);
            default -> random.nextBoolean() ? "null" : "[\"%s\",%d]".formatted(orderId, quantity);
        };
    }
}
