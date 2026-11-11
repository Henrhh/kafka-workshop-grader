package workshop.grader.levels;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import workshop.grader.grading.AttemptContext;
import workshop.grader.grading.LevelGrader;
import workshop.grader.grading.OutputReader;

/** Level 10: enrich a fast order stream with a customer table and measure latency. */
@Component
class Level10Grader implements LevelGrader {

    private static final List<String> FIRST = List.of("Ada", "Grace", "Alan", "Margaret", "Linus", "Barbara",
            "Dennis", "Frances", "Ken", "Radia", "Edsger", "Katherine");
    private static final List<String> LAST = List.of("Lovelace", "Hopper", "Turing", "Hamilton", "Torvalds",
            "Liskov", "Ritchie", "Allen", "Thompson", "Perlman", "Dijkstra", "Johnson");
    private static final List<String> TIERS = List.of("GOLD", "SILVER", "BRONZE");
    private static final int CUSTOMERS = 60;
    private static final int ORDERS = 1000;
    private static final int BATCH = 10;
    /** Orders sent this long after a tier change must see the new tier. */
    private static final Duration SETTLE = Duration.ofSeconds(3);

    private record Customer(String name, String tier) {
    }

    private record Order(String customerId, BigDecimal amount, Instant sentAt, Customer before, Customer after,
            Instant changedAt) {
    }

    @Override
    public int level() {
        return 10;
    }

    @Override
    public void grade(AttemptContext ctx) throws Exception {
        ctx.purge("in");
        ctx.awaitGroup(1, Duration.ofSeconds(60));
        String out = ctx.topic("out");

        Map<String, Customer> customers = new HashMap<>();
        for (int i = 1; i <= CUSTOMERS; i++) {
            String id = "c-%03d".formatted(i);
            Customer customer = new Customer(FIRST.get(ctx.random().nextInt(FIRST.size())) + " "
                    + LAST.get(ctx.random().nextInt(LAST.size())), TIERS.get(ctx.random().nextInt(TIERS.size())));
            customers.put(id, customer);
            sendCustomer(ctx, id, customer);
        }
        ctx.flush();
        ctx.pass("Sent %d customers to %s".formatted(CUSTOMERS, ctx.topic("customers")));
        Thread.sleep(3000);

        Map<String, Order> orders = new HashMap<>();
        Map<String, Instant> changedAt = new HashMap<>();
        Map<String, Customer> previous = new HashMap<>();
        try (OutputReader reader = ctx.readFromNow("out")) {
            for (int i = 0; i < ORDERS; i++) {
                if (i == ORDERS / 2) {
                    changeTiers(ctx, customers, previous, changedAt);
                }
                boolean unknown = ctx.random().nextInt(100) < 5;
                String customerId = unknown ? "c-9%02d".formatted(ctx.random().nextInt(100))
                        : "c-%03d".formatted(1 + ctx.random().nextInt(CUSTOMERS));
                String orderId = "o-%04d".formatted(i);
                BigDecimal amount = BigDecimal.valueOf(100 + ctx.random().nextInt(99_900), 2);
                ctx.send("in", customerId, "{\"orderId\":\"%s\",\"customerId\":\"%s\",\"amount\":%s}"
                        .formatted(orderId, customerId, amount.toPlainString()));
                orders.put(orderId, new Order(customerId, amount, Instant.now(), previous.get(customerId),
                        customers.get(customerId), changedAt.get(customerId)));
                if (i % BATCH == BATCH - 1) {
                    ctx.flush();
                    reader.poll(Duration.ofMillis(100));
                }
            }
            ctx.flush();
            ctx.pass("Fired %d orders at about %d per second, and changed %d tiers halfway".formatted(ORDERS,
                    BATCH * 10, changedAt.size()));

            reader.pollUntil(r -> r.count(out) >= ORDERS, Duration.ofSeconds(30), Duration.ofSeconds(2));
            var records = reader.records(out);
            ctx.check(records.size() >= ORDERS, "Received %d enriched orders".formatted(records.size()),
                    "Received %d of %d enriched orders on %s".formatted(records.size(), ORDERS, out));

            Map<String, Integer> seen = new HashMap<>();
            List<Long> latencies = new ArrayList<>();
            for (var record : records) {
                JsonNode o = Json.parse(ctx.json(), record.value()).orElse(null);
                ctx.require(o != null && o.isObject(), "This record is not a JSON object: " + Json.shorten(record.value()));
                String orderId = o.path("orderId").asString();
                Order want = orders.get(orderId);
                ctx.require(want != null, "Unknown order on %s: %s".formatted(out, Json.shorten(record.value())));
                ctx.require(seen.merge(orderId, 1, Integer::sum) == 1, "Order %s came out twice".formatted(orderId));
                ctx.require(want.customerId().equals(record.key()) && want.customerId().equals(o.path("customerId").asString()),
                        "Order %s should be keyed by its customerId, %s".formatted(orderId, want.customerId()));
                ctx.require(o.path("amount").isNumber() && o.path("amount").decimalValue().compareTo(want.amount()) == 0,
                        "Order %s should keep amount %s".formatted(orderId, want.amount()));
                checkCustomer(ctx, orderId, o, want);
                latencies.add(Duration.between(want.sentAt(), record.receivedAt()).toMillis());
            }
            ctx.pass("Every order has the right customer, tier and discount");
            ctx.pass("Tier changes were picked up by later orders");
            speedBonus(ctx, latencies);
        }
    }

    private void changeTiers(AttemptContext ctx, Map<String, Customer> customers, Map<String, Customer> previous,
            Map<String, Instant> changedAt) {
        for (int n = 0; n < 10; n++) {
            String id = "c-%03d".formatted(1 + ctx.random().nextInt(CUSTOMERS));
            if (changedAt.containsKey(id)) {
                continue;
            }
            Customer old = customers.get(id);
            String tier = TIERS.get((TIERS.indexOf(old.tier()) + 1 + ctx.random().nextInt(2)) % TIERS.size());
            Customer updated = new Customer(old.name(), tier);
            previous.put(id, old);
            customers.put(id, updated);
            changedAt.put(id, Instant.now());
            sendCustomer(ctx, id, updated);
        }
        ctx.flush();
    }

    private void checkCustomer(AttemptContext ctx, String orderId, JsonNode o, Order want) {
        String name = o.path("customerName").asString();
        String tier = o.path("tier").asString();
        List<Customer> allowed = new ArrayList<>();
        if (want.after() == null) {
            allowed.add(new Customer("unknown", "NEW"));
        } else {
            allowed.add(want.after());
            boolean recentChange = want.changedAt() != null
                    && want.sentAt().isBefore(want.changedAt().plus(SETTLE));
            if (recentChange) {
                allowed.add(want.before());
            }
        }
        Customer got = new Customer(name, tier);
        ctx.require(allowed.contains(got), want.after() == null
                ? "Order %s is for an unknown customer: expected customerName \"unknown\" and tier \"NEW\", got %s / %s"
                        .formatted(orderId, name, tier)
                : "Order %s should have customer %s / %s, got %s / %s".formatted(orderId, want.after().name(),
                        want.after().tier(), name, tier));
        BigDecimal expected = want.amount().subtract(want.amount().multiply(discount(tier)))
                .setScale(2, RoundingMode.HALF_UP);
        JsonNode discounted = o.path("discountedAmount");
        ctx.require(discounted.isNumber() && discounted.decimalValue().subtract(expected).abs()
                        .compareTo(new BigDecimal("0.005")) <= 0,
                "Order %s (%s, amount %s) should have discountedAmount %s, got %s".formatted(orderId, tier,
                        want.amount(), expected, discounted));
    }

    private static BigDecimal discount(String tier) {
        return switch (tier) {
            case "GOLD" -> new BigDecimal("0.10");
            case "SILVER" -> new BigDecimal("0.05");
            default -> BigDecimal.ZERO;
        };
    }

    private void speedBonus(AttemptContext ctx, List<Long> latencies) {
        latencies.sort(null);
        long p95 = latencies.get((int) Math.ceil(latencies.size() * 0.95) - 1);
        int bonus = p95 <= 500 ? 100 : p95 <= 1000 ? 60 : p95 <= 2000 ? 30 : 0;
        ctx.addBonus(bonus);
        ctx.pass(bonus > 0
                ? "p95 latency %d ms: +%d speed bonus".formatted(p95, bonus)
                : "p95 latency %d ms: no speed bonus this time (needs 2000 ms or less)".formatted(p95));
    }

    private static void sendCustomer(AttemptContext ctx, String id, Customer customer) {
        ctx.send("customers", id, ctx.json().createObjectNode()
                .put("customerId", id)
                .put("name", customer.name())
                .put("tier", customer.tier())
                .toString());
    }
}
