package workshop.grader.levels;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import workshop.grader.grading.AttemptContext;
import workshop.grader.grading.LevelGrader;
import workshop.grader.grading.OutputReader;

/** Level 6: bank balances that only come out right if per-account order is kept. */
@Component
class Level06Grader implements LevelGrader {

    private record Result(String accountId, boolean accepted, long balance) {
    }

    @Override
    public int level() {
        return 6;
    }

    @Override
    public void grade(AttemptContext ctx) throws Exception {
        ctx.purge("in");
        ctx.awaitGroup(1, Duration.ofSeconds(45));
        String out = ctx.topic("out");
        try (OutputReader reader = ctx.readFromNow("out")) {
            int accounts = 40;
            int count = 600;
            Map<String, Long> balances = new HashMap<>();
            Map<String, Result> expected = new HashMap<>();
            for (int i = 0; i < count; i++) {
                String txId = "t-%04d".formatted(i);
                String account = "acc-%02d".formatted(ctx.random().nextInt(accounts));
                boolean deposit = ctx.random().nextInt(100) < 45;
                long amount = 10 + ctx.random().nextInt(491);
                long balance = balances.getOrDefault(account, 0L);
                boolean accepted = deposit || amount <= balance;
                if (accepted) {
                    balance += deposit ? amount : -amount;
                }
                balances.put(account, balance);
                expected.put(txId, new Result(account, accepted, balance));
                ctx.send("in", account, ctx.json().createObjectNode()
                        .put("txId", txId)
                        .put("accountId", account)
                        .put("type", deposit ? "DEPOSIT" : "WITHDRAWAL")
                        .put("amount", amount)
                        .toString());
            }
            ctx.flush();
            ctx.pass("Sent %d transactions for %d accounts".formatted(count, accounts));

            reader.pollUntil(r -> r.count(out) >= count, Duration.ofSeconds(30), Duration.ofSeconds(2));
            var records = reader.records(out);
            ctx.check(records.size() >= count, "Received %d results".formatted(records.size()),
                    "Received %d of %d results on %s".formatted(records.size(), count, out));

            Map<String, Integer> seen = new HashMap<>();
            for (var record : records) {
                JsonNode result = Json.parse(ctx.json(), record.value()).orElse(null);
                ctx.require(result != null && result.isObject(),
                        "This result is not a JSON object: " + Json.shorten(record.value()));
                String txId = result.path("txId").asString();
                Result want = expected.get(txId);
                ctx.require(want != null, "Unknown txId in result: " + Json.shorten(record.value()));
                ctx.require(seen.merge(txId, 1, Integer::sum) == 1, "Transaction %s has two results".formatted(txId));
                ctx.require(want.accountId().equals(record.key()),
                        "The result for %s has key %s, expected %s".formatted(txId, record.key(), want.accountId()));
                ctx.require(result.path("accepted").isBoolean() && result.path("accepted").asBoolean() == want.accepted(),
                        "%s should be %s".formatted(txId, want.accepted() ? "accepted" : "rejected"));
                ctx.require(result.path("balance").isIntegralNumber() && result.path("balance").asLong() == want.balance(),
                        "After %s the balance of %s should be %d, but you said %s".formatted(txId, want.accountId(),
                                want.balance(), result.path("balance")));
            }
            ctx.pass("Every transaction accepted or rejected correctly");
            ctx.pass("Every balance is right");
        }
    }
}
