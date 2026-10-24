package workshop.grader.levels;

import java.time.Duration;
import org.springframework.stereotype.Component;
import workshop.grader.grading.AttemptContext;
import workshop.grader.grading.LevelGrader;
import workshop.grader.grading.OutputReader;

/** Level 2: produce the running sum of a few hundred numbers. */
@Component
class Level02Grader implements LevelGrader {

    @Override
    public int level() {
        return 2;
    }

    @Override
    public void grade(AttemptContext ctx) throws Exception {
        ctx.purge("in");
        ctx.awaitGroup(1, Duration.ofSeconds(45));
        String out = ctx.topic("out");
        try (OutputReader reader = ctx.readFromNow("out")) {
            int count = 300 + ctx.random().nextInt(201);
            long sum = 0;
            for (int i = 0; i < count; i++) {
                int number = ctx.random().nextInt(2001) - 1000;
                sum += number;
                ctx.send("in", null, Integer.toString(number));
            }
            ctx.flush();
            ctx.pass("Sent %d numbers to %s".formatted(count, ctx.topic("in")));

            String expected = Long.toString(sum);
            reader.pollUntil(r -> !r.records(out).isEmpty()
                    && expected.equals(r.records(out).getLast().value().trim()), Duration.ofSeconds(30),
                    Duration.ofSeconds(2));
            var records = reader.records(out);
            ctx.check(!records.isEmpty(), "Received totals on " + out,
                    "No totals arrived on %s. Produce the sum after each poll that returns records.".formatted(out));
            String last = records.getLast().value();
            ctx.check(last != null && expected.equals(last.trim()), "The last total is " + expected,
                    "Your last total was %s, but the numbers add up to %s".formatted(Json.shorten(last), expected));
        }
    }
}
