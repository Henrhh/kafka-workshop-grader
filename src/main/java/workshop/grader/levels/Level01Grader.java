package workshop.grader.levels;

import java.time.Duration;
import org.springframework.stereotype.Component;
import workshop.grader.grading.AttemptContext;
import workshop.grader.grading.LevelGrader;
import workshop.grader.grading.OutputReader;

/** Level 1: one record, key = team name, value = "hello". */
@Component
class Level01Grader implements LevelGrader {

    @Override
    public int level() {
        return 1;
    }

    @Override
    public void grade(AttemptContext ctx) {
        String out = ctx.topic("out");
        try (OutputReader reader = ctx.readSinceSubmission("out")) {
            reader.pollUntil(r -> r.records(out).stream()
                    .anyMatch(rec -> ctx.team().equals(rec.key()) && "hello".equals(rec.value())),
                    Duration.ofSeconds(20), Duration.ZERO);
            var records = reader.records(out);
            ctx.check(!records.isEmpty(), "Received a record on " + out,
                    "No record arrived on %s within 20 seconds. Did you call producer.send(...)?".formatted(out));
            ctx.check(records.stream().anyMatch(r -> "hello".equals(r.value())), "The value is \"hello\"",
                    "Expected the value \"hello\" but got \"%s\"".formatted(records.getLast().value()));
            ctx.check(records.stream().anyMatch(r -> "hello".equals(r.value()) && ctx.team().equals(r.key())),
                    "The key is your team name",
                    "Expected the key \"%s\" but got \"%s\"".formatted(ctx.team(), records.getLast().key()));
        }
    }
}
