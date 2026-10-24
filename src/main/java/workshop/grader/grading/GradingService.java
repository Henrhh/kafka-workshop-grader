package workshop.grader.grading;

import jakarta.annotation.PreDestroy;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;
import workshop.grader.GraderProperties;
import workshop.grader.events.EventPublisher;
import workshop.grader.events.WorkshopEvent.Type;
import workshop.grader.kafka.KafkaClients;
import workshop.grader.kafka.Topics;
import workshop.grader.score.ScoreRepository;

/** Runs attempts: one at a time per team, several teams in parallel. */
@Service
public class GradingService {

    private static final Logger log = LoggerFactory.getLogger(GradingService.class);
    private static final SecureRandom SEEDS = new SecureRandom();

    private final Map<Integer, LevelGrader> graders;
    private final KafkaClients clients;
    private final EventPublisher events;
    private final ScoreRepository scores;
    private final JsonMapper json;
    private final ExecutorService executor;
    private final Map<String, String> activeAttempts = new ConcurrentHashMap<>();

    public GradingService(List<LevelGrader> graders, KafkaClients clients, EventPublisher events,
            ScoreRepository scores, JsonMapper json, GraderProperties properties) {
        this.graders = graders.stream().collect(Collectors.toMap(LevelGrader::level, Function.identity()));
        this.clients = clients;
        this.events = events;
        this.scores = scores;
        this.json = json;
        this.executor = Executors.newFixedThreadPool(properties.maxParallelAttempts());
    }

    /** Teams with an attempt running right now, mapped to the level. */
    public Map<String, String> activeAttempts() {
        return Map.copyOf(activeAttempts);
    }

    public Set<Integer> levels() {
        return graders.keySet();
    }

    public void submit(String team, int level, String attemptId, Instant submittedAt) {
        LevelGrader grader = graders.get(level);
        if (grader == null) {
            events.publish(Type.FAILED, team, level, attemptId, "There is no level " + level, null);
            return;
        }
        if (activeAttempts.putIfAbsent(team, attemptId + ":" + level) != null) {
            events.publish(Type.FAILED, team, level, attemptId,
                    "Your team already has an attempt running. Wait for it to finish, then submit again.", null);
            return;
        }
        executor.submit(() -> run(grader, team, level, attemptId, submittedAt));
    }

    private void run(LevelGrader grader, String team, int level, String attemptId, Instant submittedAt) {
        Instant started = Instant.now();
        try {
            scores.startAttempt(attemptId, team, level);
            events.publish(Type.ATTEMPT_STARTED, team, level, attemptId,
                    "Grading level %d for team %s".formatted(level, team), null);
            AttemptContext ctx = new AttemptContext(team, level, attemptId, submittedAt, SEEDS.nextLong(), clients,
                    events, json);
            grader.grade(ctx);

            var solve = scores.recordSolve(team, level, ctx.bonus());
            if (solve.solvedAt().isBefore(started)) {
                scores.finishAttempt(attemptId, "SOLVED", "solved again");
                events.publish(Type.SOLVED, team, level, attemptId,
                        "Level %d solved again. You keep your %d points.".formatted(level, solve.points()), null);
            } else {
                scores.finishAttempt(attemptId, "SOLVED", null);
                String rank = switch (solve.rank()) {
                    case 1 -> ", first to solve it!";
                    case 2 -> ", second to solve it!";
                    case 3 -> ", third to solve it!";
                    default -> "";
                };
                events.publish(Type.SOLVED, team, level, attemptId,
                        "Level %d solved for %d points%s".formatted(level, solve.points(), rank), solve.points());
            }
        } catch (CheckFailed e) {
            scores.finishAttempt(attemptId, "FAILED", e.getMessage());
            events.publish(Type.FAILED, team, level, attemptId, e.getMessage(), null);
        } catch (Exception e) {
            log.error("Grading level {} for {} crashed", level, team, e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            scores.finishAttempt(attemptId, "ERROR", String.valueOf(e));
            events.publish(Type.FAILED, team, level, attemptId,
                    "The grader hit an error, so this attempt does not count against you: " + e.getMessage(), null);
        } finally {
            cleanUp(team, level);
            activeAttempts.remove(team);
        }
    }

    /**
     * Empties the level's input topics and Streams internal topics, so a stopped solution
     * cannot read old data or restore old state next time.
     */
    private void cleanUp(String team, int level) {
        try {
            AttemptContext ctx = new AttemptContext(team, level, "cleanup", Instant.now(), 0, clients, events, json);
            if (Topics.forLevel(level).stream().anyMatch(spec -> spec.name().equals("in"))) {
                ctx.purge("in");
            }
            ctx.purgeInternalTopics();
        } catch (Exception e) {
            log.warn("Could not clean up level {} for {}", level, team, e);
        }
    }

    @PreDestroy
    void stop() {
        executor.shutdownNow();
    }
}
