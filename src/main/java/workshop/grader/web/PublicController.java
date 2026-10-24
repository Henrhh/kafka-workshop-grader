package workshop.grader.web;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import workshop.grader.GraderProperties;
import workshop.grader.events.FeedService;
import workshop.grader.events.WorkshopEvent;
import workshop.grader.grading.GradingService;
import workshop.grader.kafka.Topics;
import workshop.grader.score.ScoreRepository;
import workshop.grader.score.Scoring;
import workshop.grader.team.TeamService;

/** What the projector page and the join page use. */
@RestController
@RequestMapping("/api")
class PublicController {

    record JoinRequest(String team, String joinCode) {
    }

    record JoinResponse(String team, String properties) {
    }

    record Board(List<String> teams, List<LevelInfo> levels, List<ScoreRepository.Solve> solves,
            List<ScoreRepository.AttemptCount> failed, Map<String, String> active) {
    }

    record LevelInfo(int level, int points) {
    }

    private final ScoreRepository scores;
    private final FeedService feed;
    private final TeamService teams;
    private final GradingService grading;
    private final GraderProperties properties;

    PublicController(ScoreRepository scores, FeedService feed, TeamService teams, GradingService grading,
            GraderProperties properties) {
        this.scores = scores;
        this.feed = feed;
        this.teams = teams;
        this.grading = grading;
        this.properties = properties;
    }

    @GetMapping("/leaderboard")
    List<ScoreRepository.LeaderboardRow> leaderboard() {
        return scores.leaderboard();
    }

    @GetMapping("/board")
    Board board() {
        List<LevelInfo> levels = Topics.levels().stream().map(l -> new LevelInfo(l, Scoring.basePoints(l))).toList();
        return new Board(scores.teams(), levels, scores.solves(), scores.failedAttempts(), grading.activeAttempts());
    }

    @GetMapping("/feed")
    List<WorkshopEvent> feed() {
        return feed.recent();
    }

    @GetMapping(path = "/feed/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    SseEmitter stream() {
        return feed.subscribe();
    }

    @GetMapping("/info")
    Map<String, Object> info() {
        return Map.of("joinCodeRequired", !properties.joinCode().isBlank());
    }

    @PostMapping("/join")
    ResponseEntity<?> join(@RequestBody JoinRequest request) {
        if (!properties.joinCode().isBlank() && !matches(properties.joinCode(), request.joinCode())) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of("error", "That join code is not right."));
        }
        String team = request.team() == null ? null : request.team().trim().toLowerCase();
        try {
            return ResponseEntity.ok(new JoinResponse(team, teams.register(team)));
        } catch (TeamService.TeamExistsException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    static boolean matches(String expected, String given) {
        return given != null && MessageDigest.isEqual(expected.getBytes(StandardCharsets.UTF_8),
                given.trim().getBytes(StandardCharsets.UTF_8));
    }
}
