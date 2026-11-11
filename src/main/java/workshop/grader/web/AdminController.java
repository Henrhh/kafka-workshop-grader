package workshop.grader.web;

import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import workshop.grader.GraderProperties;
import workshop.grader.events.EventPublisher;
import workshop.grader.score.ScoreRepository;
import workshop.grader.team.TeamService;

/** Facilitator actions. Every call needs the X-Admin-Token header to match grader.admin-token. */
@RestController
@RequestMapping("/api/admin")
class AdminController {

    record TeamRequest(String team) {
    }

    record Announcement(String message) {
    }

    private final TeamService teams;
    private final ScoreRepository scores;
    private final EventPublisher events;
    private final GraderProperties properties;

    AdminController(TeamService teams, ScoreRepository scores, EventPublisher events, GraderProperties properties) {
        this.teams = teams;
        this.scores = scores;
        this.events = events;
        this.properties = properties;
    }

    @GetMapping("/teams")
    ResponseEntity<?> listTeams(@RequestHeader(name = "X-Admin-Token", required = false) String token) {
        if (!authorized(token)) {
            return forbidden();
        }
        return ResponseEntity.ok(scores.teams());
    }

    @PostMapping("/teams")
    ResponseEntity<?> createTeam(@RequestHeader(name = "X-Admin-Token", required = false) String token,
            @RequestBody TeamRequest request) {
        if (!authorized(token)) {
            return forbidden();
        }
        try {
            return ResponseEntity.ok(Map.of("team", request.team(), "properties", teams.register(request.team())));
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/teams/{team}/reset")
    ResponseEntity<?> reset(@RequestHeader(name = "X-Admin-Token", required = false) String token,
            @PathVariable String team) {
        if (!authorized(token)) {
            return forbidden();
        }
        try {
            return ResponseEntity.ok(Map.of("team", team, "properties", teams.resetCredentials(team)));
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/announcements")
    ResponseEntity<?> announce(@RequestHeader(name = "X-Admin-Token", required = false) String token,
            @RequestBody Announcement announcement) {
        if (!authorized(token)) {
            return forbidden();
        }
        if (announcement.message() == null || announcement.message().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Write a message first."));
        }
        events.announce(announcement.message().strip());
        return ResponseEntity.ok(Map.of("sent", true));
    }

    private boolean authorized(String token) {
        return !properties.adminToken().isBlank() && PublicController.matches(properties.adminToken(), token);
    }

    private static ResponseEntity<?> forbidden() {
        return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of("error", "Wrong or missing admin token."));
    }
}
