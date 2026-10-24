package workshop.grader.score;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Teams, attempts and solves in Postgres. */
@Repository
public class ScoreRepository {

    public record Solve(String team, int level, int points, int rank, int failedAttempts, int bonus, Instant solvedAt) {
    }

    public record LeaderboardRow(String team, int points, int levelsSolved, Instant lastSolvedAt) {
    }

    public record AttemptCount(String team, int level, int failed) {
    }

    private final JdbcClient jdbc;

    public ScoreRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public boolean teamExists(String team) {
        return jdbc.sql("select count(*) from team where name = ?").param(team).query(Integer.class).single() > 0;
    }

    public void createTeam(String team) {
        jdbc.sql("insert into team(name, created_at) values (?, ?)").params(team, now()).update();
    }

    public List<String> teams() {
        return jdbc.sql("select name from team order by created_at").query(String.class).list();
    }

    public void startAttempt(String id, String team, int level) {
        jdbc.sql("insert into attempt(id, team, level, status, started_at) values (?, ?, ?, 'RUNNING', ?)")
                .params(id, team, level, now())
                .update();
    }

    public void finishAttempt(String id, String status, String message) {
        jdbc.sql("update attempt set status = ?, message = ?, finished_at = ? where id = ?")
                .params(status, message, now(), id)
                .update();
    }

    public Optional<Solve> solve(String team, int level) {
        return jdbc.sql("""
                        select team, level, points, solve_rank, failed_attempts, bonus, solved_at
                        from solve where team = ? and level = ?""")
                .params(team, level)
                .query((rs, n) -> new Solve(rs.getString(1), rs.getInt(2), rs.getInt(3), rs.getInt(4), rs.getInt(5),
                        rs.getInt(6), rs.getObject(7, OffsetDateTime.class).toInstant()))
                .optional();
    }

    /**
     * Records a first solve and returns it with its points. Synchronized, with each statement
     * committed on its own, so two teams solving at the same moment cannot get the same rank.
     * The grader runs as a single instance.
     */
    public synchronized Solve recordSolve(String team, int level, int bonus) {
        Optional<Solve> existing = solve(team, level);
        if (existing.isPresent()) {
            return existing.get();
        }
        int rank = jdbc.sql("select count(*) from solve where level = ?").param(level).query(Integer.class).single() + 1;
        int failed = jdbc.sql("select count(*) from attempt where team = ? and level = ? and status = 'FAILED'")
                .params(team, level)
                .query(Integer.class)
                .single();
        int points = Scoring.points(level, rank, failed, bonus);
        Instant solvedAt = Instant.now();
        jdbc.sql("""
                        insert into solve(team, level, points, solve_rank, failed_attempts, bonus, solved_at)
                        values (?, ?, ?, ?, ?, ?, ?)""")
                .params(team, level, points, rank, failed, bonus, solvedAt.atOffset(ZoneOffset.UTC))
                .update();
        return new Solve(team, level, points, rank, failed, bonus, solvedAt);
    }

    public List<LeaderboardRow> leaderboard() {
        return jdbc.sql("""
                        select t.name, coalesce(sum(s.points), 0), count(s.level), max(s.solved_at)
                        from team t left join solve s on s.team = t.name
                        group by t.name
                        order by coalesce(sum(s.points), 0) desc, max(s.solved_at) asc nulls last, t.name""")
                .query((rs, n) -> {
                    OffsetDateTime last = rs.getObject(4, OffsetDateTime.class);
                    return new LeaderboardRow(rs.getString(1), rs.getInt(2), rs.getInt(3),
                            last == null ? null : last.toInstant());
                })
                .list();
    }

    public List<Solve> solves() {
        return jdbc.sql("select team, level, points, solve_rank, failed_attempts, bonus, solved_at from solve")
                .query((rs, n) -> new Solve(rs.getString(1), rs.getInt(2), rs.getInt(3), rs.getInt(4), rs.getInt(5),
                        rs.getInt(6), rs.getObject(7, OffsetDateTime.class).toInstant()))
                .list();
    }

    public List<AttemptCount> failedAttempts() {
        return jdbc.sql("select team, level, count(*) from attempt where status = 'FAILED' group by team, level")
                .query((rs, n) -> new AttemptCount(rs.getString(1), rs.getInt(2), rs.getInt(3)))
                .list();
    }

    private static OffsetDateTime now() {
        return OffsetDateTime.now(ZoneOffset.UTC);
    }
}
