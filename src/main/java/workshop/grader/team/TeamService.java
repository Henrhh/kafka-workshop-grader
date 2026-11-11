package workshop.grader.team;

import java.util.regex.Pattern;
import org.springframework.stereotype.Service;
import workshop.grader.GraderProperties;
import workshop.grader.events.EventPublisher;
import workshop.grader.events.WorkshopEvent;
import workshop.grader.kafka.Credentials;
import workshop.grader.kafka.TopicProvisioner;
import workshop.grader.kafka.Topics;
import workshop.grader.score.ScoreRepository;

/** Registers teams: database row, Kafka user, topics and ACLs, and their workshop.properties. */
@Service
public class TeamService {

    /** Same rule as WorkshopConfig in the template. */
    private static final Pattern TEAM_NAME = Pattern.compile("[a-z][a-z0-9-]{1,23}");

    private final ScoreRepository scores;
    private final TopicProvisioner provisioner;
    private final EventPublisher events;
    private final GraderProperties properties;

    public TeamService(ScoreRepository scores, TopicProvisioner provisioner, EventPublisher events,
            GraderProperties properties) {
        this.scores = scores;
        this.provisioner = provisioner;
        this.events = events;
        this.properties = properties;
    }

    public static boolean validName(String team) {
        return team != null && TEAM_NAME.matcher(team).matches();
    }

    /** Creates a new team and returns its workshop.properties. */
    public synchronized String register(String team) {
        if (!validName(team)) {
            throw new IllegalArgumentException(
                    "Team names are 2-24 characters of a-z, 0-9 and '-', starting with a letter");
        }
        if (scores.teamExists(team)) {
            throw new TeamExistsException(team);
        }
        String password = Credentials.newPassword();
        provisioner.upsertUser(team, password);
        provisioner.ensureTeam(team);
        scores.createTeam(team);
        events.publish(WorkshopEvent.Type.TEAM_JOINED, team, null, null, "Team %s joined the workshop".formatted(team),
                null);
        return propertiesFile(team, password);
    }

    /** Gives an existing team a new password, for when they lose their file. */
    public synchronized String resetCredentials(String team) {
        if (!scores.teamExists(team)) {
            throw new IllegalArgumentException("No team " + team);
        }
        String password = Credentials.newPassword();
        provisioner.upsertUser(team, password);
        provisioner.ensureTeam(team);
        return propertiesFile(team, password);
    }

    String propertiesFile(String team, String password) {
        GraderProperties.Kafka kafka = properties.kafka();
        StringBuilder file = new StringBuilder();
        file.append("# Kafka workshop settings for team ").append(team).append('\n');
        file.append("# Keep this file out of git. It is already in .gitignore.\n");
        line(file, "team", team);
        line(file, "bootstrap.servers", kafka.bootstrapForTeams());
        line(file, "security.protocol", kafka.securityProtocol());
        if (kafka.usesSasl()) {
            line(file, "sasl.mechanism", kafka.saslMechanism());
            line(file, "sasl.jaas.config", Credentials.jaasConfig(Topics.principal(team), password));
        }
        if (kafka.securityProtocol().endsWith("SSL") && !kafka.caCertificate().isBlank()) {
            line(file, "ssl.truststore.type", "PEM");
            line(file, "ssl.truststore.certificates", kafka.caCertificate().strip());
        }
        return file.toString();
    }

    /** Writes key=value with java.util.Properties escaping, so newlines in PEM text survive. */
    private static void line(StringBuilder file, String key, String value) {
        StringBuilder escaped = new StringBuilder();
        for (char c : value.toCharArray()) {
            switch (c) {
                case '\\' -> escaped.append("\\\\");
                case '\n' -> escaped.append("\\n");
                case '\r' -> { }
                default -> escaped.append(c);
            }
        }
        file.append(key).append('=').append(escaped).append('\n');
    }

    public static class TeamExistsException extends RuntimeException {

        public TeamExistsException(String team) {
            super("Team " + team + " already exists. Ask a facilitator if it is yours and you lost the file.");
        }
    }
}
