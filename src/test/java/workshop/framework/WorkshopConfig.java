package workshop.framework;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Settings from {@code workshop.properties} in the project root.
 *
 * <p>{@code team} names your team. Every other key, such as {@code bootstrap.servers} or
 * {@code sasl.jaas.config}, is passed straight to the Kafka clients.
 */
public final class WorkshopConfig {

    public static final String FILE_NAME = "workshop.properties";
    private static final Pattern TEAM_NAME = Pattern.compile("[a-z][a-z0-9-]{1,23}");

    private final String team;
    private final Properties kafka;

    public WorkshopConfig(String team, Properties kafka) {
        if (team == null || !TEAM_NAME.matcher(team).matches()) {
            throw new IllegalArgumentException(
                    "team must be 2-24 characters of a-z, 0-9 and '-', starting with a letter, but was: " + team);
        }
        if (!kafka.containsKey("bootstrap.servers")) {
            throw new IllegalArgumentException("bootstrap.servers is missing");
        }
        this.team = team;
        this.kafka = kafka;
    }

    public static WorkshopConfig load() {
        Path file = Path.of(FILE_NAME);
        if (!Files.exists(file)) {
            throw new IllegalStateException("""
                    Could not find %s in %s.
                    Copy workshop.properties.example to workshop.properties and fill it in,
                    or paste the file you got when you joined the workshop."""
                    .formatted(FILE_NAME, Path.of("").toAbsolutePath()));
        }
        Properties all = new Properties();
        try (InputStream in = Files.newInputStream(file)) {
            all.load(in);
        } catch (IOException e) {
            throw new IllegalStateException("Could not read " + file, e);
        }
        String team = all.getProperty("team");
        Properties kafka = new Properties();
        all.stringPropertyNames().stream()
                .filter(key -> !key.equals("team") && !key.startsWith("workshop."))
                .forEach(key -> kafka.setProperty(key, all.getProperty(key).trim()));
        return new WorkshopConfig(team == null ? null : team.trim(), kafka);
    }

    public String team() {
        return team;
    }

    /** A fresh copy of the connection settings shared by every client. */
    public Properties kafka() {
        Properties copy = new Properties();
        copy.putAll(kafka);
        return copy;
    }
}
