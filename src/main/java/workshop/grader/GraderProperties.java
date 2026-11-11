package workshop.grader;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Settings under {@code grader.*}. See application.yml for what each one does.
 */
@ConfigurationProperties("grader")
public record GraderProperties(
        Kafka kafka,
        @DefaultValue("") String adminToken,
        @DefaultValue("") String joinCode,
        @DefaultValue("8") int maxParallelAttempts) {

    public record Kafka(
            String bootstrapServers,
            @DefaultValue("") String publicBootstrapServers,
            @DefaultValue("PLAINTEXT") String securityProtocol,
            @DefaultValue("SCRAM-SHA-512") String saslMechanism,
            @DefaultValue("") String adminUsername,
            @DefaultValue("") String adminPassword,
            @DefaultValue("") String caCertificate,
            @DefaultValue("false") boolean manageAcls) {

        public boolean usesSasl() {
            return securityProtocol.startsWith("SASL");
        }

        /** What teams put in bootstrap.servers. Defaults to the grader's own address. */
        public String bootstrapForTeams() {
            return publicBootstrapServers.isBlank() ? bootstrapServers : publicBootstrapServers;
        }
    }
}
