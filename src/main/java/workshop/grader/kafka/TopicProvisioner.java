package workshop.grader.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import workshop.grader.GraderProperties;

/** Creates topics, users and ACLs on the cluster. Every method is safe to call again. */
@Component
public class TopicProvisioner {

    private static final Logger log = LoggerFactory.getLogger(TopicProvisioner.class);

    private final Admin admin;
    private final GraderProperties.Kafka config;

    public TopicProvisioner(KafkaClients clients, GraderProperties properties) {
        this.admin = clients.admin();
        this.config = properties.kafka();
    }

    /** The feed topic. One partition, so everyone sees events in the same order. */
    public void ensureSharedTopics() {
        createIfMissing(List.of(new NewTopic(Topics.EVENTS, Optional.of(1), Optional.empty())));
    }

    /** Creates or updates a team's SCRAM user, if the cluster uses SASL. */
    public void upsertUser(String team, String password) {
        if (!config.usesSasl()) {
            return;
        }
        ScramMechanism mechanism = ScramMechanism.fromMechanismName(config.saslMechanism());
        var upsert = new UserScramCredentialUpsertion(Topics.principal(team),
                new ScramCredentialInfo(mechanism, 8192), password);
        await(admin.alterUserScramCredentials(List.of(upsert)).all());
    }

    /** Creates the team's topics and, if enabled, its ACLs. */
    public void ensureTeam(String team) {
        List<NewTopic> topics = new ArrayList<>();
        topics.add(new NewTopic(Topics.submitTopic(team), Optional.of(1), Optional.empty())
                .configs(Map.of("message.timestamp.type", "LogAppendTime")));
        for (int level : Topics.levels()) {
            for (Topics.Spec spec : Topics.forLevel(level)) {
                topics.add(new NewTopic(Topics.levelTopic(team, level, spec.name()),
                        Optional.of(spec.partitions()), Optional.empty()).configs(spec.configs()));
            }
        }
        createIfMissing(topics);
        if (config.manageAcls()) {
            await(admin.createAcls(aclsFor(team)).all());
        }
    }

    static List<AclBinding> aclsFor(String team) {
        String principal = "User:" + Topics.principal(team);
        String prefix = Topics.prefix(team);
        return List.of(
                // Everything under the team's own prefix: level topics, the submit topic and
                // the internal topics Kafka Streams creates.
                allow(new ResourcePattern(ResourceType.TOPIC, prefix, PatternType.PREFIXED), principal, AclOperation.ALL),
                allow(new ResourcePattern(ResourceType.GROUP, prefix, PatternType.PREFIXED), principal, AclOperation.ALL),
                // Read the live feed to see grading results.
                allow(new ResourcePattern(ResourceType.TOPIC, Topics.EVENTS, PatternType.LITERAL), principal, AclOperation.READ),
                allow(new ResourcePattern(ResourceType.TOPIC, Topics.EVENTS, PatternType.LITERAL), principal, AclOperation.DESCRIBE));
    }

    private static AclBinding allow(ResourcePattern pattern, String principal, AclOperation operation) {
        return new AclBinding(pattern, new AccessControlEntry(principal, "*", operation, AclPermissionType.ALLOW));
    }

    private void createIfMissing(List<NewTopic> topics) {
        Set<String> existing = await(admin.listTopics().names());
        List<NewTopic> missing = topics.stream().filter(t -> !existing.contains(t.name())).toList();
        if (missing.isEmpty()) {
            return;
        }
        var results = admin.createTopics(missing).values();
        for (var entry : results.entrySet()) {
            try {
                entry.getValue().get(60, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw new IllegalStateException("Could not create topic " + entry.getKey(), e.getCause());
                }
            } catch (Exception e) {
                throw new IllegalStateException("Could not create topic " + entry.getKey(), e);
            }
        }
        log.info("Created {} topics", missing.size());
    }

    private static <T> T await(org.apache.kafka.common.KafkaFuture<T> future) {
        try {
            return future.get(60, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause().getMessage(), e.getCause());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
