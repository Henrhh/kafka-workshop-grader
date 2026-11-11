package workshop.grader.grading;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import tools.jackson.databind.json.JsonMapper;
import workshop.grader.events.EventPublisher;
import workshop.grader.events.WorkshopEvent;
import workshop.grader.kafka.KafkaClients;
import workshop.grader.kafka.Topics;

/** Everything a {@link LevelGrader} needs for one attempt by one team. */
public final class AttemptContext {

    private final String team;
    private final int level;
    private final String attemptId;
    private final Instant submittedAt;
    private final Random random;
    private final KafkaClients clients;
    private final EventPublisher events;
    private final JsonMapper json;
    private int bonus;

    AttemptContext(String team, int level, String attemptId, Instant submittedAt, long seed, KafkaClients clients,
            EventPublisher events, JsonMapper json) {
        this.team = team;
        this.level = level;
        this.attemptId = attemptId;
        this.submittedAt = submittedAt;
        this.random = new Random(seed);
        this.clients = clients;
        this.events = events;
        this.json = json;
    }

    public String team() {
        return team;
    }

    public int level() {
        return level;
    }

    /** Seeded per attempt, so every attempt gets different data. */
    public Random random() {
        return random;
    }

    public JsonMapper json() {
        return json;
    }

    public String topic(String name) {
        return Topics.levelTopic(team, level, name);
    }

    public String groupId() {
        return Topics.groupId(team, level);
    }

    /** Extra points on top of the level's score, for example a speed bonus. */
    public void addBonus(int points) {
        bonus += points;
    }

    int bonus() {
        return bonus;
    }

    /** Tells the team a check passed. */
    public void pass(String message) {
        events.publish(WorkshopEvent.Type.CHECK_PASSED, team, level, attemptId, message, null);
    }

    /** Passes or fails a check. A failed check ends the attempt. */
    public void check(boolean ok, String passMessage, String failMessage) {
        if (ok) {
            pass(passMessage);
        } else {
            events.publish(WorkshopEvent.Type.CHECK_FAILED, team, level, attemptId, failMessage, null);
            throw new CheckFailed(failMessage);
        }
    }

    /** Fails the attempt with {@code failMessage} unless {@code ok}. Says nothing when it passes. */
    public void require(boolean ok, String failMessage) {
        if (!ok) {
            check(false, "", failMessage);
        }
    }

    /** Deletes all records in the given topics of this level, so old attempts cannot leak in. */
    public void purge(String... names) throws Exception {
        purgeTopics(Arrays.stream(names).map(this::topic).toList());
    }

    private void purgeTopics(List<String> topics) throws Exception {
        if (topics.isEmpty()) {
            return;
        }
        Admin admin = clients.admin();
        Map<TopicPartition, OffsetSpec> query = new HashMap<>();
        admin.describeTopics(topics).allTopicNames().get(30, TimeUnit.SECONDS).values()
                .forEach(d -> d.partitions().forEach(p ->
                        query.put(new TopicPartition(d.name(), p.partition()), OffsetSpec.latest())));
        Map<TopicPartition, RecordsToDelete> delete = new HashMap<>();
        admin.listOffsets(query).all().get(30, TimeUnit.SECONDS)
                .forEach((tp, info) -> delete.put(tp, RecordsToDelete.beforeOffset(info.offset())));
        admin.deleteRecords(delete).all().get(30, TimeUnit.SECONDS);
    }

    /**
     * Empties the internal topics Kafka Streams made for this level, such as window store
     * changelogs, so a restarted application does not restore state from an earlier attempt.
     * Compacted-only topics cannot be emptied and are skipped.
     */
    public void purgeInternalTopics() throws Exception {
        Admin admin = clients.admin();
        String prefix = groupId() + "-";
        List<String> internal = admin.listTopics().names().get(30, TimeUnit.SECONDS).stream()
                .filter(name -> name.startsWith(prefix))
                .toList();
        if (internal.isEmpty()) {
            return;
        }
        List<ConfigResource> resources = internal.stream()
                .map(name -> new ConfigResource(ConfigResource.Type.TOPIC, name))
                .toList();
        List<String> deletable = admin.describeConfigs(resources).all().get(30, TimeUnit.SECONDS).entrySet().stream()
                .filter(e -> {
                    var policy = e.getValue().get(TopicConfig.CLEANUP_POLICY_CONFIG);
                    return policy != null && policy.value().contains(TopicConfig.CLEANUP_POLICY_DELETE);
                })
                .map(e -> e.getKey().name())
                .toList();
        purgeTopics(deletable);
    }

    /**
     * Waits until the team's consumer group, or Streams application, is stable with at least
     * {@code minMembers} members that each own a partition, then passes a check.
     */
    public void awaitGroup(int minMembers, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        int seen = 0;
        while (Instant.now().isBefore(deadline)) {
            ConsumerGroupDescription group = describeGroup();
            if (group != null) {
                int withPartitions = (int) group.members().stream()
                        .filter(m -> !m.assignment().topicPartitions().isEmpty())
                        .count();
                seen = Math.max(seen, withPartitions);
                if (group.groupState() == GroupState.STABLE && withPartitions >= minMembers) {
                    pass(minMembers == 1
                            ? "Your consumer joined group " + groupId()
                            : "Group %s has %d members with partitions".formatted(groupId(), withPartitions));
                    return;
                }
            }
            Thread.sleep(500);
        }
        check(false, "", seen == 0
                ? "No consumer joined group %s within %ds. Is your code subscribing to %s with ws.consumerProps()?"
                        .formatted(groupId(), timeout.toSeconds(), topic("in"))
                : "Group %s needs %d members that each own a partition, but had at most %d"
                        .formatted(groupId(), minMembers, seen));
    }

    /** True if the group still has members. Used to spot a consumer that crashed. */
    public boolean groupAlive() throws Exception {
        ConsumerGroupDescription group = describeGroup();
        return group != null && !group.members().isEmpty();
    }

    private ConsumerGroupDescription describeGroup() throws Exception {
        try {
            return clients.admin().describeConsumerGroups(List.of(groupId())).all()
                    .get(15, TimeUnit.SECONDS).get(groupId());
        } catch (ExecutionException e) {
            if (e.getCause() instanceof GroupIdNotFoundException) {
                return null;
            }
            throw e;
        }
    }

    public Map<TopicPartition, Long> committedOffsets() throws Exception {
        Map<TopicPartition, OffsetAndMetadata> committed = clients.admin().listConsumerGroupOffsets(groupId())
                .partitionsToOffsetAndMetadata().get(15, TimeUnit.SECONDS);
        Map<TopicPartition, Long> result = new HashMap<>();
        committed.forEach((tp, offset) -> {
            if (offset != null) {
                result.put(tp, offset.offset());
            }
        });
        return result;
    }

    public Map<TopicPartition, Long> endOffsets(String name) throws Exception {
        String topic = topic(name);
        Map<TopicPartition, OffsetSpec> query = new HashMap<>();
        clients.admin().describeTopics(List.of(topic)).allTopicNames().get(15, TimeUnit.SECONDS).get(topic)
                .partitions().forEach(p -> query.put(new TopicPartition(topic, p.partition()), OffsetSpec.latest()));
        Map<TopicPartition, Long> result = new HashMap<>();
        clients.admin().listOffsets(query).all().get(15, TimeUnit.SECONDS)
                .forEach((tp, info) -> result.put(tp, info.offset()));
        return result;
    }

    public void send(String name, String key, String value) {
        clients.producer().send(new ProducerRecord<>(topic(name), key, value));
    }

    public void send(String name, String key, String value, long timestamp) {
        clients.producer().send(new ProducerRecord<>(topic(name), null, timestamp, key, value));
    }

    public void flush() {
        clients.producer().flush();
    }

    /** Reads only records written to these topics of this level from now on. */
    public OutputReader readFromNow(String... names) {
        return OutputReader.fromEnd(clients.newReader(), Arrays.stream(names).map(this::topic).toList());
    }

    /** Reads records written to these topics since the team submitted. Needs LogAppendTime topics. */
    public OutputReader readSinceSubmission(String... names) {
        return OutputReader.fromTime(clients.newReader(), Arrays.stream(names).map(this::topic).toList(),
                submittedAt);
    }
}
