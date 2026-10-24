package workshop.framework;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Submits a level: asks the grader for a new attempt, runs your solution while the grader
 * tests it, and prints the grader's verdict as it arrives.
 */
public final class Submit {

    static final String EVENTS_TOPIC = "workshop.events";
    private static final Duration PICKUP_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration ATTEMPT_TIMEOUT = Duration.ofMinutes(4);

    public static void main(String[] args) throws Exception {
        int levelNumber;
        WorkshopConfig config;
        try {
            levelNumber = Levels.parse(args.length > 0 ? args[0] : null);
            config = WorkshopConfig.load();
        } catch (RuntimeException e) {
            System.err.println(e.getMessage());
            System.exit(2);
            return;
        }
        Workshop ws = new Workshop(config, levelNumber);
        Level level = Levels.create(levelNumber);
        boolean solved = new Submit().submit(ws, config, level);
        System.exit(solved ? 0 : 1);
    }

    public boolean submit(Workshop ws, WorkshopConfig config, Level level) throws Exception {
        String attemptId = UUID.randomUUID().toString();
        try (KafkaConsumer<String, String> events = eventsConsumer(config)) {
            // Start reading the feed before we submit, so we cannot miss our own results.
            List<TopicPartition> partitions = events.partitionsFor(EVENTS_TOPIC, Duration.ofSeconds(30)).stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition()))
                    .toList();
            events.assign(partitions);
            events.seekToEnd(partitions);
            partitions.forEach(events::position);

            sendSubmission(ws, attemptId);
            ws.log("Submitted level %d as team %s (attempt %s)", ws.level(), ws.team(), attemptId.substring(0, 8));

            Thread solution = Thread.ofPlatform().name("level-" + ws.level()).start(() -> {
                try {
                    level.run(ws);
                } catch (Exception e) {
                    ws.log("Your solution threw an exception:");
                    e.printStackTrace(System.out);
                }
            });

            boolean solved = followAttempt(ws, events, attemptId);
            ws.stop();
            solution.join(TimeUnit.SECONDS.toMillis(15));
            if (solution.isAlive()) {
                ws.log("Your solution did not stop within 15 seconds after isRunning() turned false.");
            }
            return solved;
        }
    }

    private void sendSubmission(Workshop ws, String attemptId) throws Exception {
        ObjectNode value = Workshop.json().createObjectNode()
                .put("attemptId", attemptId)
                .put("team", ws.team())
                .put("level", ws.level())
                .put("submittedAt", Instant.now().toString());
        try (var producer = new KafkaProducer<String, String>(ws.producerProps())) {
            String topic = "team-%s.submit".formatted(ws.team());
            producer.send(new ProducerRecord<>(topic, String.valueOf(ws.level()), value.toString()))
                    .get(30, TimeUnit.SECONDS);
        }
    }

    private boolean followAttempt(Workshop ws, KafkaConsumer<String, String> events, String attemptId)
            throws Exception {
        Instant submitted = Instant.now();
        boolean started = false;
        String lastFailure = null;
        while (true) {
            Duration waited = Duration.between(submitted, Instant.now());
            if (!started && waited.compareTo(PICKUP_TIMEOUT) > 0) {
                ws.log("The grader has not picked up the submission after %ds. Is it running? Ask a facilitator.",
                        PICKUP_TIMEOUT.toSeconds());
                return false;
            }
            if (waited.compareTo(ATTEMPT_TIMEOUT) > 0) {
                ws.log("Gave up waiting for the grader after %d minutes.", ATTEMPT_TIMEOUT.toMinutes());
                return false;
            }
            for (ConsumerRecord<String, String> record : events.poll(Duration.ofMillis(500))) {
                JsonNode event;
                try {
                    event = Workshop.json().readTree(record.value());
                } catch (Exception e) {
                    continue;
                }
                if (!attemptId.equals(event.path("attemptId").asText())) {
                    continue;
                }
                String type = event.path("type").asText();
                String message = event.path("message").asText();
                switch (type) {
                    case "ATTEMPT_STARTED" -> {
                        started = true;
                        ws.log("Grader: %s", message);
                    }
                    case "CHECK_PASSED" -> ws.log("  PASS  %s", message);
                    case "CHECK_FAILED" -> {
                        lastFailure = message;
                        ws.log("  FAIL  %s", message);
                    }
                    case "SOLVED" -> {
                        ws.log("SOLVED! %s", message);
                        return true;
                    }
                    case "FAILED" -> {
                        if (message.equals(lastFailure)) {
                            ws.log("Not yet. Fix that and submit again.");
                        } else {
                            ws.log("Not yet: %s", message);
                        }
                        return false;
                    }
                    default -> ws.log("Grader: %s", message);
                }
            }
        }
    }

    private static KafkaConsumer<String, String> eventsConsumer(WorkshopConfig config) {
        Properties props = config.kafka();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new KafkaConsumer<>(props);
    }
}
