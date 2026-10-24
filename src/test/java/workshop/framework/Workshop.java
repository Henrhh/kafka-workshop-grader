package workshop.framework;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Everything your level needs: topic names and ready-made client settings.
 *
 * <p>Topics are named {@code team-<team>.levelNN.<name>}, for example
 * {@code team-falcon.level03.in}. Your consumer group and Streams application id is
 * {@code team-<team>.levelNN}.
 */
public final class Workshop {

    /** A good default timeout for {@code consumer.poll(...)}. */
    public static final Duration POLL = Duration.ofMillis(200);

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final DateTimeFormatter CLOCK = DateTimeFormatter.ofPattern("HH:mm:ss");

    private final WorkshopConfig config;
    private final int level;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private Path stateDir;

    public Workshop(WorkshopConfig config, int level) {
        this.config = config;
        this.level = level;
    }

    public String team() {
        return config.team();
    }

    public int level() {
        return level;
    }

    /** The full name of one of this level's topics, for example {@code topic("dlq")}. */
    public String topic(String name) {
        return "team-%s.level%02d.%s".formatted(team(), level, name);
    }

    /** The topic the grader writes input to. */
    public String inputTopic() {
        return topic("in");
    }

    /** The topic the grader reads your answers from. */
    public String outputTopic() {
        return topic("out");
    }

    /** Use this as your consumer group id. Streams uses it as the application id. */
    public String groupId() {
        return "team-%s.level%02d".formatted(team(), level);
    }

    /** Producer settings with String keys and values. */
    public Properties producerProps() {
        Properties props = config.kafka();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, groupId() + "-producer");
        return props;
    }

    /** Consumer settings with String keys and values, in group {@link #groupId()}. */
    public Properties consumerProps() {
        Properties props = config.kafka();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    /** Kafka Streams settings with String serdes and application id {@link #groupId()}. */
    public Properties streamsProps() {
        Properties props = config.kafka();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, groupId());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        // Send results quickly instead of buffering them for 30 seconds.
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        // Use the broker's default replication factor for internal topics.
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, -1);
        // A fresh state folder for every run, so each attempt starts from a clean slate.
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private synchronized Path stateDir() {
        if (stateDir == null) {
            try {
                stateDir = Files.createTempDirectory("kafka-streams-" + team() + "-");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return stateDir;
    }

    /** Shared Jackson mapper for reading and writing JSON values. */
    public static ObjectMapper json() {
        return JSON;
    }

    /** True until the grader has finished with your submission, or you press Ctrl+C. */
    public boolean isRunning() {
        return running.get();
    }

    /** Blocks until {@link #isRunning()} turns false. Handy for Kafka Streams levels. */
    public void awaitStop() throws InterruptedException {
        while (isRunning()) {
            Thread.sleep(100);
        }
    }

    public void stop() {
        running.set(false);
    }

    /** Prints a line prefixed with the time and level. */
    public void log(String format, Object... args) {
        System.out.printf("%s [level%02d] %s%n", LocalTime.now().format(CLOCK), level, format.formatted(args));
    }
}
