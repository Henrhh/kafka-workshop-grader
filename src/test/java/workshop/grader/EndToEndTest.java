package workshop.grader;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.StringReader;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import workshop.framework.Level;
import workshop.framework.Levels;
import workshop.framework.Submit;
import workshop.framework.Workshop;
import workshop.framework.WorkshopConfig;
import workshop.grader.score.ScoreRepository;
import workshop.grader.team.TeamService;

/**
 * Runs the reference solutions through the real submit flow against the grader, with Kafka
 * and Postgres in Docker.
 */
@SpringBootTest
class EndToEndTest {

    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.4.0");
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");

    static {
        KAFKA.start();
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("grader.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    TeamService teams;

    @Autowired
    ScoreRepository scores;

    @Test
    void referenceSolutionsSolveEveryLevel() throws Exception {
        WorkshopConfig config = join("reference");
        for (int level = 1; level <= Levels.COUNT; level++) {
            assertThat(submit(config, level, Levels.create(level))).as("level %d", level).isTrue();
        }
        // A second attempt at a Streams level must not trip over state from the first.
        assertThat(submit(config, 9, Levels.create(9))).as("level 9 again").isTrue();

        var row = scores.leaderboard().stream().filter(r -> r.team().equals("reference")).findFirst().orElseThrow();
        assertThat(row.levelsSolved()).isEqualTo(10);
        assertThat(scores.solve("reference", 1).orElseThrow().rank()).isEqualTo(1);
    }

    @Test
    void aWrongAnswerFailsAndCostsPoints() throws Exception {
        WorkshopConfig config = join("wrong-answers");
        Level alwaysFortyTwo = ws -> {
            try (var producer = new KafkaProducer<String, String>(ws.producerProps())) {
                producer.send(new ProducerRecord<>(ws.outputTopic(), "42")).get();
            }
            try (var consumer = new KafkaConsumer<String, String>(ws.consumerProps())) {
                consumer.subscribe(List.of(ws.inputTopic()));
                while (ws.isRunning()) {
                    consumer.poll(Workshop.POLL);
                }
            }
        };
        assertThat(submit(config, 2, alwaysFortyTwo)).isFalse();
        assertThat(submit(config, 2, Levels.create(2))).isTrue();
        assertThat(scores.solve("wrong-answers", 2).orElseThrow().failedAttempts()).isEqualTo(1);
    }

    private WorkshopConfig join(String team) throws Exception {
        Properties file = new Properties();
        file.load(new StringReader(teams.register(team)));
        Properties kafka = new Properties();
        file.stringPropertyNames().stream()
                .filter(key -> !key.equals("team"))
                .forEach(key -> kafka.setProperty(key, file.getProperty(key)));
        return new WorkshopConfig(file.getProperty("team"), kafka);
    }

    private static boolean submit(WorkshopConfig config, int level, Level solution) throws Exception {
        return new Submit().submit(new Workshop(config, level), config, solution);
    }
}
