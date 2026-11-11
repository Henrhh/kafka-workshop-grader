package workshop.level08;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Locale;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import workshop.framework.Level;
import workshop.framework.Workshop;

/** Reference solution for level 8. */
public class Level08 implements Level {

    @Override
    public void run(Workshop ws) throws Exception {
        try (KafkaStreams streams = new KafkaStreams(topology(ws), ws.streamsProps())) {
            streams.start();
            ws.awaitStop();
        }
    }

    Topology topology(Workshop ws) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> clicks = builder.stream(ws.inputTopic());
        clicks
                .mapValues(Level08::parse)
                .filter((key, click) -> !click.path("userAgent").asText().toLowerCase(Locale.ROOT).contains("bot"))
                .mapValues(click -> Workshop.json().createObjectNode()
                        .put("clickId", click.path("clickId").asText())
                        .put("userId", click.path("userId").asText())
                        .put("page", click.path("page").asText())
                        .put("seconds", click.path("durationMs").asLong() / 1000.0)
                        .toString())
                .split()
                .branch((key, value) -> page(value).startsWith("/checkout"),
                        Branched.withConsumer(s -> s.to(ws.topic("checkout"))))
                .branch((key, value) -> page(value).startsWith("/product"),
                        Branched.withConsumer(s -> s.to(ws.topic("product"))))
                .defaultBranch(Branched.withConsumer(s -> s.to(ws.topic("other"))));
        return builder.build();
    }

    private static JsonNode parse(String value) {
        try {
            return Workshop.json().readTree(value);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static String page(String value) {
        return parse(value).path("page").asText();
    }
}
