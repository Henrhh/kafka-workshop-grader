package workshop.level09;

import java.time.Duration;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.TimeWindows;
import workshop.framework.Level;
import workshop.framework.Workshop;

/** Reference solution for level 9. */
public class Level09 implements Level {

    static final Duration WINDOW = Duration.ofMinutes(1);
    static final Duration GRACE = Duration.ofSeconds(30);

    @Override
    public void run(Workshop ws) throws Exception {
        try (KafkaStreams streams = new KafkaStreams(topology(ws), ws.streamsProps())) {
            streams.start();
            ws.awaitStop();
        }
    }

    Topology topology(Workshop ws) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(ws.inputTopic())
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeAndGrace(WINDOW, GRACE))
                .count()
                .toStream()
                .map((window, count) -> KeyValue.pair(window.key(), Workshop.json().createObjectNode()
                        .put("userId", window.key())
                        .put("windowStart", window.window().start())
                        .put("count", count)
                        .toString()))
                .to(ws.outputTopic());
        return builder.build();
    }
}
