package workshop.level07;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import workshop.framework.Level;
import workshop.framework.Workshop;

/** Reference solution for level 7. */
public class Level07 implements Level {

    @Override
    public void run(Workshop ws) throws Exception {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(ws.consumerProps());
             KafkaProducer<String, String> producer = new KafkaProducer<>(ws.producerProps())) {
            consumer.subscribe(List.of(ws.inputTopic()));
            Set<String> seen = new HashSet<>();
            while (ws.isRunning()) {
                for (ConsumerRecord<String, String> record : consumer.poll(Workshop.POLL)) {
                    String eventId = Workshop.json().readTree(record.value()).get("eventId").asText();
                    if (seen.add(eventId)) {
                        producer.send(new ProducerRecord<>(ws.outputTopic(), eventId, record.value()));
                    }
                }
            }
        }
    }
}
