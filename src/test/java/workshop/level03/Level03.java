package workshop.level03;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import workshop.framework.Level;
import workshop.framework.Workshop;

/** Reference solution for level 3. */
public class Level03 implements Level {

    @Override
    public void run(Workshop ws) throws Exception {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(ws.consumerProps());
             KafkaProducer<String, String> producer = new KafkaProducer<>(ws.producerProps())) {
            consumer.subscribe(List.of(ws.inputTopic()));
            while (ws.isRunning()) {
                for (ConsumerRecord<String, String> record : consumer.poll(Workshop.POLL)) {
                    String userId = Workshop.json().readTree(record.value()).path("userId").asText();
                    producer.send(new ProducerRecord<>(ws.outputTopic(), userId, record.value()));
                }
            }
        }
    }
}
