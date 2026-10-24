package workshop.level02;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import workshop.framework.Level;
import workshop.framework.Workshop;

/** Reference solution for level 2. */
public class Level02 implements Level {

    @Override
    public void run(Workshop ws) throws Exception {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(ws.consumerProps());
             KafkaProducer<String, String> producer = new KafkaProducer<>(ws.producerProps())) {
            consumer.subscribe(List.of(ws.inputTopic()));
            long sum = 0;
            while (ws.isRunning()) {
                var records = consumer.poll(Workshop.POLL);
                for (ConsumerRecord<String, String> record : records) {
                    sum += Long.parseLong(record.value().trim());
                }
                if (!records.isEmpty()) {
                    producer.send(new ProducerRecord<>(ws.outputTopic(), Long.toString(sum)));
                }
            }
        }
    }
}
