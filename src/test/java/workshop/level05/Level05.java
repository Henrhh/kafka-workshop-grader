package workshop.level05;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import workshop.framework.Level;
import workshop.framework.Workshop;

/** Reference solution for level 5. */
public class Level05 implements Level {

    private static final int CONSUMERS = 3;

    @Override
    public void run(Workshop ws) throws Exception {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(ws.producerProps())) {
            List<Thread> workers = new ArrayList<>();
            for (int i = 0; i < CONSUMERS; i++) {
                int id = i;
                workers.add(Thread.ofPlatform().name("worker-" + id).start(() -> work(ws, producer, id)));
            }
            for (Thread worker : workers) {
                worker.join();
            }
        }
    }

    private void work(Workshop ws, KafkaProducer<String, String> producer, int id) {
        Properties props = ws.consumerProps();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(ws.inputTopic()));
            while (ws.isRunning()) {
                var records = consumer.poll(Workshop.POLL);
                for (ConsumerRecord<String, String> record : records) {
                    producer.send(new ProducerRecord<>(ws.outputTopic(), record.key(),
                            record.value().toUpperCase(Locale.ROOT)));
                }
                if (!records.isEmpty()) {
                    producer.flush();
                    consumer.commitSync();
                }
            }
        }
    }
}
