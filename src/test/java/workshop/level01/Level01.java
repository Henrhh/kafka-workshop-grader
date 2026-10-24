package workshop.level01;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import workshop.framework.Level;
import workshop.framework.Workshop;

/** Reference solution for level 1. */
public class Level01 implements Level {

    @Override
    public void run(Workshop ws) throws Exception {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(ws.producerProps())) {
            var metadata = producer.send(new ProducerRecord<>(ws.outputTopic(), ws.team(), "hello")).get();
            ws.log("Sent to partition %d at offset %d", metadata.partition(), metadata.offset());
        }
    }
}
