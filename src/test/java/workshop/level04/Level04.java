package workshop.level04;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import workshop.framework.Level;
import workshop.framework.Workshop;

/** Reference solution for level 4. */
public class Level04 implements Level {

    @Override
    public void run(Workshop ws) throws Exception {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(ws.consumerProps());
             KafkaProducer<String, String> producer = new KafkaProducer<>(ws.producerProps())) {
            consumer.subscribe(List.of(ws.inputTopic()));
            while (ws.isRunning()) {
                for (ConsumerRecord<String, String> record : consumer.poll(Workshop.POLL)) {
                    Optional<ObjectNode> order = parse(record.value());
                    if (order.isPresent()) {
                        producer.send(new ProducerRecord<>(ws.outputTopic(),
                                order.get().get("orderId").asText(), order.get().toString()));
                    } else {
                        producer.send(new ProducerRecord<>(ws.topic("dlq"), record.key(), record.value()));
                    }
                }
            }
        }
    }

    static Optional<ObjectNode> parse(String value) {
        JsonNode node;
        try {
            node = Workshop.json().readTree(value);
        } catch (Exception e) {
            return Optional.empty();
        }
        if (node == null || !node.isObject()
                || !node.path("orderId").isTextual() || !node.path("sku").isTextual()
                || !node.path("quantity").isIntegralNumber() || !node.path("unitPrice").isNumber()) {
            return Optional.empty();
        }
        long quantity = node.get("quantity").asLong();
        BigDecimal unitPrice = node.get("unitPrice").decimalValue();
        if (quantity <= 0 || unitPrice.signum() < 0) {
            return Optional.empty();
        }
        ObjectNode result = ((ObjectNode) node).deepCopy();
        result.put("total", unitPrice.multiply(BigDecimal.valueOf(quantity)).setScale(2, RoundingMode.HALF_UP));
        return Optional.of(result);
    }
}
