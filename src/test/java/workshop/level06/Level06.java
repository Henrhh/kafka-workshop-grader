package workshop.level06;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import workshop.framework.Level;
import workshop.framework.Workshop;

/** Reference solution for level 6. */
public class Level06 implements Level {

    @Override
    public void run(Workshop ws) throws Exception {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(ws.consumerProps());
             KafkaProducer<String, String> producer = new KafkaProducer<>(ws.producerProps())) {
            consumer.subscribe(List.of(ws.inputTopic()));
            Map<String, Long> balances = new HashMap<>();
            while (ws.isRunning()) {
                for (ConsumerRecord<String, String> record : consumer.poll(Workshop.POLL)) {
                    JsonNode tx = Workshop.json().readTree(record.value());
                    String account = tx.get("accountId").asText();
                    long amount = tx.get("amount").asLong();
                    long balance = balances.getOrDefault(account, 0L);
                    boolean accepted = true;
                    if (tx.get("type").asText().equals("DEPOSIT")) {
                        balance += amount;
                    } else if (amount <= balance) {
                        balance -= amount;
                    } else {
                        accepted = false;
                    }
                    balances.put(account, balance);
                    var result = Workshop.json().createObjectNode()
                            .put("txId", tx.get("txId").asText())
                            .put("accountId", account)
                            .put("accepted", accepted)
                            .put("balance", balance);
                    producer.send(new ProducerRecord<>(ws.outputTopic(), account, result.toString()));
                }
            }
        }
    }
}
