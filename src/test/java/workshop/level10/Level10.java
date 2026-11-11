package workshop.level10;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import workshop.framework.Level;
import workshop.framework.Workshop;

/** Reference solution for level 10. */
public class Level10 implements Level {

    @Override
    public void run(Workshop ws) throws Exception {
        try (KafkaStreams streams = new KafkaStreams(topology(ws), ws.streamsProps())) {
            streams.start();
            ws.awaitStop();
        }
    }

    Topology topology(Workshop ws) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> orders = builder.stream(ws.inputTopic());
        KTable<String, String> customers = builder.table(ws.topic("customers"));
        orders.leftJoin(customers, Level10::enrich).to(ws.outputTopic());
        return builder.build();
    }

    static String enrich(String orderJson, String customerJson) {
        JsonNode order = parse(orderJson);
        JsonNode customer = customerJson == null ? null : parse(customerJson);
        String tier = customer == null ? "NEW" : customer.path("tier").asText();
        BigDecimal discount = switch (tier) {
            case "GOLD" -> new BigDecimal("0.10");
            case "SILVER" -> new BigDecimal("0.05");
            default -> BigDecimal.ZERO;
        };
        BigDecimal amount = order.path("amount").decimalValue();
        BigDecimal discounted = amount.subtract(amount.multiply(discount)).setScale(2, RoundingMode.HALF_UP);
        return Workshop.json().createObjectNode()
                .put("orderId", order.path("orderId").asText())
                .put("customerId", order.path("customerId").asText())
                .put("customerName", customer == null ? "unknown" : customer.path("name").asText())
                .put("tier", tier)
                .put("amount", amount)
                .put("discountedAmount", discounted)
                .toString();
    }

    private static JsonNode parse(String value) {
        try {
            return Workshop.json().readTree(value);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }
}
