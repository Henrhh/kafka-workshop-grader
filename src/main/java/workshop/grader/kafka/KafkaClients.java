package workshop.grader.kafka;

import jakarta.annotation.PreDestroy;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;
import workshop.grader.GraderProperties;

/** Builds the grader's Kafka clients. The admin client and producer are shared. */
@Component
public class KafkaClients {

    private final GraderProperties.Kafka config;
    private final Admin admin;
    private final KafkaProducer<String, String> producer;

    public KafkaClients(GraderProperties properties) {
        this.config = properties.kafka();
        this.admin = Admin.create(base());
        Properties producerProps = base();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "grader");
        this.producer = new KafkaProducer<>(producerProps);
    }

    public Admin admin() {
        return admin;
    }

    public KafkaProducer<String, String> producer() {
        return producer;
    }

    /** A new consumer with no group. Use assign(), not subscribe(). Close it when done. */
    public KafkaConsumer<String, String> newReader() {
        Properties props = base();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2000);
        return new KafkaConsumer<>(props);
    }

    /** A new consumer in {@code groupId}. Close it when done. */
    public KafkaConsumer<String, String> newGroupConsumer(String groupId) {
        Properties props = base();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // A new team's submit topic is found up to 5 seconds after its first submission was
        // written, so start new partitions from the beginning to not miss it.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Notice new teams' topics quickly when subscribed to a pattern.
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 5000);
        return new KafkaConsumer<>(props);
    }

    private Properties base() {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, config.securityProtocol());
        if (config.usesSasl()) {
            props.put(SaslConfigs.SASL_MECHANISM, config.saslMechanism());
            props.put(SaslConfigs.SASL_JAAS_CONFIG, Credentials.jaasConfig(config.adminUsername(), config.adminPassword()));
        }
        if (config.securityProtocol().endsWith("SSL") && !config.caCertificate().isBlank()) {
            props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
            props.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, config.caCertificate());
        }
        return props;
    }

    @PreDestroy
    void close() {
        producer.close();
        admin.close();
    }
}
