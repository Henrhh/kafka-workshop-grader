package workshop.grader.events;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import tools.jackson.databind.json.JsonMapper;
import workshop.grader.kafka.KafkaClients;
import workshop.grader.kafka.TopicProvisioner;
import workshop.grader.kafka.Topics;

/**
 * Reads {@code workshop.events} from the start and pushes every event to the browsers that
 * have the feed open. Reading from offset 0 on startup rebuilds the recent history, so a
 * restarted grader shows the same feed.
 */
@Service
public class FeedService {

    private static final Logger log = LoggerFactory.getLogger(FeedService.class);
    private static final int HISTORY = 300;

    private final KafkaClients clients;
    private final TopicProvisioner provisioner;
    private final JsonMapper json;
    private final Deque<WorkshopEvent> history = new ArrayDeque<>();
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();
    private KafkaConsumer<String, String> consumer;
    private final ScheduledExecutorService heartbeat = Executors.newSingleThreadScheduledExecutor();
    private Thread thread;
    private volatile boolean running = true;

    public FeedService(KafkaClients clients, TopicProvisioner provisioner, JsonMapper json) {
        this.clients = clients;
        this.provisioner = provisioner;
        this.json = json;
    }

    @PostConstruct
    void start() {
        provisioner.ensureSharedTopics();
        consumer = clients.newReader();
        TopicPartition partition = new TopicPartition(Topics.EVENTS, 0);
        consumer.assign(List.of(partition));
        consumer.seekToBeginning(List.of(partition));
        thread = Thread.ofPlatform().name("feed").daemon().start(this::run);
        // Proxies such as Azure Container Apps' ingress drop idle connections.
        heartbeat.scheduleAtFixedRate(this::ping, 20, 20, TimeUnit.SECONDS);
    }

    private void ping() {
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().comment("ping"));
            } catch (IOException | IllegalStateException e) {
                emitters.remove(emitter);
            }
        }
    }

    private void run() {
        try {
            while (running) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    WorkshopEvent event;
                    try {
                        event = json.readValue(record.value(), WorkshopEvent.class);
                    } catch (Exception e) {
                        log.warn("Skipping unreadable event at offset {}", record.offset());
                        continue;
                    }
                    remember(event);
                    broadcast(event);
                }
            }
        } catch (WakeupException e) {
            // Shutting down.
        } finally {
            consumer.close();
        }
    }

    /** The most recent events, oldest first. */
    public List<WorkshopEvent> recent() {
        synchronized (history) {
            return new ArrayList<>(history);
        }
    }

    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(0L);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        emitters.add(emitter);
        try {
            // Sends the response headers now, so the browser knows it is connected.
            emitter.send(SseEmitter.event().comment("connected"));
        } catch (IOException e) {
            emitters.remove(emitter);
        }
        return emitter;
    }

    private void remember(WorkshopEvent event) {
        synchronized (history) {
            history.addLast(event);
            while (history.size() > HISTORY) {
                history.removeFirst();
            }
        }
    }

    private void broadcast(WorkshopEvent event) {
        String data = json.writeValueAsString(event);
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().name("event").data(data));
            } catch (IOException | IllegalStateException e) {
                emitters.remove(emitter);
            }
        }
    }

    @PreDestroy
    void stop() throws InterruptedException {
        running = false;
        heartbeat.shutdownNow();
        consumer.wakeup();
        thread.join(5000);
        emitters.forEach(SseEmitter::complete);
    }
}
