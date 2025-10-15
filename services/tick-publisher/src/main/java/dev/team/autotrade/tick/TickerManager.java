package dev.team.autotrade.tick;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class TickerManager {
    private static final Logger log = LoggerFactory.getLogger(TickerManager.class);

    private final ObjectMapper om = new ObjectMapper();
    private final KafkaTemplate<String, String> kafka;

    @Value("${app.desireTopic}") private String desireTopic;
    @Value("${app.tickTopic}")   private String tickTopic;

    // TTL for subscriptions and emit period
    @Value("${app.ttlSeconds:30}") private long ttlSeconds;
    @Value("${app.periodMs:200}")   private long periodMs;

    // Number of shard workers (K). 4–16 is a good start for 1k–10k symbols.
    @Value("${app.workers:8}") private int workers;

    // symbol -> (sessionId -> lastSeenMs)
    private final ConcurrentMap<String, ConcurrentMap<String, Long>> active = new ConcurrentHashMap<>();
    // per-symbol price state (no per-symbol thread anymore)
    private final ConcurrentMap<String, AtomicReference<Double>> price = new ConcurrentHashMap<>();

    private ScheduledExecutorService loopPool;

    public TickerManager(KafkaTemplate<String, String> kafka) { this.kafka = kafka; }

    // --- ingest desires ------------------------------------------------------

    @KafkaListener(topics = "${app.desireTopic}", groupId = "tick-desire-watchers")
    public void onDesired(ConsumerRecord<String, String> rec, Acknowledgment ack) {
        try {
            log.info("desired key={} value={}", rec.key(), rec.value());
            DesiredEvent ev = om.readValue(rec.value(), DesiredEvent.class);
            if (ev.symbol == null || ev.sessionId == null) return;

            String sym = ev.symbol.trim();
            active.computeIfAbsent(sym, s -> new ConcurrentHashMap<>());
            var map = active.get(sym);

            if ("SUB".equalsIgnoreCase(ev.op)) {
                // use server time so bad client clocks don’t make it stale
                map.put(ev.sessionId, Instant.now().toEpochMilli());
            } else if ("UNSUB".equalsIgnoreCase(ev.op)) {
                map.remove(ev.sessionId);
            }
        } catch (Exception e) {
            log.warn("bad desire json: {}", rec.value(), e);
        } finally {
            ack.acknowledge();
        }
    }

    // --- coalescing shard loops ----------------------------------------------

    @PostConstruct
    public void startLoops() {
        loopPool = Executors.newScheduledThreadPool(workers, r -> {
            Thread t = new Thread(r, "tick-loop");
            t.setDaemon(true);
            return t;
        });
        for (int shard = 0; shard < workers; shard++) {
            final int shardId = shard;
            loopPool.scheduleAtFixedRate(() -> runShard(shardId), 0, periodMs, TimeUnit.MILLISECONDS);
        }
        log.info("Started {} shard workers (period {} ms).", workers, periodMs);
    }

    private void runShard(int shardId) {
        long now = Instant.now().toEpochMilli();
        long ttlMs = ttlSeconds * 1000;

        // Iterate only the symbols belonging to this shard
        for (Map.Entry<String, ConcurrentMap<String, Long>> e : active.entrySet()) {
            String sym = e.getKey();
            if ((hash(sym) % workers) != shardId) continue; // not our shard

            var sessions = e.getValue();

            // prune stale sessions for this symbol
            sessions.entrySet().removeIf(s -> (now - s.getValue()) > ttlMs);

            if (sessions.isEmpty()) {
                // optional: drop symbol entirely to keep the map lean
                active.remove(sym, sessions);
                continue;
            }

            // update price (random walk)
            AtomicReference<Double> ref = price.computeIfAbsent(sym,
                    s -> new AtomicReference<>(100.0 + Math.random() * 20.0));
            double next = ref.get() + (Math.random() - 0.5) * 0.2;
            ref.set(next);

            // emit tick
            try {
                Tick tick = new Tick(sym, now, next, 1);
                String json = om.writeValueAsString(tick);
                kafka.send(tickTopic, sym, json);
                // (Key = symbol -> Kafka keeps all ticks for a symbol in the same partition)
            } catch (Exception ex) {
                log.warn("publish failed for {}", sym, ex);
            }
        }
    }

    // Keep a lightweight, periodic log to see activity
    @Scheduled(fixedDelay = 5000)
    public void heartbeatLog() {
        log.debug("activeSymbols={}, mapSize={}", active.size(), price.size());
    }

    private static int hash(String s) {
        // stable non-negative hash for shard routing
        return (s.hashCode() & 0x7fffffff);
    }

    @PreDestroy
    public void shutdown() {
        if (loopPool != null) loopPool.shutdownNow();
        log.info("TickerManager shutdown complete.");
    }
}
