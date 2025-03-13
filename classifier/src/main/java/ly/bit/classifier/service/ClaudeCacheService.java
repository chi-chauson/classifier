package ly.bit.classifier.service;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import ly.bit.classifier.domain.EntityClass;
import ly.bit.classifier.repository.EntityRepository;
import org.redisson.api.RBatch;
import org.redisson.api.RedissonClient;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.client.RedisConnectionException;
import org.redisson.client.RedisTimeoutException;
import org.redisson.client.codec.Codec;
import org.redisson.codec.JsonJacksonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class ClaudeCacheService {

    private static final Logger log = LoggerFactory.getLogger(ClaudeCacheService.class);

    private final RedissonReactiveClient redissonReactiveClient;
    private final RedissonClient redissonClient; // Non-reactive client for batch operations
    private final EntityRepository entityRepository;
    private final MeterRegistry meterRegistry;
    private final CircuitBreaker circuitBreaker;

    // Reusable codec instance
    private final Codec codec = new JsonJacksonCodec();

    // Configurable TTL values
    @Value("${cache.default-ttl-minutes:10}")
    private int defaultTtlMinutes;

    @Value("${cache.tombstone-ttl-minutes:5}")
    private int tombstoneTtlMinutes;

    @Value("${cache.batch-size:20}")
    private int batchSize;

    public ClaudeCacheService(
            RedissonReactiveClient redissonReactiveClient,
            RedissonClient redissonClient,
            EntityRepository entityRepository,
            MeterRegistry meterRegistry) {

        this.redissonReactiveClient = redissonReactiveClient;
        this.redissonClient = redissonClient;
        this.entityRepository = entityRepository;
        this.meterRegistry = meterRegistry;

        // Initialize circuit breaker using the registry pattern
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();

        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("redisCircuitBreaker");

        // Initialize metrics
        initializeMetrics();
    }

    /**
     * Generic helper method that batches a list of keys into configurable batches,
     * queries Redis, and returns a Flux of individual key/value pairs.
     */
    public <T, V> Flux<AbstractMap.SimpleEntry<String, CacheResult<V>>> fetchBatchedBucketsGeneric(
            List<T> keys,
            Function<T, String> keyMapper,
            Class<V> clazz) {

        Timer.Sample batchTimer = Timer.start(meterRegistry);
        AtomicInteger batchCounter = new AtomicInteger(0);

        // Batch the keys
        return Flux.fromIterable(keys)
                .buffer(batchSize)
                .flatMap(batch -> {
                    int currentBatch = batchCounter.incrementAndGet();

                    // Map each key in the batch to its composite key
                    List<String> batchKeys = batch.stream()
                            .map(keyMapper)
                            .collect(Collectors.toList());

                    log.debug("Processing batch #{} of size {} with keys: {}",
                            currentBatch, batch.size(), batchKeys);

                    // Query Redis with circuit breaker
                    return redissonReactiveClient.getBuckets(codec)
                            .get(batchKeys.toArray(new String[0]))
                            .transform(CircuitBreakerOperator.of(circuitBreaker))
                            .doOnSuccess(result -> {
                                batchTimer.stop(meterRegistry.timer("cache.batch.duration"));
                                log.debug("Batch #{} completed successfully", currentBatch);
                                meterRegistry.counter("cache.batch.success").increment();
                            })
                            .doOnError(e -> log.warn("Batch #{} failed: {}", currentBatch, e.getMessage()))
                            .onErrorResume(RedisTimeoutException.class, e -> {
                                meterRegistry.counter("cache.redis.timeout").increment();
                                return Mono.empty();
                            })
                            .onErrorResume(RedisConnectionException.class, e -> {
                                meterRegistry.counter("cache.redis.connection_error").increment();
                                return Mono.empty();
                            })
                            .onErrorResume(e -> {
                                log.error("Critical Redis error in batch #{}: {}", currentBatch, e.getMessage(), e);
                                meterRegistry.counter("cache.redis.critical_error").increment();
                                return Mono.error(e); // Propagate critical errors
                            });
                })
                // Flatten each batch's map into a Flux of individual entries
                .flatMap(map -> Flux.fromIterable(map.entrySet()))
                // Convert entries to CacheResult objects
                .map(entry -> {
                    if (entry.getValue() == null) {
                        // Null value means key doesn't exist
                        return new AbstractMap.SimpleEntry<>(
                                entry.getKey(), CacheResult.<V>notFound());
                    } else if (entry.getValue() instanceof CacheResult) {
                        // Direct CacheResult (could be a tombstone)
                        @SuppressWarnings("unchecked")
                        CacheResult<V> result = (CacheResult<V>) entry.getValue();
                        return new AbstractMap.SimpleEntry<>(entry.getKey(), result);
                    } else {
                        // Normal value
                        return new AbstractMap.SimpleEntry<>(
                                entry.getKey(), CacheResult.found(clazz.cast(entry.getValue())));
                    }
                });
    }

    /**
     * Main method that fetches cached EntityClass objects, falls back to database
     * for missing keys, and merges results.
     */
    public Flux<EntityClass> getCacheOrFallbackToDbBatched(List<EntityClass> keys) {
        if (keys == null || keys.isEmpty()) {
            return Flux.empty();
        }

        Timer.Sample timer = Timer.start(meterRegistry);

        return fetchFromCache(keys)
                .switchIfEmpty(fetchAllFromDatabase(keys))
                .doOnTerminate(() -> {
                    timer.stop(meterRegistry.timer("cache.operation.duration"));
                })
                .onErrorResume(e -> {
                    log.error("Final operation error, falling back to database: {}", e.getMessage(), e);
                    meterRegistry.counter("cache.final_fallback").increment();
                    return fetchAllFromDatabase(keys);
                });
    }

    /**
     * Fetch entities from cache and handle cache results.
     */
    private Flux<EntityClass> fetchFromCache(List<EntityClass> keys) {
        // Use bounded elastic scheduler for I/O-bound operations
        return fetchBatchedBucketsGeneric(keys, this::compositeKey, EntityClass.class)
                .subscribeOn(Schedulers.boundedElastic())
                // Process the results stream directly
                .groupBy(entry -> {
                    CacheResult<EntityClass> result = entry.getValue();
                    if (result == null || !result.exists()) {
                        return "missing";
                    }
                    return "found";
                })
                .flatMap(group -> {
                    if ("missing".equals(group.key())) {
                        // Process missing keys batch
                        return group
                                .map(AbstractMap.SimpleEntry::getKey)
                                .collectList()
                                .flatMapMany(missingKeys -> {
                                    if (missingKeys.isEmpty()) {
                                        return Flux.empty();
                                    }

                                    log.info("Cache miss for {} keys", missingKeys.size());
                                    meterRegistry.counter("cache.miss").increment(missingKeys.size());

                                    // Find entities that correspond to the missing keys
                                    List<EntityClass> missingEntities = keys.stream()
                                            .filter(key -> missingKeys.contains(compositeKey(key)))
                                            .collect(Collectors.toList());

                                    return fetchMissingFromDatabase(missingEntities);
                                });
                    } else {
                        // Process found entries
                        return group
                                .map(entry -> {
                                    meterRegistry.counter("cache.hit").increment();
                                    return entry.getValue().getValue();
                                });
                    }
                });
    }

    /**
     * Fetch missing entities from database in a single batch query and cache them.
     */
    private Flux<EntityClass> fetchMissingFromDatabase(List<EntityClass> missingKeys) {
        if (missingKeys.isEmpty()) {
            return Flux.empty();
        }

        log.info("Fetching {} entities from database in a single batch", missingKeys.size());

        // Extract criteria for the batch query
        List<String> entityIds = missingKeys.stream().map(EntityClass::entityId).collect(Collectors.toList());
        List<String> idTypes = missingKeys.stream().map(EntityClass::idType).collect(Collectors.toList());
        List<String> schemes = missingKeys.stream().map(EntityClass::scheme).collect(Collectors.toList());

        // Perform a SINGLE batch database query
        Timer.Sample sample = Timer.start(meterRegistry);

        return entityRepository.findByEntityIdInAndIdTypeInAndSchemeIn(entityIds, idTypes, schemes)
                .doOnComplete(() -> {
                    sample.stop(meterRegistry.timer("db.batch.query"));
                    log.info("Batch database query completed");
                })
                .collectList()
                .flatMapMany(dbResults -> {
                    log.info("Database returned {} results for {} keys", dbResults.size(), missingKeys.size());

                    // Build map of found entities by their composite key
                    Map<String, EntityClass> foundMap = dbResults.stream()
                            .collect(Collectors.toMap(this::compositeKey, entity -> entity));

                    // Find which keys were not found in the database
                    List<EntityClass> notFoundEntities = missingKeys.stream()
                            .filter(key -> !foundMap.containsKey(compositeKey(key)))
                            .collect(Collectors.toList());

                    // Cache all results (both found and not found) in a single batch
                    cacheBatch(dbResults, foundMap, notFoundEntities);

                    // Return the database results immediately
                    return Flux.fromIterable(dbResults);
                });
    }

    /**
     * Fetch all entities from database directly (final fallback).
     */
    private Flux<EntityClass> fetchAllFromDatabase(List<EntityClass> keys) {
        log.info("Direct database fallback for {} keys", keys.size());
        meterRegistry.counter("cache.db.direct_fallback").increment();

        // Extract criteria for the batch query
        List<String> entityIds = keys.stream().map(EntityClass::entityId).collect(Collectors.toList());
        List<String> idTypes = keys.stream().map(EntityClass::idType).collect(Collectors.toList());
        List<String> schemes = keys.stream().map(EntityClass::scheme).collect(Collectors.toList());

        return entityRepository.findByEntityIdInAndIdTypeInAndSchemeIn(entityIds, idTypes, schemes);
    }

    /**
     * Batch cache all results and set tombstones for missing entities.
     */
    private void cacheBatch(
            List<EntityClass> foundEntities,
            Map<String, EntityClass> foundMap,
            List<EntityClass> notFoundEntities) {

        // Skip if nothing to cache
        if (foundEntities.isEmpty() && notFoundEntities.isEmpty()) {
            return;
        }

        RBatch batch = redissonClient.createBatch();

        // Cache found entities
        for (EntityClass entity : foundEntities) {
            String key = compositeKey(entity);
            batch.getBucket(key, codec)
                    .setAsync(CacheResult.found(entity), getTtlForEntity(entity));

            log.debug("Caching entity: {}", key);
        }

        // Set tombstones for not found entities
        for (EntityClass entity : notFoundEntities) {
            String key = compositeKey(entity);
            batch.getBucket(key, codec)
                    .setAsync(CacheResult.notFound(), Duration.ofMinutes(tombstoneTtlMinutes));

            log.debug("Setting tombstone for: {}", key);
        }

        // Execute batch asynchronously and handle results/errors
        batch.executeAsync().whenComplete((result, ex) -> {
            if (ex == null) {
                int totalCached = foundEntities.size() + notFoundEntities.size();
                log.info("Successfully cached {} entities ({} found, {} tombstones)",
                        totalCached, foundEntities.size(), notFoundEntities.size());
                meterRegistry.counter("cache.write.success").increment(totalCached);
            } else {
                log.error("Failed to cache batch: {}", ex.getMessage(), ex);
                meterRegistry.counter("cache.write.error").increment();
            }
        });
    }

    /**
     * Explicitly invalidate a cache entry.
     */
    public Mono<Boolean> invalidateCache(EntityClass entity) {
        String key = compositeKey(entity);
        log.info("Invalidating cache for key: {}", key);
        meterRegistry.counter("cache.invalidation").increment();

        return redissonReactiveClient.getBucket(key).delete()
                .doOnSuccess(result -> {
                    if (Boolean.TRUE.equals(result)) {
                        log.info("Successfully invalidated cache for key: {}", key);
                    } else {
                        log.info("Key not found in cache: {}", key);
                    }
                });
    }

    /**
     * Generate composite key from entity.
     */
    private String compositeKey(EntityClass entity) {
        return String.format("%s:%s:%s",
                entity.entityId(), entity.idType(), entity.scheme());
    }

    /**
     * Determine TTL based on entity type.
     */
    private Duration getTtlForEntity(EntityClass entity) {
        // Example strategy based on entity type
        switch (entity.idType()) {
            case "IMPORTANT":
                return Duration.ofMinutes(30); // Longer TTL for important entities
            case "VOLATILE":
                return Duration.ofMinutes(5); // Shorter TTL for frequently changing entities
            default:
                return Duration.ofMinutes(defaultTtlMinutes);
        }
    }

    /**
     * Initialize metrics counters and gauges.
     */
    private void initializeMetrics() {
        // Create counters
        Counter.builder("cache.hit.total")
                .description("Total number of cache hits")
                .register(meterRegistry);

        Counter.builder("cache.miss.total")
                .description("Total number of cache misses")
                .register(meterRegistry);

        // Add a gauge for hit ratio
        meterRegistry.gauge("cache.hit.ratio",
                meterRegistry.get("cache.hit.total").counter(),
                counter -> {
                    double hits = counter.count();
                    double misses = meterRegistry.get("cache.miss.total").counter().count();
                    return (hits + misses) == 0 ? 0 : (hits / (hits + misses));
                });
    }
}

/**
 * Generic cache result wrapper to properly handle tombstones.
 */
class CacheResult<T> {
    private final T value;
    private final boolean exists;

    private CacheResult(T value, boolean exists) {
        this.value = value;
        this.exists = exists;
    }

    public static <T> CacheResult<T> notFound() {
        return new CacheResult<>(null, false);
    }

    public static <T> CacheResult<T> found(T value) {
        return new CacheResult<>(value, true);
    }

    public boolean exists() {
        return exists;
    }

    public T getValue() {
        return value;
    }
}