package ly.bit.classifier.service;

import ly.bit.classifier.domain.CacheTuple;
import ly.bit.classifier.domain.EntityClass;
import ly.bit.classifier.repository.EntityRepository;
import org.redisson.api.RBatchReactive;
import org.redisson.api.RedissonReactiveClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class CacheService {
    private static final Logger log = LoggerFactory.getLogger(CacheService.class);

    // RedissonReactiveClient for Redis operations.
    private final RedissonReactiveClient redissonReactiveClient;
    // Repository for database queries via Spring Data R2DBC.
    private final EntityRepository entityRepository;

    // A special marker (tombstone) to represent a key that is known not to exist in the DB.
    private final EntityClass TOMBSTONE = new EntityClass("TOMBSTONE", "TOMBSTONE", "TOMBSTONE", "TOMBSTONE");

    public CacheService(RedissonReactiveClient redissonReactiveClient, EntityRepository entityRepository) {
        this.redissonReactiveClient = redissonReactiveClient;
        this.entityRepository = entityRepository;
    }

    private static class ErrorMarker {
        // This class is used as a marker to indicate an error
    }

    /**
     * Generic helper method that batches a list of keys (of type T) into batches of 20,
     * queries Redis using the provided keyMapper, and returns the results as a Flux of CacheTuple.
     *
     * @param keys      List of keys of type V to retrieve from the cache.
     * @param keyMapper Function that maps each key to a String composite key used in Redis.
     * @param <V>       The type of both the keys and the values stored in Redis.
     * @return Flux emitting individual key/value pairs wrapped in CacheTuple objects.
     */
    public <V> Flux<CacheTuple<V>> getManyKeys(
            List<V> keys,
            Function<V, String> keyMapper) {

        return Flux.fromIterable(keys)
                .buffer(20)
                // For each batch, compute its composite keys.
                .flatMap(batch -> {
                    List<String> batchKeys = batch.stream()
                            .map(keyMapper)
                            .collect(Collectors.toList());

                    log.info("Processing batch of size {} with keys: {}", batch.size(), batchKeys);
                    // Query Redis for the current batch.
                    return redissonReactiveClient.getBuckets()
                            .get(batchKeys.toArray(new String[0]))
                            .onErrorResume(e -> {
                                log.error("Redis batch failed: {}", e.getMessage());
                                return Mono.empty();
                            });
                })
                // Flatten each batch's returned map (Map<String, Object>) into a Flux of its entries.
                .flatMap(map -> Flux.fromIterable(map.entrySet()))
                // Cast each entry's value to the desired type V and wrap it in a SimpleEntry.
                .map(entry -> {
                    Object value = entry.getValue();
                    boolean isError = value instanceof ErrorMarker;

                    @SuppressWarnings("unchecked")
                    V typedValue = isError ? null : (V) value;

                    return new CacheTuple<>(entry.getKey(), isError ? null : typedValue, isError);
                });
    }

    /**
     * Sets multiple key-value pairs in Redis using batched operations for improved performance.
     * Keys are processed in batches of the specified size to optimize Redis operations.
     *
     * @param keysToValues Map containing the key-value pairs to store in Redis.
     * @param ttl          The time-to-live duration for the cached entries.
     * @param batchSize    The maximum number of operations to include in each Redis batch.
     * @param <V>          The type of values to be stored.
     * @return A Mono<Void> that completes when all batch operations are finished.
     */
    private <V> Mono<Void> setManyKeys(Map<String, V> keysToValues, Duration ttl, int batchSize) {
        if (keysToValues == null || keysToValues.isEmpty()) {
            return Mono.empty();
        }

        List<List<Map.Entry<String, V>>> batches = new ArrayList<>();
        List<Map.Entry<String, V>> entries = new ArrayList<>(keysToValues.entrySet());

        for (int i = 0; i < entries.size(); i += batchSize) {
            batches.add(entries.subList(i, Math.min(i + batchSize, entries.size())));
        }

        return Flux.fromIterable(batches)
                .flatMap(batchEntries -> {
                    RBatchReactive redisBatch = redissonReactiveClient.createBatch();

                    for (Map.Entry<String, V> entry : batchEntries) {
                        redisBatch.getBucket(entry.getKey()).set(entry.getValue(), ttl);
                    }

                    return redisBatch.execute()
                            .doOnNext(result -> log.info("Executed batch of size {}", batchEntries.size()))
                            .onErrorResume(throwable -> {
                                log.error("Failed batch set operation in cache: ", throwable);
                                return Mono.empty();
                            });

                }).then();
    }


    /**
     * Main method that retrieves entities from cache with fallback to database.
     * The method first attempts to retrieve all requested entities from Redis cache,
     * then falls back to the database for any missing keys. It also maintains cache
     * consistency by setting tombstones for keys not found in the database and caching
     * database results for future use.
     *
     * @param keys List of EntityClass objects representing the requested entities.
     * @return A Flux emitting unique EntityClass results, either from cache or database.
     */
    public Flux<EntityClass> getCacheOrFallbackToDbBatched(List<EntityClass> keys) {
        // Use the generic helper to get a typed aggregated map from Redis.
        return getManyKeys(keys, this::compositeKey)
                .subscribeOn(Schedulers.parallel())
                .filter(tuple -> !tuple.error())
                .collectMap(CacheTuple::key, CacheTuple::value)
                .doOnNext(aggregatedMap -> log.info("Aggregated Redis map has {} entries", aggregatedMap.size()))
                .flatMapMany(aggregatedMap -> {
                    // Build a map of cached (non-tombstone) results.
                    Map<String, EntityClass> allCached = aggregatedMap.entrySet().stream()
                            .filter(entry -> !isTombstone(entry.getValue()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    log.info("All cached entries (non-tombstone): {}", allCached.keySet());

                    // Determine missing keys by comparing the original keys (via compositeKey) with aggregatedMap.
                    List<EntityClass> missingKeys = keys.stream()
                            .filter(entity -> !aggregatedMap.containsKey(compositeKey(entity)))
                            .collect(Collectors.toList());
                    log.info("Missing keys count: {}", missingKeys.size());

                    if (missingKeys.isEmpty()) {
                        log.info("No missing keys. Returning cached results.");
                        return Flux.fromIterable(allCached.values());
                    }

                    // Query the DB once for the union of missing keys.
                    return getFromDatabase(missingKeys)
                            .collectList()
                            .flatMapMany(dbResults -> {
                                log.info("Database returned {} results for missing keys.", dbResults.size());
                                Set<String> retrievedKeys = dbResults.stream()
                                        .map(this::compositeKey)
                                        .collect(Collectors.toSet());

                                // Identify keys still missing in DB.
                                List<EntityClass> stillMissingKeys = missingKeys.stream()
                                        .filter(entity -> !retrievedKeys.contains(compositeKey(entity)))
                                        .toList();
                                log.info("Keys still missing after DB query: {}",
                                        stillMissingKeys.stream().map(this::compositeKey).collect(Collectors.toList()));

                                // For keys still missing, set tombstones in batch
                                if (!stillMissingKeys.isEmpty()) {
                                    Map<String, EntityClass> tombstonesMap = stillMissingKeys.stream()
                                            .collect(Collectors.toMap(
                                                    this::compositeKey,
                                                    missingKey -> TOMBSTONE
                                            ));
                                    int batchSize = 20;
                                    log.info("Setting tombstones for missing keys: {}, batch size: {}", tombstonesMap.keySet(), batchSize);
                                    setManyKeys(tombstonesMap, Duration.ofMinutes(10), batchSize)
                                            .doFinally(signalType -> log.info("Tombstone batch caching completed with signal: {}", signalType))
                                            .subscribeOn(Schedulers.boundedElastic())
                                            .subscribe();
                                }

                                // Cache the DB results in batch
                                if (!dbResults.isEmpty()) {
                                    Map<String, EntityClass> dbResultsMap = dbResults.stream()
                                            .collect(Collectors.toMap(
                                                    this::compositeKey,
                                                    entity -> entity
                                            ));
                                    int batchSize = 20;
                                    log.info("Caching DB results: {}, batch size: {}", dbResultsMap.keySet(), batchSize);
                                    setManyKeys(dbResultsMap, Duration.ofMinutes(10), batchSize)
                                            .doFinally(signalType -> log.info("DB results batch caching completed with signal: {}", signalType))
                                            .subscribeOn(Schedulers.boundedElastic())
                                            .subscribe();
                                }

                                // Merge the DB results with cached results and ensure uniqueness.
                                return Flux.fromIterable(dbResults)
                                        .concatWith(Flux.fromIterable(allCached.values()))
                                        .distinct(this::compositeKey);
                            });
                })
                .switchIfEmpty(getFromDatabase(keys))
                .onErrorResume(e -> {
                    log.error("Final Redis failure, falling back to database: {}", e.getMessage());
                    return getFromDatabase(keys);
                });
    }

    // Helper method: fallback to fetching all entities from the database (via repository)
    private Flux<EntityClass> getFromDatabase(List<EntityClass> keys) {
        return Flux.fromIterable(keys)
                .flatMap(key -> entityRepository.findByEntityIdAndIdTypeAndScheme(key.entityId(), key.idType(), key.scheme()));
    }

    // Determines if the given entity is a tombstone.
    private boolean isTombstone(EntityClass entity) {
        return entity != null && "TOMBSTONE".equals(entity.entityId());
    }

    // Generates a composite key using entityId, idType, and scheme.
    private String compositeKey(EntityClass entity) {
        return entity.entityId() + "~" + entity.idType() + "~" + entity.scheme();
    }
}
