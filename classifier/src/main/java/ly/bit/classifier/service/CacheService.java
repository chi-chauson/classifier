package ly.bit.classifier.service;

import ly.bit.classifier.domain.CacheTuple;
import ly.bit.classifier.domain.EntityClass;
import ly.bit.classifier.repository.EntityRepository;
import org.redisson.api.RBatchReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.client.RedisBusyException;
import org.redisson.client.RedisConnectionException;
import org.redisson.client.RedisMovedException;
import org.redisson.client.RedisTimeoutException;
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
import java.util.concurrent.TimeUnit;
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

    private class ErrorMarker {
        // This class is used as a marker to indicate an error
    }

    /**
     * Generic helper method that batches a list of keys (of type T) into batches of 20,
     * queries Redis using the provided keyMapper and codec, and aggregates the results
     * into a Map<String, V>.
     *
     * @param keys      List of keys of type T.
     * @param keyMapper Function that maps a T to a String composite key.
     * @param <V>       The type of the value stored in Redis.
     * @return Flux emitting individual key/value pairs as AbstractMap.SimpleEntry<String, V>.
     */
    public <V> Flux<CacheTuple<V>> fetchBatchedBucketsGeneric(
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
                    return new CacheTuple<>(entry.getKey(), isError ? null : (V) value, isError);
                });
    }


    /**
     * Main method that uses the generic batching helper to query Redis, determine missing keys,
     * fall back to the database for missing keys, set tombstones for keys not found in DB, and merge all results.
     *
     * @param keys List of EntityClass objects representing the requested entities.
     * @return A Flux emitting unique EntityClass results.
     */
    public Flux<EntityClass> getCacheOrFallbackToDbBatched(List<EntityClass> keys) {
        // Use the generic helper to get a typed aggregated map from Redis.
        return fetchBatchedBucketsGeneric(keys, this::compositeKey)
                .subscribeOn(Schedulers.parallel())
                .filter(tuple -> !tuple.error())
                .collectMap(tuple -> tuple.key(), tuple -> tuple.value())
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
                                        .collect(Collectors.toList());
                                log.info("Keys still missing after DB query: {}",
                                        stillMissingKeys.stream().map(this::compositeKey).collect(Collectors.toList()));

                                // For each key still missing, set a tombstone asynchronously (fire-and-forget).
                                Flux.fromIterable(stillMissingKeys)
                                        .doOnNext(missingKey -> {
                                            log.info("Setting tombstone for missing key: {}", compositeKey(missingKey));
                                            asyncCacheSet(compositeKey(missingKey), TOMBSTONE, Duration.ofMinutes(10));
                                        })
                                        .subscribe();

                                // Cache the DB results asynchronously.
                                Flux<EntityClass> cachedDbResults = Flux.fromIterable(dbResults)
                                        .doOnNext(entity -> {
                                            log.info("Caching DB result: {}", compositeKey(entity));
                                            asyncCacheSet(compositeKey(entity), entity, Duration.ofMinutes(10));
                                        });

                                // Merge the DB results with cached results and ensure uniqueness.
                                return cachedDbResults.concatWith(Flux.fromIterable(allCached.values()))
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

    // Reusable method to asynchronously set a value (or tombstone) in Redis (fire-and-forget)
    private void asyncCacheSet(String compositeKey, EntityClass value, Duration ttl) {
        redissonReactiveClient.getBucket(compositeKey)
                .set(value, ttl)
                .onErrorResume(ex -> {
                    if (ex instanceof RedisTimeoutException) {
                        System.err.println("RedisTimeoutException: " + ex.getMessage());
                    } else if (ex instanceof RedisConnectionException) {
                        System.err.println("RedisConnectionException: " + ex.getMessage());
                    } else if (ex instanceof RedisBusyException) {
                        System.err.println("RedisBusyException: " + ex.getMessage());
                    } else if (ex instanceof RedisMovedException) {
                        System.err.println("RedisMovedException: " + ex.getMessage());
                    } else {
                        System.err.println("Unknown Redis exception: " + ex.getMessage());
                    }
                    return Mono.empty();
                })
                .subscribe();
    }

    private <V> Mono<Void> asyncCacheBatchSet(Map<String, V> keysToValues, Duration ttl, int batchSize) {
        if (keysToValues == null || keysToValues.isEmpty()) {
            return Mono.empty();
        }

        List<List<Map.Entry<String, V>>> batches = new ArrayList<>();
        List<Map.Entry<String, V>> entries = new ArrayList<>(keysToValues.entrySet());

        for (int i = 0; i < entries.size(); i += batchSize) {
            batches.add(entries.subList(i, Math.max(i + batchSize, entries.size())));
        }

        return Flux.fromIterable(batches)
                .flatMap(batchEntries -> {
                    RBatchReactive redisBatch = redissonReactiveClient.createBatch();

                    for (Map.Entry<String, V> entry : batchEntries) {
                        redisBatch.getBucket(entry.getKey())
                                .set(entry.getValue(), ttl.toMillis(), TimeUnit.MICROSECONDS);
                    }

                    return redisBatch.execute()
                            .doOnNext(result -> log.info("Executed batch of size {}", batchEntries.size()))
                            .onErrorResume(throwable -> {
                               log.error("Failed batch set operation in cache: ", throwable);
                               return Mono.empty();
                            });

                }).then();
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
