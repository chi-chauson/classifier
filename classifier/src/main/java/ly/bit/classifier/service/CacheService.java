package ly.bit.classifier.service;

import ly.bit.classifier.domain.EntityClass;
import ly.bit.classifier.repository.EntityRepository;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.client.RedisTimeoutException;
import org.redisson.client.RedisConnectionException;
import org.redisson.client.RedisBusyException;
import org.redisson.client.RedisMovedException;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class CacheService {

    private final RedissonReactiveClient redissonReactiveClient;
    private final EntityRepository entityRepository;

    // Tombstone marker to denote non-existent entries.
    private final EntityClass TOMBSTONE = new EntityClass("TOMBSTONE", "TOMBSTONE", "TOMBSTONE", "TOMBSTONE");

    public CacheService(RedissonReactiveClient redissonReactiveClient, EntityRepository entityRepository) {
        this.redissonReactiveClient = redissonReactiveClient;
        this.entityRepository = entityRepository;
    }

    // New method to process keys in batches of 20 concurrently
    public Flux<EntityClass> getCacheOrFallbackToDbInBatches(List<EntityClass> keys) {
        return Flux.fromIterable(keys)
                .buffer(20) // Split the list into batches of 20
                .flatMap(batch ->
                        getCacheOrFallbackToDb(batch)
                                .subscribeOn(Schedulers.parallel())
                );
    }

    /**
     * Retrieves a list of EntityClass from Redis (with a 20ms timeout).
     * If Redis is too slow or missing values, falls back to the database via the repository.
     * For keys not found in the database, tombstones are set (fire-and-forget).
     */
    public Flux<EntityClass> getCacheOrFallbackToDb(List<EntityClass> keys) {
        // Generate composite keys for cache lookup (composite key: entityId:idType:scheme)
        List<String> keyComposites = keys.stream()
                .map(this::compositeKey)
                .toList();

        return redissonReactiveClient.getBuckets()
                .get(keyComposites.toArray(new String[0]))
                .timeout(Duration.ofMillis(20))
                .onErrorResume(ex -> {
                    System.err.println("Redis failure: " + ex.getMessage() + ", falling back to database.");
                    return Mono.empty();
                })
                .flatMapMany(cachedValuesMap -> {
                    // Convert cached values to EntityClass and filter out tombstones.
                    List<EntityClass> cachedResults = cachedValuesMap.entrySet().stream()
                            .map(entry -> (EntityClass) entry.getValue())
                            .filter(value -> !isTombstone(value))
                            .toList();

                    // Determine which keys are missing from Redis.
                    List<EntityClass> missingKeys = keys.stream()
                            .filter(key -> !cachedValuesMap.containsKey(compositeKey(key)))
                            .toList();

                    if (missingKeys.isEmpty()) {
                        return Flux.fromIterable(cachedResults);
                    }

                    // Fetch missing entities from the database using the repository.
                    // For each missing key, use the repository method.
                    Flux<EntityClass> databaseResults = Flux.fromIterable(missingKeys)
                            .flatMap(key -> entityRepository.findByEntityIdAndIdTypeAndScheme(key.entityId(), key.idType(), key.scheme()));

                    return databaseResults
                            .collectList()
                            .flatMapMany(dbResults -> {
                                // Build a set of composite keys retrieved from the database.
                                Set<String> retrievedKeys = dbResults.stream()
                                        .map(this::compositeKey)
                                        .collect(Collectors.toSet());

                                // Identify missing keys that were not found in the database.
                                List<EntityClass> stillMissingKeys = missingKeys.stream()
                                        .filter(key -> !retrievedKeys.contains(compositeKey(key)))
                                        .toList();

                                // Fire-and-forget: for each still missing key, set a tombstone asynchronously.
                                Flux.fromIterable(stillMissingKeys)
                                        .doOnNext(missingKey -> asyncCacheSet(compositeKey(missingKey), TOMBSTONE, Duration.ofMinutes(10)))
                                        .subscribe();

                                // Fire-and-forget: cache each retrieved DB result asynchronously.
                                Flux<EntityClass> cachedDbResults = Flux.fromIterable(dbResults)
                                        .doOnNext(domainObject -> asyncCacheSet(compositeKey(domainObject), domainObject, Duration.ofMinutes(10)));

                                // Merge DB results with cached results and ensure uniqueness by composite key.
                                return cachedDbResults.concatWith(Flux.fromIterable(cachedResults))
                                        .distinct(entity -> compositeKey(entity));
                            });
                })
                .switchIfEmpty(getFromDatabaseFallback(keys))
                .onErrorResume(ex -> {
                    System.err.println("Final Redis failure, falling back to database: " + ex.getMessage());
                    return getFromDatabaseFallback(keys);
                });
    }

    // Helper method: fallback to fetching all entities from the database (via repository)
    private Flux<EntityClass> getFromDatabaseFallback(List<EntityClass> keys) {
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

    // Determines if the given entity is a tombstone.
    private boolean isTombstone(EntityClass entity) {
        return entity != null && "TOMBSTONE".equals(entity.entityId());
    }

    // Generates a composite key using entityId, idType, and scheme.
    private String compositeKey(EntityClass entity) {
        return entity.entityId() + "~" + entity.idType() + "~" + entity.scheme();
    }
}
