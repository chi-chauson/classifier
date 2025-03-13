package ly.bit.classifier.repository;

import ly.bit.classifier.domain.EntityClass;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

import java.util.List;

public interface EntityRepository extends ReactiveCrudRepository<EntityClass, String> {

    // Query by composite key fields.
    @Query("SELECT * FROM entity WHERE entity_id = :entityId AND id_type = :idType AND scheme = :scheme")
    Flux<EntityClass> findByEntityIdAndIdTypeAndScheme(String entityId, String idType, String scheme);

    /**
     * Query entities by multiple composite key combinations.
     * Fetches entities that match any of the entityId, idType, and scheme combinations in the provided lists.
     *
     * @param entityIds List of entity IDs to match
     * @param idTypes List of ID types to match
     * @param schemes List of schemes to match
     * @return Flux of EntityClass objects matching the criteria
     */
    @Query("SELECT * FROM entity WHERE entity_id IN (:entityIds) AND id_type IN (:idTypes) AND scheme IN (:schemes)")
    Flux<EntityClass> findByEntityIdInAndIdTypeInAndSchemeIn(
            List<String> entityIds,
            List<String> idTypes,
            List<String> schemes
    );
}
