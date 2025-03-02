package ly.bit.classifier.repository;

import ly.bit.classifier.domain.EntityClass;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

public interface EntityRepository extends ReactiveCrudRepository<EntityClass, String> {

    // Query by composite key fields.
    @Query("SELECT * FROM entity WHERE entity_id = :entityId AND id_type = :idType AND scheme = :scheme")
    Flux<EntityClass> findByEntityIdAndIdTypeAndScheme(String entityId, String idType, String scheme);
}
