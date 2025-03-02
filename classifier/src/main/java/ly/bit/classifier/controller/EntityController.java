package ly.bit.classifier.controller;

import ly.bit.classifier.domain.EntityClass;
import ly.bit.classifier.service.CacheService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

import java.util.List;

@RestController
@RequestMapping("/entities")
public class EntityController {

    private final CacheService cacheService;

    public EntityController(CacheService cacheService) {
        this.cacheService = cacheService;
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public Flux<EntityClass> getEntities(@RequestParam(required = false) List<String> entityIds) {
        List<EntityClass> keys;
        if (entityIds == null || entityIds.isEmpty()) {
            keys = List.of(
                    new EntityClass("A", "Type1", "Scheme1", ""),
                    new EntityClass("B", "Type1", "Scheme1", ""),
                    new EntityClass("C", "Type2", "Scheme2", ""),
                    new EntityClass("notFound", "TypeX", "SchemeX", ""),
                    new EntityClass("D", "Type3", "Scheme3", "")
            );
        } else {
            keys = entityIds.stream()
                    .map(id -> {
                        String[] tokens = id.split("~");
                        return new EntityClass(tokens[0], tokens[1], tokens[2], "");
                    })
                    .toList();
        }
        return cacheService.getCacheOrFallbackToDbBatched(keys);
    }
}
