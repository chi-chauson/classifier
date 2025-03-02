package ly.bit.classifier;

import org.redisson.Redisson;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedissonConfig {

    @Bean
    public RedissonReactiveClient redissonReactiveClient() {
        Config config = new Config();
        // Adjust for your Redis (Sentinel, cluster, etc.). For demonstration, we're using single server.
        config.useSingleServer()
                .setAddress("redis://127.0.0.1:6379")
                .setTimeout(3000);
        return Redisson.createReactive(config);
    }
}
