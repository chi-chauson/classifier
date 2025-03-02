package ly.bit.classifier;

import org.springframework.boot.SpringApplication;

public class TestClassifierApplication {

    public static void main(String[] args) {
        SpringApplication.from(ClassifierApplication::main).with(TestcontainersConfiguration.class).run(args);
    }

}
