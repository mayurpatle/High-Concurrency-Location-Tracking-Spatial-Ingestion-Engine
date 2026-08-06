package com.geopulse.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// @SpringBootApplication bundles three things:
//   @Configuration      - this class can define beans
//   @EnableAutoConfiguration - Spring Boot wires up Kafka/Redis/Cassandra/Tomcat
//                              based purely on what's on the classpath (our POM deps)
//   @ComponentScan      - discovers our @Component/@Service/@RestController classes
@SpringBootApplication(scanBasePackages = "com.geopulse")

public class IngestionServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(IngestionServiceApplication.class, args);
    }
}