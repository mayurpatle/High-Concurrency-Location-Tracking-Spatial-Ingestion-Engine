package com.geopulse.processing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "com.geopulse")
@EnableScheduling
public class ProcessingServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(ProcessingServiceApplication.class, args);
    }
}