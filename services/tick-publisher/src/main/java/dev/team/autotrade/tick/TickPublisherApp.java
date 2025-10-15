package dev.team.autotrade.tick;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableScheduling
@EnableKafka
public class TickPublisherApp {
    public static void main(String[] args) {
        SpringApplication.run(TickPublisherApp.class, args);
    }
}
