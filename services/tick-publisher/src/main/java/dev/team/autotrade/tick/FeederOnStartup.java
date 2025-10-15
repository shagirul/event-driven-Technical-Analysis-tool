package dev.team.autotrade.tick;

import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FeederOnStartup {

    @Bean
    CommandLineRunner startMarketFeeder() {
        return args -> {
            Thread t = new Thread(() -> {
                try {
                    MarketFeederClient.main(new String[0]); // reuse the runnable class
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, "market-feeder");
            t.setDaemon(true); // allow app to exit cleanly if needed
            t.start();
        };
    }
}
