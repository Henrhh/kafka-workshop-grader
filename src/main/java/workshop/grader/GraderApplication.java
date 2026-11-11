package workshop.grader;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class GraderApplication {

    public static void main(String[] args) {
        SpringApplication.run(GraderApplication.class, args);
    }
}
