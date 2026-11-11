# Build the jar first: ./gradlew bootJar
FROM eclipse-temurin:21-jre
RUN useradd --system --uid 1001 grader
USER grader
WORKDIR /app
COPY build/libs/kafka-workshop-grader-*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
