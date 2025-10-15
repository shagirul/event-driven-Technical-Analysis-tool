FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /proj
COPY . .
RUN mvn -q -DskipTests -pl services/tick-publisher -am package

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /proj/services/tick-publisher/target/*.jar app.jar
ENV KAFKA_BOOTSTRAP=kafka:9092
ENV APP_DESIRE_TOPIC=tick.desired.v1
ENV APP_TICK_TOPIC=tick.v1
ENV APP_TTL_SECONDS=30
ENV APP_PERIOD_MS=200
ENTRYPOINT ["java","-XX:+UseZGC","-jar","/app/app.jar"]
