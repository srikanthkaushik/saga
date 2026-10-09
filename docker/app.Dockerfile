# Runtime image for one Spring Boot module (order-service, payment-service, inventory-service, saga-ui).
# Build from the repository root after `mvn package` (ops\images.cmd does both):
#   docker build -f docker/app.Dockerfile --build-arg MODULE=order-service -t ghcr.io/srikanthkaushik/saga-order-service .

FROM eclipse-temurin:21-jre AS extract
ARG MODULE
WORKDIR /build
COPY ${MODULE}/target/${MODULE}-0.0.1-SNAPSHOT.jar app.jar
# Spring Boot layers: dependencies rarely change, so an update re-pushes only the small application layer
RUN java -Djarmode=tools -jar app.jar extract --layers --launcher --destination extracted

FROM eclipse-temurin:21-jre
LABEL org.opencontainers.image.source="https://github.com/srikanthkaushik/saga"
RUN useradd --system --uid 10001 --no-create-home saga
WORKDIR /app
COPY --from=extract /build/extracted/dependencies/ ./
COPY --from=extract /build/extracted/spring-boot-loader/ ./
COPY --from=extract /build/extracted/snapshot-dependencies/ ./
COPY --from=extract /build/extracted/application/ ./
USER 10001
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75"
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
