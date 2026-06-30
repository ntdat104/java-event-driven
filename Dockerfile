# Multi-stage build for either Spring Boot service. Pick the module at build time:
#   docker build --build-arg MODULE=order-service     -t edd-order-service .
#   docker build --build-arg MODULE=inventory-service -t edd-inventory-service .
# (docker-compose passes MODULE for you.)

# ---- build ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

# Resolve dependencies first so they cache across source-only changes.
COPY pom.xml settings-local.xml ./
COPY common-events/pom.xml common-events/pom.xml
COPY order-service/pom.xml order-service/pom.xml
COPY inventory-service/pom.xml inventory-service/pom.xml
RUN mvn -s settings-local.xml -B -q -pl order-service,inventory-service -am \
        dependency:go-offline -DskipTests || true

# Build the requested module (+ its modules) into a single fat jar.
COPY common-events/src common-events/src
COPY order-service/src order-service/src
COPY inventory-service/src inventory-service/src
ARG MODULE
RUN mvn -s settings-local.xml -B -q -pl "${MODULE}" -am clean package -DskipTests \
 && cp "${MODULE}"/target/*.jar /workspace/app.jar

# ---- runtime ----
FROM eclipse-temurin:21-jre
# curl is only here for the container HEALTHCHECK.
RUN apt-get update && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /workspace/app.jar /app/app.jar
EXPOSE 8080
ENV JAVA_OPTS="-Xms256m -Xmx512m"
HEALTHCHECK --interval=10s --timeout=3s --start-period=45s --retries=12 \
  CMD curl -fsS http://localhost:8080/actuator/health || exit 1
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
