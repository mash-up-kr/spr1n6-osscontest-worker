# syntax=docker/dockerfile:1

# ---- Build stage ----
FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /workspace

# Gradle 캐시 재사용을 위해 의존성 관련 파일만 먼저 복사
COPY gradlew ./
COPY gradle ./gradle
COPY build.gradle.kts settings.gradle.kts ./
RUN chmod +x gradlew && ./gradlew --no-daemon dependencies > /dev/null 2>&1 || true

COPY src ./src
RUN ./gradlew --no-daemon bootJar -x test

# ---- Runtime stage ----
FROM eclipse-temurin:21-jre-jammy AS runtime
WORKDIR /app

RUN groupadd -r worker && useradd -r -g worker worker

COPY --from=build /workspace/build/libs/*-SNAPSHOT.jar app.jar
RUN chown worker:worker app.jar

USER worker

ENV JAVA_OPTS=""

# HTTP 서버가 없는(Kafka 컨슈머 전용) 워커라 actuator/health를 HTTP로 못 때린다.
# PID 1(exec로 교체된 java 프로세스)이 살아 있는지로 liveness만 확인한다.
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD grep -qa app.jar /proc/1/cmdline || exit 1

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
